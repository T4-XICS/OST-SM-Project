import pandas as pd
import torch
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit
from pyspark.sql.types import *
from influxdb_client import InfluxDBClient, Point
import os

from model_inference import load_model_safe, predict_forecast


# --------------------------------------------
# Model feature order (51 features)
# --------------------------------------------
model_features = [
    "FIT101", "LIT101", "MV101", "P101", "P102",
    "AIT201", "AIT202", "AIT203", "FIT201", "MV201",
    "P201", "P202", "P203", "P204", "P205",
    "P206", "DPIT301", "FIT301", "LIT301", "MV301",
    "MV302", "MV303", "MV304", "P301", "P302",
    "AIT401", "AIT402", "FIT401", "LIT401", "P401",
    "P402", "P403", "P404", "UV401", "AIT501",
    "AIT502", "AIT503", "AIT504", "FIT501", "FIT502",
    "FIT503", "FIT504", "P501", "P502", "PIT501",
    "PIT502", "PIT503", "FIT601", "P601", "P602", "P603"
]

# Only write forecast for selected features
FEATURES_TO_SAVE = ["AIT201", "FIT301", "LIT101"]
SEQ_LEN = 10
model = None


# ----------------------------------------------------
# Preprocess Spark Batch → Tensor for the Model
# ----------------------------------------------------
def preprocess_spark(df):
    """Convert Spark DataFrame batch to a PyTorch tensor with the last SEQ_LEN rows."""
    # Add missing columns with 0.0
    for col_name in model_features:
        if col_name not in df.columns:
            df = df.withColumn(col_name, lit(0.0))

    # Select features in correct order and convert to Pandas
    pdf = df.select(*model_features).toPandas().astype("float32").fillna(0.0)

    if len(pdf) < SEQ_LEN:
        return None  # Not enough rows

    arr = pdf.to_numpy()
    arr = arr[-SEQ_LEN:]  # Last SEQ_LEN rows
    return torch.tensor(arr, dtype=torch.float32).unsqueeze(0)  # Shape: (1, seq, 51)



# ----------------------------------------------------
# Write forecast of SELECTED FEATURES to InfluxDB
# ----------------------------------------------------
def write_predictions_to_influx(forecast, batch_id):
    FEATURES_TO_SAVE = ["AIT201", "FIT301", "LIT101"]

    # Convert forecast to numpy array safely
    if isinstance(forecast, torch.Tensor):
        forecast_arr = forecast.cpu().numpy()
    elif isinstance(forecast, tuple):
        # if forecast is a tuple, take the first element
        forecast_arr = forecast[0]
        if isinstance(forecast_arr, torch.Tensor):
            forecast_arr = forecast_arr.cpu().numpy()
    else:
        forecast_arr = forecast  # assume already numpy

    # Handle different shapes
    if forecast_arr.ndim == 3:  # (1, SEQ_LEN, num_features)
        forecast_arr = forecast_arr[0, -1, :]  # last timestep
    elif forecast_arr.ndim == 2:  # (1, num_features)
        forecast_arr = forecast_arr[0]  # remove batch dim
    # else: already 1D (num_features,)

    # Filter only desired features
    filtered = {
        feat: float(forecast_arr[i])
        for i, feat in enumerate(model_features)
        if feat in FEATURES_TO_SAVE
    }

    if not filtered:
        print("[WARN] No matching features found in forecast output.")
        return


    bucket = os.getenv("INFLUX_BUCKET", "forecasting")
    org = os.getenv("INFLUX_ORG", "ucs")
    token = os.getenv("INFLUX_TOKEN")
    url = os.getenv("INFLUX_URL", "http://influxdb:8086")

    client = InfluxDBClient(url=url, token=token, org=org)
    write_api = client.write_api()

    p = Point("forecast")
    for feature, value in filtered.items():
        p = p.field(feature, value)
        write_api.write(bucket=bucket, record=p)
    
    client.close()
    print("[DEBUG] Writing point to InfluxDB:", p)
    print("[INFO] Forecast written to InfluxDB (single measurement, multi-field).")


# ----------------------------------------------------
# foreachBatch evaluation function
# ----------------------------------------------------
def run_eval(batch_df, batch_id):
    global model

    print(f"\n====== Running batch {batch_id} ======")

    # Skip empty micro-batch
    if batch_df.rdd.isEmpty():
        print("[INFO] Batch empty — skipping.")
        return

    # Load model once
    if model is None:
        print("[INFO] Loading model...")
        model = load_model_safe()
        if model is None:
            print("[ERROR] Model failed to load — cannot run inference.")
            return
        print("[INFO] Model loaded successfully.")

    # Preprocess Spark batch → PyTorch tensor
    input_tensor = preprocess_spark(batch_df)
    if input_tensor is None:
        print("[INFO] Not enough rows for a sequence — skipping batch.")
        return

    # Run model
    try:
        forecast = predict_forecast(input_tensor)
        print("[INFO] Forecast generated.")
    except Exception as e:
        print(f"[ERROR] Failed to generate forecast: {e}")
        return

    # Write forecast to InfluxDB
    try:
        write_predictions_to_influx(forecast, batch_id)
        print("[INFO] Forecast written to InfluxDB.")
    except Exception as e:
        print(f"[ERROR] Failed to write to InfluxDB: {e}")



# ----------------------------------------------------
# Spark Streaming Setup (unchanged)
# ----------------------------------------------------
def main():
    spark = (
        SparkSession.builder
        .appName("ForecastConsumer")
        .master("local[*]")
        .getOrCreate()
    )

    # Define minimal schema for incoming SWaT messages
    schema = StructType([
        StructField("Timestamp", StringType()),
        *[StructField(feat, DoubleType()) for feat in model_features],
        StructField("Normal_Attack", StringType())
    ])

    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "ics-sensor-data")
        .load()
        .select(from_json(col("value").cast("string"), schema).alias("data"))
        .select("data.*")
    )

    (
        df.writeStream
        .foreachBatch(run_eval)
        .start()
        .awaitTermination()
    )



if __name__ == "__main__":
    main()
