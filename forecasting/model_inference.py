"""
inference.py — Cleanly refactored

This module now serves TWO roles cleanly:

1. As an importable **model inference helper**:
    - load_model_safe()
    - predict_single()
    - preprocess_spark()
    - create_dataloader()
    - evaluate_lstm()

2. As an executable **Spark Structured Streaming pipeline**:
    - start_spark_streaming()

Importing this file NO LONGER starts Spark automatically.
Spark only runs if:
    python inference.py
or another script explicitly calls:
    start_spark_streaming()
"""

import os
import time
import threading
import logging
from collections import defaultdict

import torch
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, from_json, trim, when

from influxdb_client import InfluxDBClient

import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

# Local imports
from model.preprocess_data import preprocess_spark, create_dataloader
from model.network import LSTMVAE, load_model as load_model_pytorch
from model.evaluate import evaluate_lstm
import socket
from pyspark.sql import SparkSession
# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
SERVICE_NAME = os.getenv("SERVICE_NAME", "spark-consumer")
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(SERVICE_NAME)

# -----------------------------------------------------------------------------
# GLOBAL MODEL + AUTO-RELOAD THREAD
# -----------------------------------------------------------------------------
model_path = "/weights/lstm_vae_swat.pth"
current_model = None
last_modified_time = None
model_lock = threading.Lock()

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
logger.info(f"Using device: {device}")

def load_model_safe():
    """
    Safe wrapper that loads the PyTorch model
    and updates global state.
    """
    global current_model, last_modified_time

    try:
        if not os.path.exists(model_path):
            logger.warning(f"Model path {model_path} does not exist.")
            return None

        logger.info(f"Loading model from {model_path}")

        with model_lock:
            current_model = load_model_pytorch(model_path, device=device)
            current_model.eval()
            last_modified_time = os.path.getmtime(model_path)

        logger.info("Model loaded successfully.")
        return current_model

    except Exception as e:
        logger.error(f"Failed to load model: {e}")
        return None


def _reload_if_needed():
    """Check if model file changed and reload."""
    global last_modified_time

    if not os.path.exists(model_path):
        return

    try:
        mtime = os.path.getmtime(model_path)
        if last_modified_time is None or mtime > last_modified_time:
            logger.info("Model weights updated — reloading.")
            load_model_safe()
    except Exception as e:
        logger.error(f"Error checking model: {e}")


def start_model_reload_thread(interval=60):
    """
    Background thread to periodically reload model weights.
    """

    def _worker():
        logger.info(f"Model reload thread started (interval = {interval}s)")
        while True:
            time.sleep(interval)
            _reload_if_needed()

    t = threading.Thread(target=_worker, daemon=True)
    t.start()

# Load initial model
load_model_safe()
start_model_reload_thread(60)

# -----------------------------------------------------------------------------
# PREDICTION HELPER FOR NON-SPARK PIPELINES
# -----------------------------------------------------------------------------
def predict_single(model, input_tensor):
    """Simple wrapper used by forecasting_stream_mining.py."""
    with torch.no_grad():
        output = model(input_tensor.to(device))
    return output.cpu().numpy()

# -----------------------------------------------------------------------------
# OPTIONAL SPARK STREAMING PIPELINE
# -----------------------------------------------------------------------------
def _validate_bootstrap(bootstrap):
    # bootstrap may be "host1:9092,host2:9092"
    hosts = [h.split(':')[0] for h in bootstrap.split(',')]
    for h in hosts:
        try:
            socket.gethostbyname(h)
        except Exception as e:
            raise RuntimeError(f"Cannot resolve Kafka bootstrap host '{h}'. "
                               f"Set KAFKA_BOOTSTRAP to an address reachable from Spark driver. Error: {e}")


import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

def build_kafka_stream(spark: SparkSession):
    """
    Builds a Spark streaming DataFrame from Kafka with a proper schema.

    Auto-detects bootstrap server based on environment:
    - If running inside Docker, use 'kafka:9092'
    - If running on host machine, use 'localhost:9092'
    """
    # Determine Kafka bootstrap server
    bootstrap = os.getenv("KAFKA_BOOTSTRAP")
    if not bootstrap:
        # Try to detect host environment
        if os.getenv("INSIDE_DOCKER", "0") == "1":
            bootstrap = "kafka:9092"
        else:
            bootstrap = "localhost:9092"

    # Optional: validate the bootstrap server format
    if ":" not in bootstrap:
        raise ValueError(f"Invalid KAFKA_BOOTSTRAP: {bootstrap}")

    # Define schema of the Kafka messages (adjust fields as needed)
    schema = StructType([
        StructField("sensor_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True)
    ])

    # Keep track of all field names
    all_fields = [field.name for field in schema.fields]

    # Build streaming DataFrame from Kafka
    stream_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", "ics-sensor-data")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Parse the JSON payload in the `value` column
    stream_df = stream_df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    return stream_df, schema, all_fields



def run_eval(batch_df, batch_id, schema, numeric_cols):
    """
    Process a single Spark batch.
    """

    if batch_df.count() == 0:
        logger.info(f"Batch {batch_id} empty — skipping.")
        return

    logger.info(f"Batch {batch_id}: parsing Kafka JSON...")

    # Directly use the existing columns
    parsed = batch_df

    # Cast numeric columns
    for c in numeric_cols:
        parsed = parsed.withColumn(c, when(trim(col(c)) == "", None)
                                   .otherwise(col(c).cast(DoubleType())))

    parsed = parsed.withColumn("timestamp", trim(col("timestamp")))

    total = parsed.count()
    logger.info(f"Batch {batch_id}: parsed rows = {total}")

    # Preprocess for LSTM-VAE
    df_prep = preprocess_spark(parsed)
    dataloader = create_dataloader(df_prep, batch_size=32, sequence_length=30)

    if dataloader is None:
        logger.info(f"Batch {batch_id}: Not enough data — skipping.")
        return

    with model_lock:
        model_to_use = current_model

    if model_to_use is None:
        logger.warning("Model not loaded — skipping.")
        return

    # Evaluate
    anomalies = evaluate_lstm(model_to_use, dataloader, device, percentile=90)
    logger.info(f"Batch {batch_id}: anomalies = {anomalies}")

    # Write anomalies to InfluxDB
    write_to_influx(anomalies, batch_id)

     # Save anomalies to a file
    with open(f"anomalies_batch_{batch_id}.txt", "w") as f:
        f.write(f"Batch {batch_id} anomalies:\n")
        f.write("\n".join(map(str, anomalies)))


def write_to_influx(anomaly_indices, batch_id):
    client = InfluxDBClient(host='localhost', port=8086, database='forecasting')
    json_body = [
        {
            "measurement": "anomalies",
            "tags": {
                "batch_id": batch_id
            },
            "fields": {
                "count": len(anomaly_indices)
            }
        }
    ]
    client.write_points(json_body)

def start_spark_streaming():
    """
    Explicit entry point for Spark Structured Streaming.
    Safe to call — will NOT run on import.
    """

    logger.info("Starting Spark streaming pipeline...")

    spark = (
        SparkSession.builder
        .appName("MockKafkaConsumerWithInference")
        .master("local[*]")
        .getOrCreate()
    )

    stream_df, schema, all_fields = build_kafka_stream(spark)

    numeric_cols = [c for c in all_fields if c not in ("Timestamp", "Normal_Attack")]

    query = (
        stream_df
        .writeStream
        .foreachBatch(lambda df, batch_id:
                      run_eval(df, batch_id, schema, numeric_cols))
        .start()
    )

    logger.info("Spark streaming started — awaiting termination")
    query.awaitTermination()


# -----------------------------------------------------------------------------
# RUNNING DIRECTLY
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    # Running: python inference.py
    start_spark_streaming()
