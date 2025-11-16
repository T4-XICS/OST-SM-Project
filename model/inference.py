import logging
import os
import threading
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, from_json, trim, when
import torch
from preprocess_data import preprocess_spark, create_dataloader
from prometheus_client import start_http_server, Gauge

# Configure logging to stdout so Promtail/Loki can scrape it
SERVICE_NAME = os.getenv("SERVICE_NAME", "spark-consumer")
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format=f"%(asctime)s %(levelname)s %(name)s app={SERVICE_NAME} %(message)s",
)
logger = logging.getLogger(SERVICE_NAME)

# Gauge for anomalous sensor events
anomaly_gauge = Gauge(
    'anomalous_sensor_event',
    'Detected anomalous sensor event',
    ['sensor']
)

start_http_server(8001)
logger.info("Prometheus metrics server started on port 8001")

spark = (
    SparkSession.builder
    .appName("MockKafkaConsumerWithInference")
    .master("local[*]")
    .config("spark.hadoop.security.authentication", "simple")
    .getOrCreate()
)

# replace the previous typed schema with an all-string schema, then cast later
all_fields = [
    "Timestamp","FIT101","LIT101","MV101","P101","P102","AIT201","AIT202","AIT203",
    "FIT201","MV201","P201","P202","P203","P204","P205","P206","DPIT301","FIT301",
    "LIT301","MV301","MV302","MV303","MV304","P301","P302","AIT401","AIT402","FIT401",
    "LIT401","P401","P402","P403","P404","UV401","AIT501","AIT502","AIT503","AIT504",
    "FIT501","FIT502","FIT503","FIT504","P501","P502","PIT501","PIT502","PIT503",
    "FIT601","P601","P602","P603","Normal_Attack"
]

schema = StructType([StructField(f, StringType(), True) for f in all_fields])
numeric_cols = [c for c in all_fields if c not in ("Timestamp", "Normal_Attack")]

stream_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "ics-sensor-data")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

from network import LSTMVAE, load_model
from evaluate import evaluate_lstm
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
logger.info(f"Using device: {device}")

model_lock = threading.Lock()
current_model = None
model_path = "/weights/lstm_vae_swat.pth"
last_modified_time = None

def load_model_safe():
    global current_model, last_modified_time
    try:
        if not os.path.exists(model_path):
            logger.warning(f"Model path {model_path} does not exist yet.")
            return
        logger.info(f"Loading model from {model_path}")
        with model_lock:
            current_model = load_model(model_path, device=device)
            current_model.eval()
            last_modified_time = os.path.getmtime(model_path)
            logger.info(f"Model loaded successfully. Last modified: {last_modified_time}")
    except Exception as e:
        logger.error(f"Failed to load model: {e}")

def check_and_reload_model():
    global last_modified_time
    try:
        if not os.path.exists(model_path):
            return
        
        current_mtime = os.path.getmtime(model_path)
        if last_modified_time is None or current_mtime > last_modified_time:
            logger.info(f"Model weights updated (mtime: {current_mtime}). Reloading model...")
            load_model_safe()
        else:
            logger.info("Model weights have not changed; no reload needed.")
    except Exception as e:
        logger.error(f"Error checking model file: {e}")

def model_reload_worker(check_interval=60):
    logger.info(f"Model reload worker started. Check interval: {check_interval}s")
    while True:
        time.sleep(check_interval)
        check_and_reload_model()

load_model_safe()

# Start background thread for periodic model reloading
reload_thread = threading.Thread(target=model_reload_worker, args=(60,), daemon=True)
reload_thread.start()
logger.info("Model auto-reload thread started")


def run_eval(batch_df, batch_id):
    if batch_df.count() == 0:
        logger.info(f"Batch {batch_id} is empty. Skipping evaluation.")
        return

    logger.info(f"Batch {batch_id} RAW SAMPLE (value column)")
    batch_df.select(col("value").cast("string").alias("raw_value")).show(5, False)

    # parse JSON into string columns
    cleaned = batch_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    # trim and cast numeric string columns -> DoubleType, treat empty strings as null
    parsed = cleaned
    for c in numeric_cols:
        parsed = parsed.withColumn(c, when(trim(col(c)) == "", None).otherwise(col(c).cast(DoubleType())))

    # trim timestamp and label
    parsed = parsed.withColumn("Timestamp", trim(col("Timestamp")))\
                   .withColumn("Normal_Attack", trim(col("Normal_Attack")))

    logger.info(f"Batch {batch_id} PARSED SAMPLE")
    parsed.show(5, False)

    total = parsed.count()
    logger.info(f"Parsed rows: {total}")
    for f in parsed.columns:
        nulls = parsed.filter(col(f).isNull()).count()
        logger.info(f"{f}: nulls={nulls}")

    # only then preprocess and create dataloader
    df_pre = preprocess_spark(parsed)
    dataloader = create_dataloader(df_pre, batch_size=32, sequence_length=30)

    if dataloader is None:
        logger.info(f"Batch {batch_id}: not enough data to run inference yet. Skipping.")
        return

    df_pre.show(5, False)

    with model_lock:
        if current_model is None:
            logger.warning(f"Batch {batch_id}: Model not loaded yet. Skipping evaluation.")
            return
        model_to_use = current_model

    # Evaluate
    anomalies = evaluate_lstm(model_to_use, dataloader, device, 90)

    logger.info(f"Batch {batch_id} Evaluation")
    logger.info(f"Anomalies: {anomalies}")

    logger.debug(f"Starting Prometheus reporting for batch {batch_id} with {len(anomalies)} anomalies.")
    if len(anomalies) > 0:
        try:
            data = dataloader.dataset
            for idx in anomalies:
                if idx < 0 or idx >= len(numeric_cols):
                    logger.warning(f"Prometheus: Anomaly index {idx} out of range for sensors list.")
                    continue
                sensor = numeric_cols[idx]
                logger.debug(f"Prometheus: Processing anomaly for sensor '{sensor}' (index {idx})")
                # Find the latest sample in the batch (sequence)
                # For a typical dataloader, data[-1] is a tensor of shape (sequence_length, num_sensors)
                # We want the last time step, and the sensor index
                if hasattr(data, 'iloc'):
                    tensor_seq = data.iloc[-1]
                else:
                    tensor_seq = data[-1]
                # tensor_seq should be shape (sequence_length, num_sensors)
                if hasattr(tensor_seq, 'shape') and len(tensor_seq.shape) == 2:
                    seq_len, num_sensors = tensor_seq.shape
                    if idx < num_sensors:
                        value = tensor_seq[-1, idx].item()
                        logger.debug(f"Prometheus: Sensor {sensor}, value: {value}")
                        if value is not None:
                            anomaly_gauge.labels(sensor=sensor).set(value)
                            logger.info(f"Prometheus metric set: anomalous_sensor_event{{sensor='{sensor}'}} = {value}")
        except Exception as e:
            logger.error(f"Prometheus reporting failed: {e}")
    else:
        logger.debug(f"Prometheus: No anomalies detected, resetting all gauges to 0.")
        # Reset all gauges to 0 if there is no anomaly
        for sensor in numeric_cols:
            anomaly_gauge.labels(sensor=sensor).set(0)
            logger.info(f"Prometheus metric reset: anomalous_sensor_event{{sensor='{sensor}'}} = 0")

query = (
    stream_df
    .writeStream
    .foreachBatch(lambda df, batch_id: run_eval(df, batch_id))
    #.option("checkpointLocation", "checkpoints/mock_consumer")
    .start()
)

query.awaitTermination()
