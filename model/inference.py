import logging
import os
import threading
import time
import json

import numpy as np
import torch
from kafka import KafkaProducer

from prometheus_client import start_http_server, Gauge, Counter

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, from_json, trim, when

from preprocess_data import preprocess_spark, create_dataloader
from network import load_model, loss_function

# -----------------------------------------------------------------------------#
# Logging
# -----------------------------------------------------------------------------#
SERVICE_NAME = os.getenv("SERVICE_NAME", "spark-consumer")

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format=f"%(asctime)s %(levelname)s %(name)s app={SERVICE_NAME} %(message)s",
)
logger = logging.getLogger(SERVICE_NAME)

# -----------------------------------------------------------------------------#
# Prometheus Metrics
# -----------------------------------------------------------------------------#
ae_last_anomaly_score = Gauge(
    "ae_last_anomaly_score",
    "Last anomaly score from LSTM-VAE",
)

ae_threshold = Gauge(
    "ae_threshold",
    "Threshold used for anomaly detection",
)

ae_is_anomaly = Gauge(
    "ae_is_anomaly",
    "Whether last sequence was anomaly (1/0)",
)

ae_anomaly_events_total = Counter(
    "ae_anomaly_events_total",
    "Total number of anomaly events detected",
)

anomaly_gauge = Gauge(
    "anomalous_sensor_event",
    "Detected anomalous event (1 if any anomaly in batch, else 0)",
    ["sensor"],
)

def start_prometheus_exporter():
    """
    Expose the SAME metrics registry on both ports:
      - 9109  -> job 'spark-consumer-metrics'
      - 8001  -> job 'spark-consumer-inference'
    This keeps group configs intact and makes both Prometheus targets UP.
    """
    logger.info("Starting Prometheus exporter on ports 9109 and 8001...")
    # main existing port used by the team
    start_http_server(9109)
    # extra port to satisfy spark-consumer-inference target
    start_http_server(8001)
    logger.info("Prometheus exporter started on ports 9109 and 8001.")

# start the exporter in a background thread
threading.Thread(target=start_prometheus_exporter, daemon=True).start()

# -----------------------------------------------------------------------------#
# Spark session
# -----------------------------------------------------------------------------#
spark = (
    SparkSession.builder
    .appName("MockKafkaConsumerWithInference")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# -----------------------------------------------------------------------------#
# Schema
# -----------------------------------------------------------------------------#
all_fields = [
    "Timestamp", "FIT101", "LIT101", "MV101", "P101", "P102",
    "AIT201", "AIT202", "AIT203", "FIT201", "MV201", "P201", "P202", "P203",
    "P204", "P205", "P206", "DPIT301", "FIT301", "LIT301", "MV301", "MV302",
    "MV303", "MV304", "P301", "P302", "AIT401", "AIT402", "FIT401", "LIT401",
    "P401", "P402", "P403", "P404", "UV401", "AIT501", "AIT502", "AIT503",
    "AIT504", "FIT501", "FIT502", "FIT503", "FIT504", "P501", "P502",
    "PIT501", "PIT502", "PIT503", "FIT601", "P601", "P602", "P603",
    "Normal_Attack",
]

schema = StructType([StructField(f, StringType(), True) for f in all_fields])
numeric_cols = [c for c in all_fields if c not in ("Timestamp", "Normal_Attack")]

# -----------------------------------------------------------------------------#
# Kafka config
# -----------------------------------------------------------------------------#
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_INPUT_TOPIC = os.getenv("KAFKA_INPUT_TOPIC", "ics-sensor-data")
KAFKA_OUTPUT_TOPIC = os.getenv("KAFKA_OUTPUT_TOPIC", "ics-anomaly-scores")

# -----------------------------------------------------------------------------#
# Kafka producer with retry
# -----------------------------------------------------------------------------#
def create_kafka_producer_with_retry(max_retries: int = 40, sleep_sec: float = 3.0):
    """
    Create KafkaProducer with retries.
    Prevents container crash when Kafka is not ready yet.
    """
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(
                f"Trying to connect KafkaProducer to {KAFKA_BOOTSTRAP} "
                f"(attempt {attempt}/{max_retries})"
            )
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BOOTSTRAP],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            logger.info("KafkaProducer connected successfully.")
            return producer
        except Exception as exc:
            logger.warning(
                f"Kafka not ready (attempt {attempt}/{max_retries}): {exc}"
            )
            time.sleep(sleep_sec)

    logger.error(
        f"Failed to connect to Kafka at {KAFKA_BOOTSTRAP} after {max_retries} attempts."
    )
    return None

producer = create_kafka_producer_with_retry()

# -----------------------------------------------------------------------------#
# Spark stream
# -----------------------------------------------------------------------------#
stream_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_INPUT_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
logger.info(f"Using device: {device}")

# -----------------------------------------------------------------------------#
# Compute scores helper
# -----------------------------------------------------------------------------#
def compute_scores(model, dataloader, device, percentile_threshold=90):
    model.eval()
    scores = []

    with torch.no_grad():
        for batch in dataloader:
            batch = torch.tensor(batch, dtype=torch.float32).to(device)
            for i in range(batch.shape[0]):
                seq = batch[i:i+1]
                recon, mean, logvar = model(seq)
                loss = loss_function(recon, seq, mean, logvar)
                scores.append(float(loss.item()))

    if not scores:
        return [], [], None

    threshold = float(np.percentile(scores, percentile_threshold))
    anomalies = [i for i, s in enumerate(scores) if s > threshold]

    return scores, anomalies, threshold

# -----------------------------------------------------------------------------#
# Model loading (with auto-reload)
# -----------------------------------------------------------------------------#
model_lock = threading.Lock()
current_model = None
model_path = os.getenv("MODEL_PATH", "/weights/lstm_vae_swat.pth")
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
            logger.info(
                f"Model loaded successfully. Last modified: {last_modified_time}"
            )
    except Exception as e:
        logger.error(f"Failed to load model: {e}")

def check_and_reload_model():
    global last_modified_time
    try:
        if not os.path.exists(model_path):
            return
        current_mtime = os.path.getmtime(model_path)
        if last_modified_time is None or current_mtime > last_modified_time:
            logger.info("Model weights updated. Reloading...")
            load_model_safe()
    except Exception as e:
        logger.error(f"Error checking model file: {e}")

def model_reload_worker(check_interval=60):
    logger.info(f"Model reload worker started. Interval: {check_interval}s")
    while True:
        time.sleep(check_interval)
        check_and_reload_model()

load_model_safe()
threading.Thread(
    target=model_reload_worker,
    args=(60,),
    daemon=True
).start()
logger.info("Model auto-reload thread started")

# -----------------------------------------------------------------------------#
# foreachBatch handler
# -----------------------------------------------------------------------------#
def run_eval(batch_df, batch_id):
    if batch_df.count() == 0:
        logger.info(f"Batch {batch_id} empty, skipping.")
        return

    cleaned = (
        batch_df
        .select(from_json(col("value").cast("string"), schema).alias("data"))
        .select("data.*")
    )

    parsed = cleaned
    for c in numeric_cols:
        parsed = parsed.withColumn(
            c,
            when(trim(col(c)) == "", None).otherwise(col(c).cast(DoubleType())),
        )

    parsed = (
        parsed.withColumn("Timestamp", trim(col("Timestamp")))
        .withColumn("Normal_Attack", trim(col("Normal_Attack")))
    )

    df_pre = preprocess_spark(parsed)
    dataloader = create_dataloader(df_pre, batch_size=32, sequence_length=30)

    if dataloader is None:
        logger.info(f"Batch {batch_id}: not enough data.")
        return

    with model_lock:
        model_to_use = current_model

    if model_to_use is None:
        logger.warning(f"Batch {batch_id}: model not loaded yet.")
        return

    # --- Run inference ---
    scores, anomalies, threshold = compute_scores(
        model_to_use, dataloader, device
    )

    logger.info(f"BATCH {batch_id} — {len(scores)} scores, threshold={threshold}")

    # --- Prometheus ---
    if scores:
        last_score = scores[-1]
        is_anomaly = 1 if anomalies else 0

        ae_last_anomaly_score.set(last_score)
        if threshold is not None:
            ae_threshold.set(threshold)
        ae_is_anomaly.set(is_anomaly)
        if is_anomaly:
            ae_anomaly_events_total.inc()

        anomaly_gauge.labels(sensor="any").set(is_anomaly)

        logger.info(
            f"Prometheus updated: score={last_score:.5f}, threshold={threshold}, "
            f"is_anomaly={is_anomaly}"
        )

    # --- SEND REAL LSTM SCORES TO KAFKA ---
    if scores:
        if producer is None:
            logger.error("Kafka producer is None; skipping send.")
        else:
            try:
                for s in scores:
                    payload = {"anomaly_score": float(s)}
                    producer.send(KAFKA_OUTPUT_TOPIC, payload)
                producer.flush()
                logger.info(
                    f"Sent {len(scores)} anomaly scores to Kafka topic '{KAFKA_OUTPUT_TOPIC}'"
                )
            except Exception as e:
                logger.error(f"Failed to send anomaly scores: {e}")

# -----------------------------------------------------------------------------#
# Start streaming
# -----------------------------------------------------------------------------#
query = (
    stream_df
    .writeStream
    .foreachBatch(run_eval)
    .start()
)

query.awaitTermination()
