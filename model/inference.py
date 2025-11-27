import logging
import os
import threading
import time
import json

import numpy as np
import torch
from kafka import KafkaProducer

from prometheus_client import start_http_server, Gauge, Counter
from collections import defaultdict
import numpy as np
import joblib

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, from_json, trim, when

from preprocess_data import preprocess_spark, create_dataloader
from network import LSTMVAE, load_model  # load_model should return a torch model instance
from evaluate import evaluate_lstm  # evaluate_lstm returns anomalies

from prometheus_client import start_http_server, Counter, Gauge, Histogram

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
    "Last anomaly score from LSTM-VAE (normalized to [0, 1] w.r.t. threshold)",
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
    Expose metrics on 9109 (existing) and 8001 (extra) so both Prom targets stay UP.
    """
    logger.info("Starting Prometheus exporter on ports 9109 and 8001...")
    start_http_server(9109)
    start_http_server(8001)
    logger.info("Prometheus exporter started on ports 9109 and 8001.")


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
# Kafka producer
# -----------------------------------------------------------------------------#
def create_kafka_producer_with_retry(max_retries: int = 40, sleep_sec: float = 3.0):
    """
    Create KafkaProducer with retries to avoid crashing if Kafka is not ready yet.
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
    """
    Run the LSTM-VAE over the dataloader, collect reconstruction losses
    as 'scores', and compute a percentile-based threshold.

    Returns:
        scores: list[float]
        anomalies: list[int]
        threshold: float | None
    """
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

    scores, anomalies, threshold = compute_scores(
        model_to_use, dataloader, device
    )

    logger.info(f"BATCH {batch_id} — {len(scores)} scores, threshold={threshold}")

    # --- PROMETHEUS METRICS ---
    if scores:
        last_score = scores[-1]
        is_anomaly = 1 if anomalies else 0

        # --- Normalize last_score with respect to threshold into [0, 1] ---
        if threshold is not None and threshold > 0:
            normalized_score = last_score / threshold
        else:
            normalized_score = last_score

        # Clamp to [0, 1] so dashboards always see a bounded value
        if normalized_score < 0.0:
            normalized_score = 0.0
        elif normalized_score > 1.0:
            normalized_score = 1.0

        # Export normalized score to Prometheus
        ae_last_anomaly_score.set(normalized_score)

        # Export threshold & anomaly flags as before
        if threshold is not None:
            ae_threshold.set(threshold)
        ae_is_anomaly.set(is_anomaly)
        if is_anomaly:
            ae_anomaly_events_total.inc()

        anomaly_gauge.labels(sensor="any").set(is_anomaly)

        logger.info(
            "Prometheus updated: raw_score=%.5f, norm_score=%.5f, threshold=%s, is_anomaly=%d",
            last_score,
            normalized_score,
            threshold,
            is_anomaly,
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
                    "Sent %d anomaly scores to Kafka topic '%s'",
                    len(scores),
                    KAFKA_OUTPUT_TOPIC,
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

logger.info("Spark streaming started - awaiting termination")
query.awaitTermination()
