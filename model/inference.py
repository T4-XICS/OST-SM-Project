import logging
import os
import threading
import numpy as np
import torch

from prometheus_client import start_http_server, Gauge, Counter

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, from_json, trim, when

from preprocess_data import preprocess_spark, create_dataloader
from network import LSTMVAE, load_model, loss_function


# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
SERVICE_NAME = os.getenv("SERVICE_NAME", "spark-consumer")

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format=f"%(asctime)s %(levelname)s %(name)s app={SERVICE_NAME} %(message)s",
)
logger = logging.getLogger(SERVICE_NAME)


# -----------------------------------------------------------------------------
# Prometheus Metrics
# -----------------------------------------------------------------------------
# Gauges
ae_last_anomaly_score = Gauge(
    "ae_last_anomaly_score",
    "Last anomaly score from LSTM-VAE"
)

ae_threshold = Gauge(
    "ae_threshold",
    "Threshold used for anomaly detection"
)

ae_is_anomaly = Gauge(
    "ae_is_anomaly",
    "Whether last sequence was anomaly (1/0)"
)

# Counters
ae_anomaly_events_total = Counter(
    "ae_anomaly_events_total",
    "Total number of anomaly events detected"
)


def start_prometheus_exporter():
    """Run exporter on port 9109 in background thread."""
    logger.info("Starting Prometheus exporter on 9109...")
    start_http_server(9109)
    logger.info("Prometheus exporter started.")


# start metrics server in background
threading.Thread(target=start_prometheus_exporter, daemon=True).start()


# -----------------------------------------------------------------------------
# Spark
# -----------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("MockKafkaConsumerWithInference")
    .master("local[*]")
    .config("spark.hadoop.security.authentication", "simple")
    .getOrCreate()
)


# -----------------------------------------------------------------------------
# Schema
# -----------------------------------------------------------------------------
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


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_INPUT_TOPIC = os.getenv("KAFKA_INPUT_TOPIC", "ics-sensor-data")
KAFKA_OUTPUT_TOPIC = os.getenv("KAFKA_OUTPUT_TOPIC", "ics-anomaly-scores")


# -----------------------------------------------------------------------------
# Kafka stream
# -----------------------------------------------------------------------------
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


# -----------------------------------------------------------------------------
# Evaluation Function
# -----------------------------------------------------------------------------
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


# -----------------------------------------------------------------------------
# foreachBatch
# -----------------------------------------------------------------------------
def run_eval(batch_df, batch_id):
    if batch_df.count() == 0:
        logger.info(f"Batch {batch_id} empty, skipping.")
        return

    # parse JSON
    cleaned = (
        batch_df
        .select(from_json(col("value").cast("string"), schema).alias("data"))
        .select("data.*")
    )

    parsed = cleaned
    for c in numeric_cols:
        parsed = parsed.withColumn(
            c,
            when(trim(col(c)) == "", None).otherwise(col(c).cast(DoubleType()))
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

    # Load model
    model = load_model("weights/lstm_vae_swat.pth", device=device)

    scores, anomaly_indices, threshold = compute_scores(
        model, dataloader, device
    )

    logger.info(f"BATCH {batch_id} — {len(scores)} scores, threshold={threshold}")

    # --------------------------------------------------------------------------
    # 🔥 PROMETHEUS METRICS UPDATE
    # --------------------------------------------------------------------------
    if scores:
        last_score = scores[-1]
        is_anomaly = 1 if len(anomaly_indices) > 0 else 0

        ae_last_anomaly_score.set(last_score)
        ae_threshold.set(threshold)
        ae_is_anomaly.set(is_anomaly)

        if is_anomaly:
            ae_anomaly_events_total.inc()

        logger.info(f"Prometheus updated: score={last_score} anom={is_anomaly}")

    # Kafka output (optional)
    # ...


# -----------------------------------------------------------------------------
# Start Spark Stream
# -----------------------------------------------------------------------------
query = (
    stream_df
    .writeStream
    .foreachBatch(lambda df, batch_id: run_eval(df, batch_id))
    .start()
)

query.awaitTermination()
