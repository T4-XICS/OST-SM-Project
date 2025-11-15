import logging
import os

import numpy as np
import torch

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, from_json, trim, when

from preprocess_data import preprocess_spark, create_dataloader
from network import LSTMVAE, load_model
# we keep evaluate_lstm as-is for other code, but here we use our own scoring
# from evaluate import evaluate_lstm  # not needed in this file

# -----------------------------------------------------------------------------
# Logging config
# -----------------------------------------------------------------------------
SERVICE_NAME = os.getenv("SERVICE_NAME", "spark-consumer")

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format=f"%(asctime)s %(levelname)s %(name)s app={SERVICE_NAME} %(message)s",
)
logger = logging.getLogger(SERVICE_NAME)

# -----------------------------------------------------------------------------
# Spark session
# -----------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("MockKafkaConsumerWithInference")
    .master("local[*]")
    .config("spark.hadoop.security.authentication", "simple")
    .getOrCreate()
)

# -----------------------------------------------------------------------------
# Schema: read everything as string first, then cast numerics
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
# Read stream from Kafka
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
# Local helper: compute anomaly scores + indices for a dataloader
# (copy of evaluate_lstm logic, but returns scores too)
# -----------------------------------------------------------------------------
from network import loss_function  # imported here to avoid circular confusion


def compute_anomaly_scores(model, data_loader, device, percentile_threshold: int = 90):
    """Compute reconstruction loss for each sequence and mark anomalies.

    Returns:
        scores: list of float anomaly scores (one per sequence)
        anomaly_indices: list of indices that are above the percentile threshold
        threshold: the numeric threshold used
    """
    model.eval()
    scores = []

    with torch.no_grad():
        for batch in data_loader:
            batch = torch.tensor(batch, dtype=torch.float32).to(device)

            for i in range(batch.shape[0]):
                sequence = batch[i, :, :].unsqueeze(0)
                recon_batch, mean, logvar = model(sequence)
                loss = loss_function(recon_batch, sequence, mean, logvar)
                scores.append(float(loss.item()))

    if not scores:
        return [], [], None

    threshold = float(np.percentile(scores, percentile_threshold))
    anomaly_indices = [i for i, s in enumerate(scores) if s > threshold]

    return scores, anomaly_indices, threshold


# -----------------------------------------------------------------------------
# foreachBatch function
# -----------------------------------------------------------------------------
def run_eval(batch_df, batch_id: int) -> None:
    """Run LSTM-VAE evaluation on one micro-batch coming from Kafka."""
    if batch_df.count() == 0:
        logger.info(f"Batch {batch_id} is empty. Skipping evaluation.")
        return

    logger.info(f"Batch {batch_id} RAW SAMPLE (value column)")
    batch_df.select(col("value").cast("string").alias("raw_value")).show(5, False)

    # -------------------------------------------------------------------------
    # Parse JSON into columns
    # -------------------------------------------------------------------------
    cleaned = (
        batch_df
        .select(from_json(col("value").cast("string"), schema).alias("data"))
        .select("data.*")
    )

    # Trim and cast numeric string columns -> DoubleType, treat empty strings as null
    parsed = cleaned
    for c in numeric_cols:
        parsed = parsed.withColumn(
            c,
            when(trim(col(c)) == "", None).otherwise(col(c).cast(DoubleType()))
        )

    # Trim timestamp and label
    parsed = (
        parsed.withColumn("Timestamp", trim(col("Timestamp")))
              .withColumn("Normal_Attack", trim(col("Normal_Attack")))
    )

    logger.info(f"Batch {batch_id} PARSED SAMPLE")
    parsed.show(5, False)

    total = parsed.count()
    logger.info(f"Parsed rows: {total}")
    for f in parsed.columns:
        nulls = parsed.filter(col(f).isNull()).count()
        logger.info(f"{f}: nulls={nulls}")

    # -------------------------------------------------------------------------
    # Preprocess and create dataloader
    # -------------------------------------------------------------------------
    df_pre = preprocess_spark(parsed)
    dataloader = create_dataloader(df_pre, batch_size=32, sequence_length=30)

    if dataloader is None:
        logger.info(
            f"Batch {batch_id}: not enough data to run inference yet. Skipping."
        )
        return

    df_pre.show(5, False)

    # -------------------------------------------------------------------------
    # Load model and evaluate (one per batch – small batches so OK)
    # -------------------------------------------------------------------------
    model = load_model("weights/lstm_vae_swat.pth", device=device)
    model.eval()

    scores, anomaly_indices, threshold = compute_anomaly_scores(
        model, dataloader, device, percentile_threshold=90
    )

    logger.info(f"Batch {batch_id} Evaluation")
    logger.info(f"Anomaly scores (len={len(scores)}), threshold={threshold}")
    logger.info(f"Anomaly indices: {anomaly_indices}")

    # -------------------------------------------------------------------------
    # Build a small batch DataFrame with anomaly scores and flags,
    # then send it to Kafka topic KAFKA_OUTPUT_TOPIC
    # -------------------------------------------------------------------------
    if not scores:
        logger.info(f"Batch {batch_id}: no scores computed, skipping Kafka output.")
        return

    records = []
    anomaly_index_set = set(anomaly_indices)

    for idx, score in enumerate(scores):
        record = {
            "batch_id": int(batch_id),
            "sequence_index": int(idx),
            "anomaly_score": float(score),
            "is_anomaly": 1 if idx in anomaly_index_set else 0,
            "threshold": float(threshold) if threshold is not None else None,
        }
        records.append(record)

    output_df = spark.createDataFrame(records)

    # Serialize as JSON for Kafka "value" field
    kafka_df = output_df.selectExpr("to_json(struct(*)) AS value")

    (
        kafka_df
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", KAFKA_OUTPUT_TOPIC)
        .save()
    )

    logger.info(
        f"Batch {batch_id}: sent {len(records)} anomaly-score records to topic "
        f"{KAFKA_OUTPUT_TOPIC}"
    )


# -----------------------------------------------------------------------------
# Start streaming query
# -----------------------------------------------------------------------------
query = (
    stream_df
    .writeStream
    .foreachBatch(lambda df, batch_id: run_eval(df, batch_id))
    # .option("checkpointLocation", "checkpoints/mock_consumer")
    .start()
)

query.awaitTermination()
