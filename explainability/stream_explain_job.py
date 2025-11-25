# -*- coding: utf-8 -*-
"""
Stream Explainability Job

1) Read SWaT JSON rows from Kafka (topic: ics-sensor-data),
   compute rolling features, train a surrogate model on REAL
   LSTM anomaly scores, and publish SHAP-based feature importance
   to Prometheus.

2) In parallel, read anomaly-score messages from Kafka
   (topic: ics-anomaly-scores) produced by the LSTM-VAE pipeline
   and buffer them. These REAL scores are used as the regression
   target y for the surrogate model.

High-level flow
---------------
Kafka (sensor data)  --> Spark Structured Streaming --> rolling_features()
                                                   --> SurrogateExplainer.fit(X, y)
                                                   --> SHAP (TreeExplainer)
                                                   --> Prometheus metrics
Kafka (anomaly scores) --> AE bridge thread --> AE_SCORE_BUFFER (deque)

This file is designed to run inside the "explainability" container.
"""

import json
import logging
import os
import threading
import time
from collections import deque
from typing import List

import pandas as pd
from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 🔴 مهم: این سه تا خط را عوض کردم به relative imports
from .features import rolling_features
from .explainer_surrogate import SurrogateExplainer
from .metrics_exporter import (
    start_metrics_server,
    publish_shap_mean,
    publish_topk,
)


# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
LOGGER = logging.getLogger("stream_explain")
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_SENSOR_TOPIC = os.getenv("KAFKA_SENSOR_TOPIC", "ics-sensor-data")
KAFKA_ANOMALY_TOPIC = os.getenv("KAFKA_ANOMALY_TOPIC", "ics-anomaly-scores")

# Port inside the explainability container for Prometheus metrics
PROMETHEUS_PORT = int(os.getenv("EXPLAIN_METRICS_PORT", "9109"))

# Columns we want to use as numeric sensors.
SENSOR_COLUMNS: List[str] = [
    "FIT101", "LIT101", "P101", "P102",
    "FIT201", "LIT201", "P201", "P202",
    "FIT301", "LIT301", "P301", "P302",
]

TIMESTAMP_COL = "Timestamp"          # from SWaT JSON
LABEL_COL = "Normal_Attack"          # "Normal" / "Attack" (optional)

# -----------------------------------------------------------------------------
# Global state for AE (LSTM anomaly scores)
# -----------------------------------------------------------------------------
AE_SCORE_BUFFER = deque(maxlen=10_000)
AE_LOCK = threading.Lock()

# Single global surrogate explainer instance
SURROGATE = SurrogateExplainer(n_estimators=200, random_state=42)


# -----------------------------------------------------------------------------
# Helper: parse JSON rows from Spark batch -> Pandas DataFrame
# -----------------------------------------------------------------------------
def parse_json_batch(batch_df) -> pd.DataFrame:
    """
    Convert a Spark DataFrame with a 'value' column containing JSON strings
    into a Pandas DataFrame with one column per JSON key.

    If the batch is empty or parsing fails, returns an empty DataFrame.
    """
    count = batch_df.count()
    if count == 0:
        return pd.DataFrame()

    # value is a binary column; cast to string first
    pdf = batch_df.select(col("value").cast("string").alias("value")).toPandas()
    try:
        records = [json.loads(v) for v in pdf["value"].tolist() if v]
    except Exception as exc:
        LOGGER.warning("Failed to parse JSON batch: %s", exc)
        return pd.DataFrame()

    if not records:
        return pd.DataFrame()

    return pd.DataFrame.from_records(records)


# -----------------------------------------------------------------------------
# AE bridge: consume anomaly-score topic and buffer messages
# -----------------------------------------------------------------------------
def _ae_bridge_loop(max_retries: int = 60, sleep_sec: float = 5.0) -> None:
    """
    Background loop:
      1) Tries to create a KafkaConsumer with retries
      2) On success, keeps consuming anomaly_score messages
         and appends them into AE_SCORE_BUFFER.

    This fixes the old behaviour where a single failure while
    Kafka was still starting killed the AE-bridge forever.
    """
    LOGGER.info(
        "[AE-bridge] starting consumer on topic=%s, bootstrap=%s",
        KAFKA_ANOMALY_TOPIC,
        KAFKA_BOOTSTRAP,
    )

    consumer = None
    attempt = 0

    # Retry connecting to Kafka
    while consumer is None and attempt < max_retries:
        attempt += 1
        try:
            LOGGER.info(
                "[AE-bridge] creating KafkaConsumer (attempt %d/%d)",
                attempt,
                max_retries,
            )
            consumer = KafkaConsumer(
                KAFKA_ANOMALY_TOPIC,
                bootstrap_servers=[KAFKA_BOOTSTRAP],
                auto_offset_reset="latest",
                enable_auto_commit=True,
                group_id="explainability-ae-bridge",
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )
        except Exception as exc:  # defensive logging
            LOGGER.warning(
                "[AE-bridge] FAILED to create KafkaConsumer: %s "
                "(attempt %d/%d) – retrying in %ds",
                exc,
                attempt,
                max_retries,
                sleep_sec,
            )
            time.sleep(sleep_sec)

    if consumer is None:
        LOGGER.error(
            "[AE-bridge] giving up after %d attempts. "
            "AE bridge will not receive LSTM scores.",
            max_retries,
        )
        return

    LOGGER.info(
        "[AE-bridge] KafkaConsumer successfully created, "
        "starting to consume anomaly_score messages...",
    )

    # Main consumption loop
    for msg in consumer:
        payload = msg.value
        score_raw = None

        if isinstance(payload, dict):
            score_raw = payload.get("anomaly_score")
        else:
            score_raw = payload

        try:
            score = float(score_raw)
        except Exception:
            LOGGER.warning(
                "[AE-bridge] could not parse anomaly_score from payload=%r",
                payload,
            )
            continue

        with AE_LOCK:
            AE_SCORE_BUFFER.append(score)
            buf_size = len(AE_SCORE_BUFFER)

        LOGGER.debug(
            "[AE-bridge] got anomaly_score=%f from partition=%d offset=%d "
            "(buffer_size=%d)",
            score,
            msg.partition,
            msg.offset,
            buf_size,
        )


def start_ae_bridge() -> None:
    """Start AE-bridge consumer thread."""
    t = threading.Thread(target=_ae_bridge_loop, daemon=True)
    t.start()
    LOGGER.info("AE bridge thread started.")


# -----------------------------------------------------------------------------
# foreachBatch handler for Spark Structured Streaming
# -----------------------------------------------------------------------------
def process_batch(batch_df, batch_id: int) -> None:
    """
    Called by Spark for each micro-batch.

    Steps:
      1) Parse Kafka JSON into Pandas DataFrame
      2) Compute rolling features
      3) Align with latest LSTM scores from AE_SCORE_BUFFER
      4) Train / update surrogate on (X, y)
      5) Explain last window via SHAP
      6) Publish metrics to Prometheus
    """
    LOGGER.info("[batch %d] received %d raw rows from Kafka.", batch_id, batch_df.count())
    df_raw = parse_json_batch(batch_df)

    if df_raw.empty:
        LOGGER.info("[batch %d] empty after JSON parsing, skipping.", batch_id)
        return

    # Keep only the sensor columns that exist in df_raw
    cols = [c for c in SENSOR_COLUMNS if c in df_raw.columns]
    if not cols:
        LOGGER.warning("[batch %d] none of SENSOR_COLUMNS present, skipping.", batch_id)
        return

    feats = rolling_features(df_raw, cols=cols, window=3)
    if feats is None or feats.empty:
        LOGGER.info("[batch %d] rolling_features produced empty frame, skipping.", batch_id)
        return

    LOGGER.info("[batch %d] feats size after rolling = %d", batch_id, len(feats))

    # ------------------------------------------------------------------
    # Align features with available LSTM scores
    # ------------------------------------------------------------------
    with AE_LOCK:
        buffer_len = len(AE_SCORE_BUFFER)
        if buffer_len == 0:
            LOGGER.info(
                "[batch %d] no LSTM scores yet (buffer_len=0), skipping.", batch_id
            )
            return

        n = min(buffer_len, len(feats))
        # take the most recent n scores / rows
        batch_scores = list(AE_SCORE_BUFFER)[-n:]
        feats_aligned = feats.tail(n).copy()

    y = pd.Series(batch_scores)
    X = feats_aligned

    LOGGER.info(
        "[batch %d] using %d samples for surrogate training (buffer_len=%d).",
        batch_id,
        len(X),
        buffer_len,
    )

    if len(X) < 1:
        LOGGER.info("[batch %d] not enough samples after alignment, skipping.", batch_id)
        return

    # ------------------------------------------------------------------
    # Train / update surrogate + compute SHAP
    # ------------------------------------------------------------------
    try:
        start_t = time.time()
        SURROGATE.fit(X, y)
        mean_map, top_pairs = SURROGATE.explain_last(X, top_k=5)
        duration = time.time() - start_t
    except Exception as exc:
        LOGGER.exception("[batch %d] error in surrogate / SHAP: %s", batch_id, exc)
        return

    LOGGER.info(
        "[batch %d] explained %d samples in %.3fs",
        batch_id,
        len(X),
        duration,
    )
    LOGGER.info("[batch %d] top features: %s", batch_id, top_pairs)

    # ------------------------------------------------------------------
    # Publish to Prometheus
    # ------------------------------------------------------------------
    try:
        if mean_map:
            publish_shap_mean(mean_map)
        if top_pairs:
            publish_topk(top_pairs)
    except Exception as exc:
        LOGGER.exception("[batch %d] failed to publish metrics: %s", batch_id, exc)


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main() -> None:
    LOGGER.info("Starting explainability streaming job.")
    LOGGER.info("Kafka bootstrap: %s", KAFKA_BOOTSTRAP)
    LOGGER.info("Sensor topic: %s", KAFKA_SENSOR_TOPIC)
    LOGGER.info("Anomaly topic: %s", KAFKA_ANOMALY_TOPIC)
    LOGGER.info("Prometheus metrics port: %d", PROMETHEUS_PORT)

    # Start Prometheus exporter
    # Start Prometheus exporter correctly
    start_metrics_server(PROMETHEUS_PORT)
    LOGGER.info("Prometheus metrics exporter started on port %d.", PROMETHEUS_PORT)

    # Start AE bridge in background
    start_ae_bridge()

    # Spark session
    spark = (
        SparkSession.builder
        .appName("ExplainabilityStreamJob")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Kafka source
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_SENSOR_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    value_df = raw_df.select(col("value"))

    query = (
        value_df.writeStream
        .foreachBatch(process_batch)
        .outputMode("append")
        .start()
    )

    LOGGER.info("Streaming query started, awaiting termination.")
    query.awaitTermination()


if __name__ == "__main__":
    main()
