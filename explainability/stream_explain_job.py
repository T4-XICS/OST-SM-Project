# -*- coding: utf-8 -*-
"""
Stream Explainability Job

Reads SWaT JSON rows from Kafka, computes rolling features and a proxy
anomaly score, trains a RandomForest surrogate model, and publishes
SHAP-based feature importance metrics to Prometheus.

This runs as a Structured Streaming job in Spark.
"""

import os
import json
import time
from typing import List

import pandas as pd
import threading
from kafka import KafkaConsumer

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from explainability.features import rolling_features, proxy_anomaly_score
from explainability.explainer_surrogate import SurrogateExplainer
from explainability import metrics_exporter
from prometheus_client import Counter, Gauge, Histogram, start_http_server

# --------------------------------------------------------------------
# Configuration from environment (same as docker-compose)
# --------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ics-sensor-data")

PROMETHEUS_PORT = int(os.getenv("EXPLAIN_METRICS_PORT", "9109"))

# Columns we want to use as numeric sensors.
# You can extend this list based on SWaT schema.
SENSOR_COLUMNS: List[str] = [
    # put a reasonable subset here; we use a small example list
    "FIT101", "LIT101", "P101", "P102",
    "FIT201", "LIT201", "P201", "P202",
    "FIT301", "LIT301", "P301", "P302",
]

TIMESTAMP_COL = "Timestamp"          # from SWaT JSON
LABEL_COL = "Normal_Attack"          # "Normal" / "Attack" (optional)

# --------------------------------------------------------------------
# Global objects (live on the driver for foreachBatch)
# --------------------------------------------------------------------
SURROGATE = SurrogateExplainer(n_estimators=200, random_state=42)

# --------------------------------------------------------------------
# AE anomaly-score Prometheus metrics
# These will be exposed on the same /metrics endpoint that
# metrics_exporter.start(PROMETHEUS_PORT) creates.
# --------------------------------------------------------------------
ae_anomaly_events_total = Counter(
    "ae_anomaly_events_total",
    "Number of anomaly-score messages seen from AE."
)

ae_last_anomaly_score = Gauge(
    "ae_last_anomaly_score",
    "Last anomaly score received from AE (parsed as float if possible)."
)


def consume_ae_scores():
    """
    Background Kafka consumer for anomaly scores coming from Spark AE.

    It listens on topic `ics-anomaly-scores` and updates the
    ae_anomaly_events_total and ae_last_anomaly_score metrics.
    """
    try:
        consumer = KafkaConsumer(
            "ics-anomaly-scores",
            bootstrap_servers=[KAFKA_BOOTSTRAP],
            auto_offset_reset="latest",
            enable_auto_commit=True,
            group_id="explainability-ae-consumer",
            value_deserializer=lambda m: m.decode("utf-8", errors="ignore"),
        )
        print("[explainability] [ae-consumer] started, "
              "listening on topic 'ics-anomaly-scores'")
    except Exception as exc:
        print(f"[explainability] [ae-consumer] failed to start KafkaConsumer: {exc}")
        return

    for msg in consumer:
        raw = msg.value
        print(f"[explainability] [ae-consumer] got anomaly-score message: {raw!r}")
        ae_anomaly_events_total.inc()

        # try to parse the raw value as a float
        try:
            score = float(raw)
        except Exception:
            score = None

        if score is not None:
            ae_last_anomaly_score.set(score)


def parse_json_batch(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Parse the raw Kafka 'value' column (JSON strings) into a
    wide pandas DataFrame with numeric sensor columns.
    """
    if pdf.empty:
        return pdf

    parsed_rows = []
    for raw in pdf["value"]:
        try:
            obj = json.loads(raw)
            parsed_rows.append(obj)
        except Exception as exc:
            # debug malformed rows but do not crash the batch
            print(f"[explainability] JSON parse error: {exc}")
            continue

    if not parsed_rows:
        return pd.DataFrame()

    df = pd.DataFrame(parsed_rows)

    # Keep only the columns we actually care about
    cols = [c for c in SENSOR_COLUMNS if c in df.columns]
    if TIMESTAMP_COL in df.columns:
        cols = [TIMESTAMP_COL] + cols
    if LABEL_COL in df.columns:
        cols = cols + [LABEL_COL]

    df = df[cols].copy()

    # Convert numeric sensors to float
    for c in SENSOR_COLUMNS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    return df


def process_batch(sdf, batch_id: int):
    """
    foreachBatch function executed on the driver for every micro-batch.
    Converts Spark DataFrame -> pandas, computes features, trains surrogate,
    and publishes SHAP metrics to Prometheus.
    """
    start_time = time.perf_counter()

    # DEBUG: empty batch
    if sdf.rdd.isEmpty():
        print(f"[explainability] [batch {batch_id}] empty batch, skipping.")
        return

    row_count = sdf.count()
    print(f"[explainability] [batch {batch_id}] received {row_count} raw rows from Kafka.")

    # Convert only the Kafka value (JSON) to pandas
    pdf_raw = sdf.select(col("value").cast("string")).toPandas()
    df = parse_json_batch(pdf_raw)

    if df.empty:
        print(f"[explainability] [batch {batch_id}] no parsable JSON rows after parsing, skipping.")
        return

    # Drop rows with too many NaNs
    df = df.dropna(subset=[c for c in SENSOR_COLUMNS if c in df.columns])
    if df.empty:
        print(f"[explainability] [batch {batch_id}] all rows NaN after cleaning, skipping.")
        return

    # Build rolling features
    numeric_cols = [c for c in SENSOR_COLUMNS if c in df.columns]
    feats = rolling_features(df, numeric_cols, window=3)
    if feats.empty:
        print(f"[explainability] [batch {batch_id}] not enough rows for rolling window, skipping.")
        return

    # Compute proxy anomaly score (sum of |z-last|)
    scores = proxy_anomaly_score(feats)

    # Train / update surrogate model
    SURROGATE.fit(feats, scores)

    # Explain the last window
    abs_mean_map, top_pairs = SURROGATE.explain_last(feats, top_k=5)

    # Publish metrics to Prometheus (SHAP-related)
    metrics_exporter.publish_shap_mean(abs_mean_map)
    metrics_exporter.publish_topk(top_pairs)

    duration = time.perf_counter() - start_time
    print(f"[explainability] [batch {batch_id}] explained {len(feats)} samples in {duration:.3f}s")
    print(f"[explainability] [batch {batch_id}] top features: {top_pairs}")


def main():
    # DEBUG: job startup
    print("[explainability] starting stream_explain_job")
    print(f"[explainability] KAFKA_BOOTSTRAP={KAFKA_BOOTSTRAP}, topic={KAFKA_TOPIC}")
    print(f"[explainability] Prometheus metrics on port {PROMETHEUS_PORT}")

    # Start metrics HTTP server for Prometheus scraping (SHAP metrics + AE metrics)
    metrics_exporter.start(PROMETHEUS_PORT)

    # Start background Kafka consumer for AE anomaly scores
    threading.Thread(target=consume_ae_scores, daemon=True).start()

    # Create Spark session
    spark = (
        SparkSession.builder
        .appName("ExplainabilityStream")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Read raw Kafka stream (no schema; JSON handled in foreachBatch)
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .load()
    )

    # We only need the value column (JSON string)
    value_df = kafka_df.select("value")

    # Attach foreachBatch with our explainability logic
    query = (
        value_df.writeStream
        .foreachBatch(process_batch)
        .outputMode("append")   # no aggregation sink, just side effects
        .start()
    )

    print("[explainability] streaming query started, awaiting termination.")
    query.awaitTermination()


if __name__ == "__main__":
    main()
