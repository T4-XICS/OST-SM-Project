# -*- coding: utf-8 -*-
"""
Stream Explainability Job

1) Reads SWaT JSON rows from Kafka (ics-sensor-data),
   computes rolling features, trains a surrogate model,
   publishes SHAP-based feature importance to Prometheus.

2) Runs an "AE-bridge" Kafka consumer that
   reads anomaly-score messages from topic ics-anomaly-scores
   and LOGS them for debugging.
   (AE metrics themselves are exported by the spark-consumer service.)

This runs as a Structured Streaming job in Spark, plus
a background thread for the AE bridge.
"""

import os
import json
import time
import threading
from typing import List

import pandas as pd
from kafka import KafkaConsumer

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from explainability.features import rolling_features, proxy_anomaly_score
from explainability.explainer_surrogate import SurrogateExplainer
from explainability import metrics_exporter


# --------------------------------------------------------------------
# Configuration from environment (same as docker-compose)
# --------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_SENSOR_TOPIC = os.getenv("KAFKA_TOPIC", "ics-sensor-data")
KAFKA_ANOMALY_TOPIC = os.getenv("ANOMALY_TOPIC", "ics-anomaly-scores")

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

# --------------------------------------------------------------------
# Global objects (live on the driver for foreachBatch)
# --------------------------------------------------------------------
SURROGATE = SurrogateExplainer(n_estimators=200, random_state=42)


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
            print(f"[explainability] JSON parse error: {exc}")
            continue

    if not parsed_rows:
        return pd.DataFrame()

    df = pd.DataFrame(parsed_rows)

    cols = [c for c in SENSOR_COLUMNS if c in df.columns]
    if TIMESTAMP_COL in df.columns:
        cols = [TIMESTAMP_COL] + cols
    if LABEL_COL in df.columns:
        cols = cols + [LABEL_COL]

    df = df[cols].copy()

    # convert numeric sensors to float
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

    if sdf.rdd.isEmpty():
        print(f"[explainability] [batch {batch_id}] empty batch, skipping.")
        return

    row_count = sdf.count()
    print(f"[explainability] [batch {batch_id}] received {row_count} raw rows from Kafka.")

    try:
        pdf_raw = sdf.select(col("value").cast("string")).toPandas()
        df = parse_json_batch(pdf_raw)

        if df.empty:
            print(f"[explainability] [batch {batch_id}] no parsable JSON rows after parsing, skipping.")
            return

        df = df.dropna(subset=[c for c in SENSOR_COLUMNS if c in df.columns])
        if df.empty:
            print(f"[explainability] [batch {batch_id}] all rows NaN after cleaning, skipping.")
            return

        numeric_cols = [c for c in SENSOR_COLUMNS if c in df.columns]
        feats = rolling_features(df, numeric_cols, window=3)
        if feats.empty:
            print(f"[explainability] [batch {batch_id}] not enough rows for rolling window, skipping.")
            return

        # proxy anomaly score (not the AE score, just local one for surrogate)
        scores = proxy_anomaly_score(feats)

        # train / update surrogate on this batch
        SURROGATE.fit(feats, scores)

        # explain last sample
        abs_mean_map, top_pairs = SURROGATE.explain_last(feats, top_k=5)

        # ---- SHAP → Prometheus ----
        metrics_exporter.publish_shap_mean(abs_mean_map)
        metrics_exporter.publish_topk(top_pairs)

        duration = time.perf_counter() - start_time
        print(f"[explainability] [batch {batch_id}] explained {len(feats)} samples in {duration:.3f}s")
        print(f"[explainability] [batch {batch_id}] top features: {top_pairs}")

        # optional latency metric, if defined
        if hasattr(metrics_exporter, "HIST_LATENCY"):
            metrics_exporter.HIST_LATENCY.observe(duration)

    except Exception as exc:
        print(f"[explainability] [batch {batch_id}] ERROR during SHAP pipeline: {exc}")
        if hasattr(metrics_exporter, "COUNTER_ERR"):
            metrics_exporter.COUNTER_ERR.inc()


# --------------------------------------------------------------------
# AE bridge: consume anomaly-score topic and log messages
# (AE metrics are exported by spark-consumer, not here.)
# --------------------------------------------------------------------
def _ae_bridge_loop():
    """Background loop: consumes anomaly-score messages and logs them."""
    print(
        f"[explainability][AE-bridge] starting consumer on "
        f"topic={KAFKA_ANOMALY_TOPIC}, bootstrap={KAFKA_BOOTSTRAP}"
    )

    try:
        consumer = KafkaConsumer(
            KAFKA_ANOMALY_TOPIC,
            bootstrap_servers=[KAFKA_BOOTSTRAP],
            auto_offset_reset="latest",
            enable_auto_commit=True,
            group_id="explainability-ae-bridge",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )
    except Exception as exc:
        print(f"[explainability][AE-bridge] FAILED to create KafkaConsumer: {exc}")
        return

    for msg in consumer:
        payload = msg.value
        score_raw = None

        if isinstance(payload, dict):
            # expected keys from spark-consumer: "anomaly_score"
            score_raw = payload.get("anomaly_score")
        else:
            # fallback: treat value as plain number
            score_raw = payload

        try:
            score = float(score_raw)
        except Exception:
            print(f"[explainability][AE-bridge] could not parse anomaly_score from payload={payload!r}")
            continue

        print(
            f"[explainability][AE-bridge] got anomaly_score={score} "
            f"from partition={msg.partition}, offset={msg.offset}"
        )
        # metrics for AE are handled in spark-consumer exporter now.


def start_ae_bridge():
    """Start AE-bridge consumer thread."""
    t = threading.Thread(target=_ae_bridge_loop, daemon=True)
    t.start()
    print("[explainability] AE bridge thread started.")


# --------------------------------------------------------------------
# main
# --------------------------------------------------------------------
def main():
    print("[explainability] starting stream_explain_job")
    print(f"[explainability] KAFKA_BOOTSTRAP={KAFKA_BOOTSTRAP}")
    print(f"[explainability] sensor topic={KAFKA_SENSOR_TOPIC}")
    print(f"[explainability] anomaly topic={KAFKA_ANOMALY_TOPIC}")
    print(f"[explainability] Prometheus metrics on port {PROMETHEUS_PORT}")

    # start Prometheus HTTP server INSIDE the explainability container
    metrics_exporter.start(PROMETHEUS_PORT)

    # start AE bridge in background (logs anomaly scores)
    start_ae_bridge()

    # Spark session
    spark = (
        SparkSession.builder
        .appName("ExplainabilityStream")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_SENSOR_TOPIC)
        .load()
    )

    value_df = kafka_df.select("value")

    query = (
        value_df.writeStream
        .foreachBatch(process_batch)
        .outputMode("append")
        .start()
    )

    print("[explainability] streaming query started, awaiting termination.")
    query.awaitTermination()


if __name__ == "__main__":
    main()
