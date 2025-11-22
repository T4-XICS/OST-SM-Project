# -*- coding: utf-8 -*-
"""
Stream Explainability Job (Surrogate on REAL LSTM / AE scores)

1) Reads SWaT JSON rows from Kafka (ics-sensor-data),
   computes rolling features, trains a surrogate model
   that mimics the anomaly scores of the LSTM / AE model,
   and publishes SHAP-based feature importance to Prometheus.

2) Runs an "AE-bridge" Kafka consumer that
   reads anomaly-score messages from topic ics-anomaly-scores
   and feeds them into an in-memory buffer so that, for each
   micro-batch, the surrogate target y comes from the REAL
   anomaly scores (NOT from a fake proxy).

This runs as a Structured Streaming job in Spark, plus
a background thread for the AE bridge.
"""

import os
import json
import time
import threading
from typing import List, Dict
from collections import deque

import numpy as np
import pandas as pd
from kafka import KafkaConsumer

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from explainability.features import rolling_features
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

# Sliding window length (in number of batches) for SHAP window metrics
# Industrial: we keep last 50 batches in memory
SHAP_WINDOW_SIZE = int(os.getenv("EXPLAIN_SHAP_WINDOW_SIZE", "50"))

# We will compute SHAP means for these batch-window sizes
SHAP_WINDOW_SIZES = [5, 10, 20, 50]

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
# Online surrogate model (tree-based regressor + SHAP)
SURROGATE = SurrogateExplainer(n_estimators=200, random_state=42)

# Sliding window over per-batch SHAP means
# Each element is a dict: feature_name -> mean |SHAP| in that batch
SHAP_WINDOW = deque(maxlen=SHAP_WINDOW_SIZE)

# Buffer of AE / LSTM anomaly scores coming from Kafka (ics-anomaly-scores)
# We store a sequence of scalar scores; for each micro-batch we will
# consume ONE score and use it as the target for all rows in that batch.
AE_SCORES = deque(maxlen=10000)


# --------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------
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


def _update_sliding_window(abs_mean_map: Dict[str, float]) -> None:
    """
    Update global SHAP_WINDOW and publish multi-window mean via metrics_exporter.

    We keep last SHAP_WINDOW_SIZE batches in SHAP_WINDOW.
    For each window size in SHAP_WINDOW_SIZES (5, 10, 20, 50),
    we compute the mean |SHAP| over the last N batches.
    """
    # push current batch statistics into the window
    SHAP_WINDOW.append(abs_mean_map)

    if not SHAP_WINDOW:
        return

    # build multi-window mean maps
    window_means: Dict[str, Dict[str, float]] = {}

    # we use the keys of current batch as feature list
    features = list(abs_mean_map.keys())

    # SHAP_WINDOW is a deque of dicts: [{f1: v, f2: v2, ...}, {...}, ...]
    window_list = list(SHAP_WINDOW)

    for w in SHAP_WINDOW_SIZES:
        if not window_list:
            continue

        if len(window_list) >= w:
            recent = window_list[-w:]
        else:
            # not enough batches yet: use all we have
            recent = window_list

        mean_map: Dict[str, float] = {}
        for f in features:
            vals = [m.get(f, 0.0) for m in recent]
            if vals:
                mean_map[f] = float(sum(vals) / len(vals))
            else:
                mean_map[f] = 0.0

        window_means[str(w)] = mean_map

    # publish multi-window SHAP if helper is available
    if hasattr(metrics_exporter, "publish_shap_windows"):
        metrics_exporter.publish_shap_windows(window_means)


# --------------------------------------------------------------------
# foreachBatch callback
# --------------------------------------------------------------------
def process_batch(sdf, batch_id: int) -> None:
    """
    foreachBatch function executed on the driver for every micro-batch.
    Converts Spark DataFrame -> pandas, computes features, trains surrogate,
    and publishes SHAP metrics to Prometheus.

    IMPORTANT:
    - Target y comes from REAL AE / LSTM anomaly scores (AE_SCORES buffer),
      not from any local proxy.
    """
    batch_start = time.perf_counter()

    if sdf.rdd.isEmpty():
        print(f"[explainability] [batch {batch_id}] empty batch, skipping.")
        return

    row_count = sdf.count()
    print(f"[explainability] [batch {batch_id}] received {row_count} raw rows from Kafka.")

    try:
        # value column is binary -> cast to string and collect in pandas
        pdf_raw = sdf.select(col("value").cast("string")).toPandas()
        df = parse_json_batch(pdf_raw)

        if df.empty:
            print(f"[explainability] [batch {batch_id}] no parsable JSON rows after parsing, skipping.")
            return

        # drop rows where all selected sensor cols are NaN
        df = df.dropna(subset=[c for c in SENSOR_COLUMNS if c in df.columns])
        if df.empty:
            print(f"[explainability] [batch {batch_id}] all rows NaN after cleaning, skipping.")
            return

        numeric_cols = [c for c in SENSOR_COLUMNS if c in df.columns]
        feats = rolling_features(df, numeric_cols, window=3)
        if feats.empty:
            print(f"[explainability] [batch {batch_id}] not enough rows for rolling window, skipping.")
            return

        # ------------------------------------------------------------------
        # 1) Get REAL AE / LSTM anomaly score from AE_SCORES buffer
        # ------------------------------------------------------------------
        # We expect at least ONE anomaly_score per micro-batch.
        # If there is no score yet, we SKIP this batch (no fake proxy).
        if len(AE_SCORES) < 1:
            print(
                f"[explainability] [batch {batch_id}] "
                f"not enough AE scores yet (have {len(AE_SCORES)}, need 1), "
                f"skipping surrogate fit."
            )
            return

        # Take ONE score for this batch (FIFO)
        ae_score = AE_SCORES.popleft()
        print(
            f"[explainability] [batch {batch_id}] "
            f"using AE anomaly_score={ae_score} as surrogate target."
        )

        # Build target vector y: same AE score for all samples in this batch
        y = np.full(shape=(len(feats),), fill_value=float(ae_score), dtype=float)

        # ------------------------------------------------------------------
        # 2) Train / update surrogate on this batch
        # ------------------------------------------------------------------
        SURROGATE.fit(feats, y)

        # 3) Explain last sample
        abs_mean_map, top_pairs = SURROGATE.explain_last(feats, top_k=5)

        # ---- SHAP → Prometheus ----
        # 1) per-batch mean SHAP
        metrics_exporter.publish_shap_mean(abs_mean_map)

        # 2) multi-window mean SHAP (5, 10, 20, 50 batches)
        _update_sliding_window(abs_mean_map)

        # 3) top-k last-sample SHAP
        metrics_exporter.publish_topk(top_pairs)

        duration = time.perf_counter() - batch_start
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
# AE bridge: consume anomaly-score topic and fill AE_SCORES buffer
# --------------------------------------------------------------------
def _ae_bridge_loop() -> None:
    """
    Background loop:
    - consumes anomaly-score messages from KAFKA_ANOMALY_TOPIC
    - extracts "anomaly_score"
    - appends it to AE_SCORES buffer
    """
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

        # push into global AE score buffer
        AE_SCORES.append(score)

        print(
            f"[explainability][AE-bridge] buffered anomaly_score={score} "
            f"(buffer size now {len(AE_SCORES)}) "
            f"from partition={msg.partition}, offset={msg.offset}"
        )
        # AE score metrics themselves (if any) are handled in spark-consumer.


def start_ae_bridge() -> None:
    """Start AE-bridge consumer thread."""
    t = threading.Thread(target=_ae_bridge_loop, daemon=True)
    t.start()
    print("[explainability] AE bridge thread started.")


# --------------------------------------------------------------------
# main
# --------------------------------------------------------------------
def main() -> None:
    print("[explainability] starting stream_explain_job")
    print(f"[explainability] KAFKA_BOOTSTRAP={KAFKA_BOOTSTRAP}")
    print(f"[explainability] sensor topic={KAFKA_SENSOR_TOPIC}")
    print(f"[explainability] anomaly topic={KAFKA_ANOMALY_TOPIC}")
    print(f"[explainability] Prometheus metrics on port {PROMETHEUS_PORT}")
    print(f"[explainability] SHAP window size (batches) = {SHAP_WINDOW_SIZE}")
    print(f"[explainability] SHAP windows = {SHAP_WINDOW_SIZES}")

    # start Prometheus HTTP server inside the explainability container
    metrics_exporter.start(PROMETHEUS_PORT)

    # start AE bridge in background (logs + buffers anomaly scores)
    start_ae_bridge()

    # Spark session
    spark = (
        SparkSession.builder
        .appName("ExplainabilityStream")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Kafka source
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
