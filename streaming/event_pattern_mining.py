#!/usr/bin/env python3
"""
event_pattern_mining.py

Streaming event-pattern mining for ICS data.

Modes
-----
- Kafka mode (default):
    Consumes JSON rows from a Kafka topic and detects short temporal patterns.
- File mode (--test-file path/to/SWaT_Dataset_Normal_v0_1.csv):
    Streams rows from CSV to simulate near-real-time arrival.

Current example pattern
-----------------------
"FIT101 drops below a threshold, then within N seconds LIT101 rises above
another threshold" -> pattern ID: FIT_drop_then_LIT_rise

Detected pattern events are either:
- written to InfluxDB (measurement: pattern_events), or
- logged to stdout when Influx is disabled.

Usage examples
--------------
# Test locally from CSV
python event_pattern_mining.py \
    --test-file datasets/swat/normal/SWaT_Dataset_Normal_v0_1.csv

# Kafka mode (requires Kafka on kafka:9092)
export KAFKA_BOOTSTRAP=kafka:9092
python event_pattern_mining.py

# With Influx
export INFLUX_URL=http://influxdb:8086
export INFLUX_TOKEN=yourtoken
export INFLUX_ORG=ucs
export INFLUX_BUCKET=swat_db
python event_pattern_mining.py

# Auto-tune thresholds from normal CSV, then run
python event_pattern_mining.py \
    --tune-thresholds-from datasets/swat/normal/SWaT_Dataset_Normal_v0_1.csv \
    --test-file datasets/swat/normal/SWaT_Dataset_Normal_v0_1.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional

# Optional dependencies; import when available
try:
    from kafka import KafkaConsumer
    KAFKA_AVAILABLE = True
except Exception:
    KafkaConsumer = None
    KAFKA_AVAILABLE = False

try:
    from influxdb_client import InfluxDBClient, Point, WritePrecision
    INFLUX_AVAILABLE = True
except Exception:
    InfluxDBClient = Point = WritePrecision = None  # type: ignore
    INFLUX_AVAILABLE = False


# --------------------------------------------------------------------------- #
# Logging
# --------------------------------------------------------------------------- #

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
)
log = logging.getLogger("pattern-miner")


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

# Thresholds and pattern parameters (tunable via env vars)
FIT_DROP_THRESHOLD: float = float(os.getenv("FIT_DROP_THRESHOLD", "3.0"))   # flow low
LIT_RISE_THRESHOLD: float = float(os.getenv("LIT_RISE_THRESHOLD", "512.0"))   # level high
FOLLOWUP_WINDOW_SECONDS: int = int(os.getenv("FOLLOWUP_WINDOW_SECONDS", "60"))

# Extra knobs for more robust pattern logic
MIN_FIT_DROP: float = float(os.getenv("MIN_FIT_DROP", "0.1"))       # min drop in FIT101
MIN_LIT_RISE: float = float(os.getenv("MIN_LIT_RISE", "1.0"))       # min rise in LIT101
COOLDOWN_SECONDS: int = int(os.getenv("COOLDOWN_SECONDS", "60"))    # cooldown between events

# Kafka settings
KAFKA_BOOTSTRAP: str = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", "ics-sensor-data")

# InfluxDB settings
INFLUX_URL: Optional[str] = os.getenv("INFLUX_URL")
INFLUX_TOKEN: Optional[str] = os.getenv("INFLUX_TOKEN")
INFLUX_ORG: str = os.getenv("INFLUX_ORG", "ucs")
INFLUX_BUCKET: str = os.getenv("INFLUX_BUCKET", "swat_db")

# Minimal fields required from the stream / CSV
RELEVANT_FIELDS: List[str] = ["Timestamp", "FIT101", "LIT101", "Normal_Attack"]


# --------------------------------------------------------------------------- #
# Data structures
# --------------------------------------------------------------------------- #

@dataclass
class PatternEvent:
    """Structured representation of a detected pattern event."""
    pattern: str
    sensor: str
    severity: float
    details: str
    ts: datetime

    def to_influx_point(self) -> "Point":
        """Convert the event to an InfluxDB Point."""
        if not INFLUX_AVAILABLE or Point is None or WritePrecision is None:
            raise RuntimeError("InfluxDB client libraries are not available.")
        return (
            Point("pattern_events")
            .tag("pattern", self.pattern)
            .tag("sensor", self.sensor)
            .field("severity", float(self.severity))
            .field("details", self.details)
            .time(self.ts, WritePrecision.NS)
        )


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def parse_timestamp(ts_str: Optional[str]) -> datetime:
    """Parse a SWaT-style timestamp, falling back to UTC now on failure."""
    if not ts_str:
        return datetime.utcnow()

    formats = (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
    )
    for fmt in formats:
        try:
            return datetime.strptime(ts_str, fmt)
        except Exception:
            continue

    # Try ISO format, then fallback
    try:
        return datetime.fromisoformat(ts_str)
    except Exception:
        log.debug("Failed to parse timestamp %r, using UTC now.", ts_str)
        return datetime.utcnow()


def safe_float(x: object) -> Optional[float]:
    """Convert to float; return None on failure."""
    try:
        return float(x)  # type: ignore[arg-type]
    except Exception:
        return None


def _quantile(sorted_vals: List[float], q: float) -> Optional[float]:
    """
    Simple quantile (q in [0,1]) without numpy.
    Assumes sorted_vals is non-empty.
    """
    if not sorted_vals:
        return None
    n = len(sorted_vals)
    pos = q * (n - 1)
    lo = int(pos)
    hi = min(lo + 1, n - 1)
    frac = pos - lo
    return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac


def compute_thresholds_from_csv(
    csv_path: str,
    fit_low_q: float = 0.05,
    lit_high_q: float = 0.95,
    drop_frac: float = 0.10,
    rise_frac: float = 0.10,
) -> (float, float, float, float):
    """
    Compute data-driven thresholds from a 'normal' SWaT CSV.

    - FIT_DROP_THRESHOLD ≈ low quantile of FIT101
    - LIT_RISE_THRESHOLD ≈ high quantile of LIT101
    - MIN_FIT_DROP, MIN_LIT_RISE ≈ fractions of their ranges

    Returns
    -------
    fit_drop_th, lit_rise_th, min_fit_drop, min_lit_rise
    """
    fit_vals: List[float] = []
    lit_vals: List[float] = []

    if not os.path.exists(csv_path):
        raise FileNotFoundError(csv_path)

    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # If Normal_Attack column is present, use only normal rows
            label = row.get("Normal_Attack")
            if label is not None:
                lab = label.strip().lower()
                if lab not in ("normal", "0", ""):
                    continue

            fit = safe_float(row.get("FIT101"))
            lit = safe_float(row.get("LIT101"))
            if fit is not None:
                fit_vals.append(fit)
            if lit is not None:
                lit_vals.append(lit)

    if not fit_vals or not lit_vals:
        raise RuntimeError("No FIT101/LIT101 values found for threshold tuning.")

    fit_vals.sort()
    lit_vals.sort()

    fit_low = _quantile(fit_vals, fit_low_q)
    lit_high = _quantile(lit_vals, lit_high_q)

    fit_min, fit_max = fit_vals[0], fit_vals[-1]
    lit_min, lit_max = lit_vals[0], lit_vals[-1]

    fit_range = fit_max - fit_min
    lit_range = lit_max - lit_min

    min_fit_drop = drop_frac * fit_range
    min_lit_rise = rise_frac * lit_range

    if fit_low <= 1.0 or fit_low > 5.0:
        fit_low = 3.0

    if lit_high < 400 or lit_high > 600:
        lit_high = 512.0

    if min_fit_drop < 0.05:
        min_fit_drop = 0.1

    if min_lit_rise < 0.5:
        min_lit_rise = 1.0
        
    # Type ignore is safe because we validated non-empty lists
    return float(fit_low), float(lit_high), float(min_fit_drop), float(min_lit_rise)


# --------------------------------------------------------------------------- #
# InfluxDB writer
# --------------------------------------------------------------------------- #

class InfluxWriter:
    """Thin wrapper around InfluxDB client for writing pattern events."""

    def __init__(self, url: str, token: str, org: str, bucket: str) -> None:
        if not INFLUX_AVAILABLE or InfluxDBClient is None:
            raise RuntimeError(
                "influxdb-client not installed. Install with `pip install influxdb-client`."
            )
        self.url = url
        self.token = token
        self.org = org
        self.bucket = bucket
        self.client = InfluxDBClient(url=self.url, token=self.token, org=self.org)
        self.write_api = self.client.write_api()

    def write_pattern_event(self, ev: PatternEvent) -> None:
        point = ev.to_influx_point()
        self.write_api.write(self.bucket, self.org, point)

    def close(self) -> None:
        try:
            self.client.close()
        except Exception:
            pass


# --------------------------------------------------------------------------- #
# Pattern detection logic
# --------------------------------------------------------------------------- #

def detect_patterns_on_window(rows: List[Dict[str, object]]) -> List[PatternEvent]:
    """
    Detect FIT_drop_then_LIT_rise events in a sliding window.

    Improved pattern:
      - FIT101 makes a significant drop from previous value
        and falls below FIT_DROP_THRESHOLD
      - Within FOLLOWUP_WINDOW_SECONDS, LIT101 makes a significant rise
        and crosses LIT_RISE_THRESHOLD
      - Cooldown to avoid spamming multiple events for the same episode
    """
    events: List[PatternEvent] = []
    if not rows:
        return events

    # Ensure temporal order
    rows = sorted(rows, key=lambda r: r["ts"])  # type: ignore[index]
    n = len(rows)

    last_event_ts: Optional[datetime] = None  # cooldown anchor

    for i in range(1, n):  # start from 1 to have a previous sample
        fit_prev = safe_float(rows[i - 1].get("FIT101"))
        fit_now = safe_float(rows[i].get("FIT101"))
        if fit_prev is None or fit_now is None:
            continue

        # Check for a meaningful drop
        fit_drop = fit_prev - fit_now
        if not (fit_now < FIT_DROP_THRESHOLD and fit_drop >= MIN_FIT_DROP):
            continue

        t0: datetime = rows[i]["ts"]  # type: ignore[assignment]

        # Cooldown: skip if we recently fired an event
        if last_event_ts is not None:
            if (t0 - last_event_ts).total_seconds() < COOLDOWN_SECONDS:
                continue

        # Value of LIT at (or near) drop time, for delta
        lit_at_drop = safe_float(rows[i].get("LIT101"))
        cutoff = t0 + timedelta(seconds=FOLLOWUP_WINDOW_SECONDS)

        j = i + 1
        while j < n and rows[j]["ts"] <= cutoff:  # type: ignore[index]
            lit_now = safe_float(rows[j].get("LIT101"))
            if lit_now is not None:
                lit_delta = (lit_now - lit_at_drop) if lit_at_drop is not None else 0.0
                if lit_now > LIT_RISE_THRESHOLD and lit_delta >= MIN_LIT_RISE:
                    ev = PatternEvent(
                        pattern="FIT_drop_then_LIT_rise",
                        sensor="FIT101->LIT101",
                        severity=1.0,
                        details=(
                            f"FIT dropped from {fit_prev} to {fit_now} at {t0.isoformat()}, "
                            f"LIT rose to {lit_now} (Δ={lit_delta:.2f}) "
                            f"at {rows[j]['ts'].isoformat()}"  # type: ignore[index]
                        ),
                        ts=rows[j]["ts"],  # type: ignore[index]
                    )
                    events.append(ev)
                    last_event_ts = rows[j]["ts"]  # type: ignore[index]
                    break  # only first follow-up match per drop
            j += 1

    return events


# --------------------------------------------------------------------------- #
# Kafka consumer loop
# --------------------------------------------------------------------------- #

def _process_buffer(
    buffer: List[Dict[str, object]],
    influx_writer: Optional[InfluxWriter],
) -> None:
    """Run pattern detection on buffer and emit events."""
    events = detect_patterns_on_window(buffer)
    for ev in events:
        if influx_writer:
            influx_writer.write_pattern_event(ev)
        else:
            log.info("EVENT: %s", ev)


def kafka_consume_loop(
    influx_writer: Optional[InfluxWriter],
    poll_timeout: int = 5,
    batch_size: int = 500,
) -> None:
    """Consume from Kafka, buffer rows, and run pattern detection."""
    if not KAFKA_AVAILABLE or KafkaConsumer is None:
        log.error(
            "kafka-python not installed. Install with `pip install kafka-python` "
            "or run in --test-file mode."
        )
        return

    log.info("Connecting to Kafka at %s, topic=%s", KAFKA_BOOTSTRAP, KAFKA_TOPIC)
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda v: v.decode("utf-8"),
        consumer_timeout_ms=poll_timeout * 1000,
    )

    buffer: List[Dict[str, object]] = []

    try:
        while True:
            # Iterate over messages; iterator ends after consumer_timeout_ms
            for msg in consumer:
                raw = msg.value
                try:
                    payload = json.loads(raw)
                except Exception:
                    # Optionally parse CSV-style payloads here
                    continue

                rec: Dict[str, object] = {
                    "ts": parse_timestamp(
                        payload.get("Timestamp") or payload.get("Time")
                    )
                }
                for f in RELEVANT_FIELDS:
                    if f in payload:
                        rec[f] = payload[f]
                buffer.append(rec)

                if len(buffer) >= batch_size:
                    _process_buffer(buffer, influx_writer)
                    buffer.clear()

            # If we time out but still have data, process it
            if buffer:
                _process_buffer(buffer, influx_writer)
                buffer.clear()

            time.sleep(0.5)

    except KeyboardInterrupt:
        log.info("Interrupted by user; shutting down.")
    finally:
        try:
            consumer.close()
        except Exception:
            pass
        if influx_writer:
            influx_writer.close()


# --------------------------------------------------------------------------- #
# File (test) mode
# --------------------------------------------------------------------------- #

def file_stream_mode(
    csv_path: str,
    influx_writer: Optional[InfluxWriter],
    delay: float = 0.05,
) -> None:
    """
    Read a CSV file and stream rows through the detector.

    This simulates near-real-time behavior for debugging and experiments.
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(csv_path)

    log.info("Starting file stream mode using %s", csv_path)
    window: List[Dict[str, object]] = []

    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=1):
            ts_str = row.get("Timestamp") or row.get("Time")
            rec: Dict[str, object] = {"ts": parse_timestamp(ts_str)}
            for fld in RELEVANT_FIELDS:
                if fld in row:
                    rec[fld] = row[fld]
            window.append(rec)

            # Periodically run detection on the sliding window
            if len(window) >= 200 or (i % 50 == 0):
                _process_buffer(window, influx_writer)

                # Keep only last FOLLOWUP_WINDOW_SECONDS worth of rows
                cutoff = rec["ts"] - timedelta(seconds=FOLLOWUP_WINDOW_SECONDS + 5)  # type: ignore[operator]
                window = [r for r in window if r["ts"] >= cutoff]  # type: ignore[index]

            time.sleep(delay)

    # Final flush
    if window:
        _process_buffer(window, influx_writer)

    if influx_writer:
        influx_writer.close()


# --------------------------------------------------------------------------- #
# CLI entrypoint
# --------------------------------------------------------------------------- #

def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Streaming event pattern mining for ICS data."
    )
    parser.add_argument(
        "--test-file", "-t",
        help="Path to CSV file for test streaming mode.",
    )
    parser.add_argument(
        "--batch-size", "-b",
        type=int,
        default=500,
        help="Kafka batch size for buffering events.",
    )
    parser.add_argument(
        "--poll-timeout",
        type=int,
        default=5,
        help="Kafka poll timeout in seconds (consumer_timeout_ms).",
    )
    parser.add_argument(
        "--no-influx",
        action="store_true",
        help="Do not write to InfluxDB even if INFLUX_URL and INFLUX_TOKEN are set.",
    )
    parser.add_argument(
        "--tune-thresholds-from",
        help="Path to NORMAL SWaT CSV; auto-tune FIT/LIT thresholds from data and use them.",
    )
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()

    # Optional: auto-tune thresholds from a normal CSV
    if args.tune_thresholds_from:
        global FIT_DROP_THRESHOLD, LIT_RISE_THRESHOLD, MIN_FIT_DROP, MIN_LIT_RISE
        fit_th, lit_th, min_fit_drop, min_lit_rise = compute_thresholds_from_csv(
            args.tune_thresholds_from
        )
        FIT_DROP_THRESHOLD = fit_th
        LIT_RISE_THRESHOLD = lit_th
        MIN_FIT_DROP = min_fit_drop
        MIN_LIT_RISE = min_lit_rise
        log.info(
            "Auto-tuned thresholds from %s: FIT_DROP_THRESHOLD=%.3f, "
            "LIT_RISE_THRESHOLD=%.3f, MIN_FIT_DROP=%.3f, MIN_LIT_RISE=%.3f",
            args.tune_thresholds_from,
            FIT_DROP_THRESHOLD,
            LIT_RISE_THRESHOLD,
            MIN_FIT_DROP,
            MIN_LIT_RISE,
        )

    influx_writer: Optional[InfluxWriter] = None
    if not args.no_influx and INFLUX_URL and INFLUX_TOKEN:
        if not INFLUX_AVAILABLE:
            log.error("influxdb-client missing. Install with `pip install influxdb-client`")
        else:
            try:
                influx_writer = InfluxWriter(
                    url=INFLUX_URL,
                    token=INFLUX_TOKEN,
                    org=INFLUX_ORG,
                    bucket=INFLUX_BUCKET,
                )
                log.info("Influx writer initialized at %s", INFLUX_URL)
            except Exception as e:
                log.error("Failed to initialize Influx writer: %s", e)
                influx_writer = None

    # File mode (easier to debug locally)
    if args.test_file:
        file_stream_mode(args.test_file, influx_writer=influx_writer)
        return

    # Kafka mode
    if not KAFKA_AVAILABLE:
        log.error(
            "Kafka mode requested but kafka-python is not available. "
            "Install with `pip install kafka-python` or use --test-file."
        )
        return

    try:
        kafka_consume_loop(
            influx_writer=influx_writer,
            poll_timeout=args.poll_timeout,
            batch_size=args.batch_size,
        )
    except Exception as e:
        log.exception("Fatal error in kafka_consume_loop: %s", e)
    finally:
        if influx_writer:
            influx_writer.close()


if __name__ == "__main__":
    main()
