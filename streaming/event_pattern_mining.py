#!/usr/bin/env python3
"""
event_pattern_mining.py

Standalone event-pattern-mining script.

Modes:
 - Kafka mode (default): consumes messages from Kafka topic (JSON rows)
 - File mode (--test-file path/to/SWaT_Dataset_Normal_v0_1.csv): stream rows from CSV for local testing

Behavior:
 - Detects simple example pattern: "FIT101 drops below a threshold then LIT101 rises within 60s"
 - Writes detected pattern events to InfluxDB if INFLUX_URL and INFLUX_TOKEN are provided.
 - Otherwise prints detected events to stdout.

Usage examples:
  # test locally from CSV
  python streaming/event_pattern_mining.py --test-file datasets/swat/normal/SWaT_Dataset_Normal_v0_1.csv

  # Kafka mode (requires Kafka on kafka:9092)
  export KAFKA_BOOTSTRAP=kafka:9092
  python streaming/event_pattern_mining.py

  # With Influx
  export INFLUX_URL=http://influxdb:8086
  export INFLUX_TOKEN=yourtoken
  export INFLUX_ORG=ucs
  export INFLUX_BUCKET=swat_db
  python streaming/event_pattern_mining.py
"""

import os
import json
import time
import csv
import argparse
import logging
from datetime import datetime, timedelta

# Optional dependencies; import when needed to allow file-only testing without Kafka or Influx.
try:
    from kafka import KafkaConsumer
    KAFKA_AVAILABLE = True
except Exception:
    KAFKA_AVAILABLE = False

try:
    from influxdb_client import InfluxDBClient, Point, WritePrecision
    INFLUX_AVAILABLE = True
except Exception:
    INFLUX_AVAILABLE = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("pattern-miner")

# --- Configurable thresholds & pattern params (tune as needed) ---
FIT_DROP_THRESHOLD = float(os.getenv("FIT_DROP_THRESHOLD", "10.0"))   # example: flow low
LIT_RISE_THRESHOLD = float(os.getenv("LIT_RISE_THRESHOLD", "80.0"))   # example: level high
FOLLOWUP_WINDOW_SECONDS = int(os.getenv("FOLLOWUP_WINDOW_SECONDS", "60"))

# --- Environment / service settings ---
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ics-sensor-data")
INFLUX_URL = os.getenv("INFLUX_URL", None)
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "admintoken123")
INFLUX_ORG = os.getenv("INFLUX_ORG", "ucs")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "swat_db")

# Minimal fields we need for this example
RELEVANT_FIELDS = ["Timestamp", "FIT101", "LIT101", "Normal_Attack"]

# --- Helpers -----------------------------------------------------------------
def parse_timestamp(ts_str):
    """Try multiple timestamp formats from SWaT CSV; fallback to now."""
    if ts_str is None:
        return datetime.utcnow()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y/%m/%d %H:%M:%S", "%d/%m/%Y %H:%M:%S"):
        try:
            return datetime.strptime(ts_str, fmt)
        except Exception:
            pass
    # try isoformat last
    try:
        return datetime.fromisoformat(ts_str)
    except Exception:
        return datetime.utcnow()

def safe_float(x):
    try:
        return float(x)
    except Exception:
        return None

# Influx writer (optional)
class InfluxWriter:
    def __init__(self, url, token, org, bucket):
        if not INFLUX_AVAILABLE:
            raise RuntimeError("influxdb-client not installed (pip install influxdb-client)")
        self.url = url
        self.token = token
        self.org = org
        self.bucket = bucket
        self.client = InfluxDBClient(url=self.url, token=self.token, org=self.org)
        self.write_api = self.client.write_api()

    def write_pattern_event(self, ev: dict):
        """
        ev: { pattern, sensor, severity, details, ts (datetime) }
        writes to measurement 'pattern_events'
        """
        p = Point("pattern_events") \
            .tag("pattern", ev.get("pattern", "unknown")) \
            .tag("sensor", ev.get("sensor", "")) \
            .field("severity", float(ev.get("severity", 1.0))) \
            .field("details", ev.get("details", "")) \
            .time(ev["ts"], WritePrecision.NS)
        self.write_api.write(self.bucket, self.org, p)

    def close(self):
        try:
            self.client.close()
        except Exception:
            pass

# Pattern detection logic (single-batch)
def detect_patterns_on_window(rows):
    """
    rows: list of dicts with keys: Timestamp, FIT101, LIT101, ...
    returns: list of event dicts
    Pattern implemented (example):
      - If a row has FIT101 < FIT_DROP_THRESHOLD, then within the next FOLLOWUP_WINDOW_SECONDS
        if any later row has LIT101 > LIT_RISE_THRESHOLD => produce event.
    """
    events = []
    # sort by timestamp
    rows = sorted(rows, key=lambda r: r["ts"])
    n = len(rows)
    for i in range(n):
        fit = safe_float(rows[i].get("FIT101"))
        if fit is None:
            continue
        if fit < FIT_DROP_THRESHOLD:
            t0 = rows[i]["ts"]
            # scan forward
            j = i + 1
            while j < n and (rows[j]["ts"] - t0).total_seconds() <= FOLLOWUP_WINDOW_SECONDS:
                lit = safe_float(rows[j].get("LIT101"))
                if lit is not None and lit > LIT_RISE_THRESHOLD:
                    ev = {
                        "pattern": "FIT_drop_then_LIT_rise",
                        "sensor": "FIT101->LIT101",
                        "severity": 1,
                        "details": f"FIT dropped to {fit} at {t0.isoformat()}, LIT rose to {lit} at {rows[j]['ts'].isoformat()}",
                        "ts": rows[j]["ts"]
                    }
                    events.append(ev)
                    break
                j += 1
    return events

# --- Kafka consumer loop ----------------------------------------------------
def kafka_consume_loop(influx_writer=None, poll_timeout=5, batch_size=500):
    if not KAFKA_AVAILABLE:
        log.error("kafka-python not installed. Install with `pip install kafka-python` or run in test mode.")
        return
    log.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP}, topic {KAFKA_TOPIC}")
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda v: v.decode("utf-8"),
        consumer_timeout_ms=1000
    )
    buffer = []
    try:
        while True:
            # poll for messages
            for msg in consumer:
                raw = msg.value
                try:
                    payload = json.loads(raw)
                except Exception:
                    # try CSV-line like
                    # skip if cannot parse
                    continue
                # ensure we have keys
                rec = {"ts": parse_timestamp(payload.get("Timestamp"))}
                for f in RELEVANT_FIELDS:
                    if f in payload:
                        rec[f] = payload.get(f)
                buffer.append(rec)
                if len(buffer) >= batch_size:
                    # process batch
                    events = detect_patterns_on_window(buffer)
                    for ev in events:
                        if influx_writer:
                            influx_writer.write_pattern_event(ev)
                        else:
                            log.info("EVENT: %s", ev)
                    buffer.clear()
            # end for; if loop ends (timeout), process any buffered rows
            if buffer:
                events = detect_patterns_on_window(buffer)
                for ev in events:
                    if influx_writer:
                        influx_writer.write_pattern_event(ev)
                    else:
                        log.info("EVENT: %s", ev)
                buffer.clear()
            time.sleep(0.5)
    except KeyboardInterrupt:
        log.info("Interrupted by user")
    finally:
        try:
            consumer.close()
        except Exception:
            pass
        if influx_writer:
            influx_writer.close()

# --- File (test) mode -------------------------------------------------------
def file_stream_mode(csv_path, influx_writer=None, delay=0.05):
    """
    Read CSV file and stream rows to detection pipeline in small windows.
    This is helpful to simulate near-real-time arrival.
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(csv_path)
    log.info(f"Starting file stream mode using {csv_path}")
    window = []
    last_ts = None
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=1):
            # map fields to our format, parse timestamp
            ts = row.get("Timestamp") or row.get("Time") or None
            rec = {"ts": parse_timestamp(ts)}
            for f in RELEVANT_FIELDS:
                if f in row:
                    rec[f] = row.get(f)
            # append and keep sliding window (we detect inside window)
            window.append(rec)
            # every N rows or when timestamp jumps, run detection on last 200 rows
            if len(window) >= 200 or (i % 50 == 0):
                events = detect_patterns_on_window(window)
                for ev in events:
                    if influx_writer:
                        influx_writer.write_pattern_event(ev)
                    else:
                        log.info("EVENT: %s", ev)
                # keep last FOLLOWUP_WINDOW_SECONDS worth of rows to catch cross-window patterns
                cutoff = rec["ts"] - timedelta(seconds=FOLLOWUP_WINDOW_SECONDS+5)
                window = [r for r in window if r["ts"] >= cutoff]
            # small sleep to simulate streaming
            time.sleep(delay)
    # final flush
    if window:
        events = detect_patterns_on_window(window)
        for ev in events:
            if influx_writer:
                influx_writer.write_pattern_event(ev)
            else:
                log.info("EVENT: %s", ev)
    if influx_writer:
        influx_writer.close()

# --- Argparse / entrypoint --------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Event pattern mining (Kafka or file test)")
    parser.add_argument("--test-file", "-t", help="Path to CSV file for test streaming mode")
    parser.add_argument("--batch-size", "-b", type=int, default=500, help="Kafka batch size (for buffering)")
    parser.add_argument("--poll-timeout", type=int, default=5, help="Kafka poll timeout seconds")
    parser.add_argument("--no-influx", action="store_true", help="Do not write to Influx even if token provided (print only)")
    args = parser.parse_args()

    influx_writer = None
    if not args.no_influx and INFLUX_URL and INFLUX_TOKEN:
        if not INFLUX_AVAILABLE:
            log.error("influxdb-client missing. Install with `pip install influxdb-client`")
        else:
            influx_writer = InfluxWriter(INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET)
            log.info("Influx writer initialized")

    # Test-file mode (easier to debug locally)
    if args.test_file:
        file_stream_mode(args.test_file, influx_writer=influx_writer)
        return

    # Kafka mode
    if not KAFKA_AVAILABLE:
        log.error("kafka-python not installed; install with `pip install kafka-python` or use --test-file mode")
        return

    try:
        kafka_consume_loop(influx_writer=influx_writer, poll_timeout=args.poll_timeout, batch_size=args.batch_size)
    except Exception as e:
        log.exception("Fatal error in kafka_consume_loop: %s", e)
    finally:
        if influx_writer:
            influx_writer.close()

if __name__ == "__main__":
    main()
