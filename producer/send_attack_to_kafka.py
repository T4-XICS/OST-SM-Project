#!/usr/bin/env python3
"""
Safe attack CSV streamer to Kafka.

Usage (host):
  python producer/send_attack_to_kafka_safe.py --csv datasets/swat/attack/SWaT_Dataset_Attack_v0_1.csv --bootstrap localhost:9092

Usage (inside Docker container):
  python producer/send_attack_to_kafka_safe.py --csv /app/SWaT_Dataset_Attack_v0_1.csv --bootstrap kafka:9092
"""
import os
import csv
import json
import time
import argparse
import sys
from kafka import KafkaProducer

DEFAULT_TOPIC = "ics-sensor-data-attack"

def create_producer(bootstrap_servers, request_timeout_ms=30000, max_request_size=10 * 1024 * 1024):
    """
    max_request_size is client-side guard; set to a value lower than broker allowed max.
    """
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        request_timeout_ms=request_timeout_ms,
        max_request_size=max_request_size,
        linger_ms=0
    )

def wait_for_metadata(producer, topic, timeout=15):
    """Try to fetch partitions_for(topic) until non-None or timeout."""
    t0 = time.time()
    last = None
    while True:
        try:
            parts = producer.partitions_for(topic)
            if parts is not None:
                print("Connected to broker; topic metadata available:", parts)
                return
        except Exception as e:
            last = e
        if time.time() - t0 > timeout:
            raise RuntimeError(
                f"Unable to fetch metadata for topic '{topic}' after {timeout}s. "
                f"Bootstrap servers: {producer.config.get('bootstrap_servers')}. "
                "Check broker, listeners, and advertised.listeners."
            ) from last
        time.sleep(1)

def safe_send(producer, topic, obj, max_bytes=50 * 1024 * 1024):
    """Serialize and check size before sending. Returns True if sent, False if skipped."""
    try:
        # Check the size of serialized payload without sending bytes
        size = len(json.dumps(obj, ensure_ascii=False).encode("utf-8"))
    except Exception as e:
        print(f"[ERROR] JSON serialization failed: {e}", file=sys.stderr)
        return False

    if size > max_bytes:
        print(f"[SKIP] payload too large ({size} bytes) — skipping send to {topic}", file=sys.stderr)
        return False

    future = producer.send(topic, value=obj)  # <- send dict directly
    try:
        future.get(timeout=10)
        return True
    except Exception as e:
        print(f"[ERROR] Failed to deliver message: {e}", file=sys.stderr)
        return False


def stream_csv(csv_path, producer, topic, rate_seconds, max_payload_bytes):
    print(f"Streaming CSV: {csv_path} -> topic: {topic} (rate {rate_seconds}s/row)")
    sent = 0
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=1):
            ok = safe_send(producer, topic, row, max_bytes=max_payload_bytes)
            preview = ", ".join([f"{k}={v}" for k, v in list(row.items())[:3]])
            status = "SENT" if ok else "SKIP/ERR"
            print(f"[{i:05}] {status} | {preview} ...")
            if ok:
                sent += 1
            if i % 100 == 0:
                producer.flush()
            if rate_seconds and rate_seconds > 0:
                time.sleep(rate_seconds)
    producer.flush()
    print(f"Finished. Total rows successfully sent: {sent}")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--csv", default=os.environ.get("ATTACK_CSV", "datasets/swat/attack/SWaT_Dataset_Attack_v0_1.csv"))
    p.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", None),
                   help="Bootstrap server(s). If not provided auto-detects 'kafka' in Docker or 'localhost'.")
    p.add_argument("--topic", default=os.environ.get("ATTACK_TOPIC", DEFAULT_TOPIC))
    p.add_argument("--rate", type=float, default=float(os.environ.get("ATTACK_RATE", "0.3")))
    p.add_argument("--max-bytes", type=int, default=int(os.environ.get("ATTACK_MAX_BYTES", 50 * 1024 * 1024)),
                   help="Max message size allowed to send (bytes). Default 50MB.")
    args = p.parse_args()

    if args.bootstrap:
        bootstrap = args.bootstrap
    else:
        bootstrap = "kafka:9092" if os.path.exists("/.dockerenv") or os.environ.get("DOCKER") else "localhost:9092"

    if not os.path.exists(args.csv):
        print(f"[FATAL] CSV file not found: {args.csv}", file=sys.stderr)
        sys.exit(2)

    # Create producer with client-side max_request_size slightly larger than our max-bytes guard
    producer = create_producer(bootstrap, max_request_size=max(args.max_bytes + 1024, 10 * 1024 * 1024))

    try:
        wait_for_metadata(producer, args.topic, timeout=15)
    except RuntimeError as e:
        print("[FATAL] Broker metadata check failed:", e, file=sys.stderr)
        producer.close()
        sys.exit(3)

    try:
        stream_csv(args.csv, producer, args.topic, args.rate, args.max_bytes)
    except KeyboardInterrupt:
        print("Interrupted by user - flushing and exiting")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
