#!/usr/bin/env python3
"""
Simple Kafka -> Influx writer for testing.
Writes measurement 'sensor_stats' and optional 'anomalies'.
Dependencies: kafka-python, requests

Usage:
pip install kafka-python requests
python consumer/kafka_to_influx.py --bootstrap localhost:9092 --topic raw-sensors \
  --influx-url http://localhost:8086 --influx-org ics-org --influx-bucket ics --token <TOKEN>
"""
import argparse
import json
import time
from kafka import KafkaConsumer
import requests

def write_line(url, org, bucket, token, line, precision="s"):
    api = f"{url.rstrip('/')}/api/v2/write"
    params = {"org": org, "bucket": bucket, "precision": precision}
    headers = {"Authorization": f"Token {token}", "Content-Type": "text/plain; charset=utf-8"}
    r = requests.post(api, params=params, headers=headers, data=line.encode("utf-8"), timeout=10)
    r.raise_for_status()
    return r

def to_line(measurement, tags, fields, ts=None):
    # tags: dict, fields: dict -> line protocol
    tag_str = ",".join(f"{k}={v}" for k,v in tags.items())
    field_parts = []
    for k,v in fields.items():
        if isinstance(v, bool):
            field_parts.append(f"{k}={'true' if v else 'false'}")
        elif isinstance(v, int):
            field_parts.append(f"{k}={v}i")
        else:
            field_parts.append(f"{k}={float(v)}")
    ts_suffix = f" {int(ts)}" if ts else ""
    return f"{measurement},{tag_str} " + ",".join(field_parts) + ts_suffix

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="raw-sensors")
    p.add_argument("--influx-url", required=True)
    p.add_argument("--influx-org", default="ics-org")
    p.add_argument("--influx-bucket", default="ics")
    p.add_argument("--token", required=True)
    p.add_argument("--group", default="kafka_to_influx_group")
    args = p.parse_args()

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=[args.bootstrap],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=args.group,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    print("Connected to Kafka, listening...")

    for msg in consumer:
        try:
            value = msg.value
            # Expect value to be dict like {"sensor_id":"S1","value":12.3, "ts": 169xxxx}
            sensor_id = value.get("sensor_id", "S_unknown")
            ts = int(value.get("ts", time.time()))
            # write sensor_stats
            fields = {"value": float(value.get("value", 0.0))}
            line = to_line("sensor_stats", {"sensor_id": sensor_id}, fields, ts)
            write_line(args.influx_url, args.influx_org, args.influx_bucket, args.token, line)
            # If payload contains anomaly flag/score, write anomalies measurement too
            if "anomaly_score" in value:
                fields2 = {"anomaly_score": float(value["anomaly_score"]), "is_anomaly": bool(value.get("is_anomaly", True))}
                line2 = to_line("anomalies", {"sensor_id": sensor_id}, fields2, ts)
                write_line(args.influx_url, args.influx_org, args.influx_bucket, args.token, line2)
            print(".", end="", flush=True)
        except Exception as e:
            print("\nError writing point:", e)

if __name__ == "__main__":
    main()
