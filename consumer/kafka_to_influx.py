#!/usr/bin/env python3
"""
Kafka consumer that reads SWaT sensor data and writes to InfluxDB.
Processes all sensor fields dynamically and batches writes for performance.
Dependencies: kafka-python, influxdb-client, pandas

Usage:
pip install kafka-python influxdb-client pandas
python consumer/kafka_to_influx.py --bootstrap localhost:9092 --topic ics-sensor-data \
  --influx-url http://localhost:8086 --influx-org ucs --influx-bucket swat_db --token <TOKEN>
"""
import argparse
import json
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import pandas as pd

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="ics-sensor-data")
    p.add_argument("--influx-url", default="http://localhost:8086")
    p.add_argument("--influx-org", default="ucs")
    p.add_argument("--influx-bucket", default="swat_db")
    p.add_argument("--token", required=True)
    p.add_argument("--group", default="kafka_to_influx_group")
    p.add_argument("--batch-size", type=int, default=10, help="Number of points to batch before writing")
    p.add_argument("--max-messages", type=int, default=None, help="Maximum messages to process (None for unlimited)")
    args = p.parse_args()
    
    print(f"Batch size set to: {args.batch_size}")
    print(f"Max messages: {args.max_messages}")
    print()

    print(f"Connecting to Kafka at {args.bootstrap}...")
    print(f"Reading from topic: {args.topic}")
    print(f"Writing to InfluxDB at {args.influx_url} (bucket: {args.influx_bucket})")
    print()

    # Create Kafka consumer
    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=[args.bootstrap],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=args.group,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    # Create InfluxDB client
    print(f"Connecting to InfluxDB at {args.influx_url}...")
    influx_client = InfluxDBClient(url=args.influx_url, token=args.token, org=args.influx_org)
    
    # Test InfluxDB connection
    try:
        health = influx_client.ping()
        print(f"InfluxDB connection test successful: {health}")
    except Exception as e:
        print(f"InfluxDB connection test failed: {e}")
        raise
        
    write_api = influx_client.write_api(write_options=SYNCHRONOUS)
    print("InfluxDB write API initialized")

    count = 0
    points = []

    try:
        print("Connected to Kafka, listening...")
        print("Waiting for first message...")
        for message in consumer:
            print(f"Received message from partition {message.partition}, offset {message.offset}")
            data = message.value
            print(f"Message data: {json.dumps(data, indent=2)}")
            
            # Create point
            point = Point("swat_sensor_data")
            
            # Extract timestamp and status
            timestamp_str = data.get('Timestamp', '')
            status = data.get('Normal_Attack', 'Normal').strip()
            
            point.tag("status", status)
            
            # Add all numeric sensor readings as fields
            for key, value in data.items():
                if key not in ['Timestamp', 'Normal_Attack']:
                    try:
                        numeric_value = float(value)
                        point.field(key, numeric_value)
                    except (ValueError, TypeError):
                        pass
            
            # Parse timestamp
            if timestamp_str:
                try:
                    ts = pd.to_datetime(timestamp_str.strip(), format="%d/%m/%Y %I:%M:%S %p")
                    point.time(ts, WritePrecision.S)
                except Exception as e:
                    print(f"Warning: Could not parse timestamp '{timestamp_str}': {e}")
                    point.time(pd.Timestamp.now(), WritePrecision.S)
            
            points.append(point)
            count += 1
            
            # Write in batches
            if len(points) >= args.batch_size:
                try:
                    print(f"\nAttempting to write {len(points)} points to InfluxDB...")
                    print(f"Target bucket: {args.influx_bucket}")
                    print(f"Organization: {args.influx_org}")
                    write_api.write(bucket=args.influx_bucket, org=args.influx_org, record=points)
                    print(f"✓ Successfully wrote {len(points)} points to InfluxDB (total: {count})")
                    points = []
                except Exception as e:
                    print(f"✗ Error writing to InfluxDB: {e}")
                    print(f"Error type: {type(e)}")
                    print(f"Error details: {str(e)}")
                    points = []
            
            # Stop after max_messages if specified
            if args.max_messages and count >= args.max_messages:
                break

    except KeyboardInterrupt:
        print("\nStopping consumer...")

    finally:
        # Write any remaining points
        if points:
            try:
                write_api.write(bucket=args.influx_bucket, org=args.influx_org, record=points)
                print(f"✓ Wrote final {len(points)} points to InfluxDB (total: {count})")
            except Exception as e:
                print(f"✗ Error writing final batch: {e}")
        
        consumer.close()
        influx_client.close()
        print(f"\nDone! Processed {count} messages total")

if __name__ == "__main__":
    main()
