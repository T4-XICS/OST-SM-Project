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
import logging
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
    p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"], 
                    help="Set logging level (default: INFO)")
    args = p.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    logger.info(f"Batch size set to: {args.batch_size}")
    logger.info(f"Max messages: {args.max_messages}")
    logger.info(f"Log level: {args.log_level}")

    logger.info(f"Connecting to Kafka at {args.bootstrap}...")
    logger.info(f"Reading from topic: {args.topic}")
    logger.info(f"Writing to InfluxDB at {args.influx_url} (bucket: {args.influx_bucket})")

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
    logger.info(f"Connecting to InfluxDB at {args.influx_url}...")
    influx_client = InfluxDBClient(url=args.influx_url, token=args.token, org=args.influx_org)
    
    # Test InfluxDB connection
    try:
        health = influx_client.ping()
        logger.info(f"InfluxDB connection test successful: {health}")
    except Exception as e:
        logger.error(f"InfluxDB connection test failed: {e}")
        raise
        
    write_api = influx_client.write_api(write_options=SYNCHRONOUS)
    logger.info("InfluxDB write API initialized")

    count = 0
    points = []

    try:
        logger.info("Connected to Kafka, listening for messages...")
        for message in consumer:
            logger.debug(f"Received message from partition {message.partition}, offset {message.offset}")
            data = message.value
            logger.debug(f"Message data: {json.dumps(data, indent=2)}")
            
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
                    except (ValueError, TypeError) as e:
                        logger.debug(f"Skipping non-numeric value for key '{key}': {value} ({e})")
            
            # Parse timestamp
            if timestamp_str:
                try:
                    ts = pd.to_datetime(timestamp_str.strip(), format="%d/%m/%Y %I:%M:%S %p")
                    point.time(ts, WritePrecision.S)
                except Exception as e:
                    logger.warning(f"Could not parse timestamp '{timestamp_str}': {e}")
                    point.time(pd.Timestamp.now(), WritePrecision.S)
            
            points.append(point)
            count += 1
            
            # Write in batches
            if len(points) >= args.batch_size:
                try:
                    write_api.write(bucket=args.influx_bucket, org=args.influx_org, record=points)
                    logger.info(f"Successfully wrote batch of {len(points)} points to InfluxDB (total processed: {count})")
                    points = []
                except Exception as e:
                    logger.error(f"Error writing to InfluxDB: {e}")
                    logger.debug(f"Error type: {type(e)}, details: {str(e)}")
                    points = []
            
            # Stop after max_messages if specified
            if args.max_messages and count >= args.max_messages:
                break

    except KeyboardInterrupt:
        logger.info("Stopping consumer...")

    finally:
        # Write any remaining points
        if points:
            try:
                write_api.write(bucket=args.influx_bucket, org=args.influx_org, record=points)
                logger.info(f"Wrote final batch of {len(points)} points to InfluxDB (total processed: {count})")
            except Exception as e:
                logger.error(f"Error writing final batch: {e}")
        
        consumer.close()
        influx_client.close()
        logger.info(f"Done! Processed {count} messages total")

if __name__ == "__main__":
    main()
