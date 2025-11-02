from kafka import KafkaProducer
from prometheus_client import start_http_server, Counter, Histogram
import csv
import json
import time
import os

TOPIC_NAME = "ics-sensor-data"
KAFKA_SERVER = "kafka:9092"  # Use "kafka:9092" when running in Docker

# Check if running in Docker (file in /app) or locally (file in datasets/)
if os.path.exists("/app/SWaT_Dataset_Normal_v0_1.csv"):
    CSV_FILE_PATH = "/app/SWaT_Dataset_Normal_v0_1.csv"  # Docker path
else:
    CSV_FILE_PATH = "datasets/swat/normal/SWaT_Dataset_Normal_v0_1.csv"  # Local path

# Prometheus metrics
MESSAGES_SENT = Counter('kafka_messages_sent_total', 'Total number of messages sent to Kafka')
PROCESSING_TIME = Histogram('kafka_message_processing_seconds', 'Time taken to send message to Kafka')

def stream_csv_to_kafka():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    # Start Prometheus metrics server
    start_http_server(8000)
    with open(CSV_FILE_PATH, "r", encoding="utf-8-sig", newline='') as file:
        reader = csv.DictReader(file)
        print(f"Streaming {CSV_FILE_PATH} to Kafka topic '{TOPIC_NAME}'...")
        for row in reader:
            start_time = time.time()
            producer.send(TOPIC_NAME, row)
            producer.flush()
            duration = time.time() - start_time

            # Update Prometheus metrics
            MESSAGES_SENT.inc()
            PROCESSING_TIME.observe(duration)

            print(f"Sent: {row} | Took: {duration:.4f}s")
            time.sleep(0.3)  # simulate 3Hz ICS sensor feed

    producer.flush()

if __name__ == "__main__":
    stream_csv_to_kafka()