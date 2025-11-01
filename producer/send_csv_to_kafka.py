from kafka import KafkaProducer
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

def stream_csv_to_kafka():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    # open with utf-8-sig to remove BOM from header (if present)
    with open(CSV_FILE_PATH, "r", encoding="utf-8-sig", newline='') as file:
        reader = csv.DictReader(file)
        print(f"Streaming {CSV_FILE_PATH} to Kafka topic '{TOPIC_NAME}'...")
        for row in reader:
            producer.send(TOPIC_NAME, row)
            print(f"Sent: {row}")  # DEBUG
            time.sleep(0.3)  # simulate 3Hz ICS sensor feed

    producer.flush()

if __name__ == "__main__":
    stream_csv_to_kafka()
