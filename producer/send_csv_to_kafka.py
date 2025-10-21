from kafka import KafkaProducer
import csv
import json
import time

TOPIC_NAME = "ics-sensor-data"
KAFKA_SERVER = "kafka:9092"
CSV_FILE_PATH = "SWaT_Dataset_Normal_v0_1.csv"

def stream_csv_to_kafka():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    with open(CSV_FILE_PATH, "r") as file:
        reader = csv.DictReader(file)
        print(f"Streaming {CSV_FILE_PATH} to Kafka topic '{TOPIC_NAME}'...")
        for row in reader:
            producer.send(TOPIC_NAME, row)
            print(f"Sent: {row}")  # DEBUG
            time.sleep(0.3)  # simulate 3Hz ICS sensor feed

    producer.flush()
    print("✅ Finished streaming CSV to Kafka.")

if __name__ == "__main__":
    stream_csv_to_kafka()
