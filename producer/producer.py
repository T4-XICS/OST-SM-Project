from kafka import KafkaProducer
import csv
import json
import time
from pathlib import Path

# Kafka settings
TOPIC = "ics-stream"
BOOTSTRAP_SERVERS = "localhost:9092"

# CSV file path
CSV_PATH = Path(__file__).parent.parent / "datasets" / "swat" / "SWaT_Dataset_Attack_v0 1.csv"

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# Read CSV and send rows to Kafka
with open(CSV_PATH, newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        producer.send(TOPIC, row)
        print("Sent:", list(row.items())[:5], "...")
        time.sleep(0.2)

producer.flush()
print("Done.")
