from kafka import KafkaProducer
import csv
import json
import time
import os
from prometheus_client import start_http_server, Counter, Gauge, Histogram
from collections import deque

# --- Kafka configuration ---
TOPIC_NAME = "ics-sensor-data"
KAFKA_SERVER = "kafka:9092"

# CSV path: prefer environment variable
CSV_FILE_PATH = os.getenv("CSV_FILE_PATH", "/app/SWaT_Dataset_Attack_v0_1.csv")
# fallback where you might have mounted
if not os.path.exists(CSV_FILE_PATH):
    alt = "/app/datasets/swat/attack/SWaT_Dataset_Attack_v0_1.csv"
    if os.path.exists(alt):
        CSV_FILE_PATH = alt

# Prometheus metrics port (default 8000)
METRICS_PORT = int(os.getenv("METRICS_PORT", "8003"))

# --- Prometheus metrics ---
MESSAGES_SENT = Counter('kafka_messages_sent_total', 'Total number of messages sent to Kafka')
PROCESSING_TIME = Histogram('kafka_message_processing_seconds', 'Time taken to send message to Kafka')
ATTACK_ROWS_SENT = Counter('kafka_attack_rows_total', 'Total attack rows sent to Kafka')
CURRENT_ROW_ATTACK_FLAG = Gauge('kafka_current_row_attack', 'Is current row an attack (1) or not (0)')
DATA_RATE = Gauge('kafka_data_rows_per_second', 'Producer data rate in rows per second (1s window)')

def stream_rows_to_kafka():
    print(f"Starting Kafka producer for topic '{TOPIC_NAME}'")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    recent_times = deque(maxlen=10)  # For data rate calculation
    rate = 0.0  # initialize rate for first row

    with open(CSV_FILE_PATH, "r", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)
        for i, row in enumerate(reader, start=1):
            
            # Detect if row is attack
            label_column = row.get("Normal_Attack") or row.get("Label") or row.get("Attack") or ""
            is_attack = str(label_column).strip().lower() == "attack"
            CURRENT_ROW_ATTACK_FLAG.set(1 if is_attack else 0)
            if is_attack:
                ATTACK_ROWS_SENT.inc()

            # Send row to Kafka (both normal and attack)
            start_time = time.time()
            producer.send(TOPIC_NAME, row)
            producer.flush()
            duration = time.time() - start_time

            MESSAGES_SENT.inc()
            PROCESSING_TIME.observe(duration)

            # Update data rate
            recent_times.append(time.time())
            if len(recent_times) >= 2:
                rate = (len(recent_times) - 1) / (recent_times[-1] - recent_times[0])
                DATA_RATE.set(rate)

            status = "ATTACK" if is_attack else "NORMAL"
            print(f"[{i:05}] {status} row sent | Duration={duration:.4f}s | Rate={rate:.2f}/s")

            # Simulate real-time streaming
            time.sleep(0.3)

    producer.flush()
    print("Finished streaming all rows to Kafka.")

if __name__ == "__main__":
    start_http_server(METRICS_PORT)  # Prometheus metrics port (container/internal)
    stream_rows_to_kafka()

