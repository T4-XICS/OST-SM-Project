from kafka import KafkaProducer
from prometheus_client import start_http_server, Counter, Histogram, Gauge
import csv
import json
import time
import os
from collections import deque

# Kafka configuration
TOPIC_NAME = "ics-sensor-data"
KAFKA_SERVER = "kafka:9092"  # When running in Docker

# Detect correct CSV path (works both locally and in Docker)
if os.path.exists("/app/SWaT_Dataset_Normal_v0_1.csv"):
    CSV_FILE_PATH = "/app/SWaT_Dataset_Normal_v0_1.csv"  # Docker path
else:
    CSV_FILE_PATH = "datasets/swat/normal/SWaT_Dataset_Normal_v0_1.csv"  # Local path

# Basic producer performance metrics
MESSAGES_SENT = Counter('kafka_messages_sent_total', 'Total number of messages sent to Kafka')
PROCESSING_TIME = Histogram('kafka_message_processing_seconds', 'Time taken to send message to Kafka')

# Attack and anomaly metrics
ATTACK_ROWS_SENT = Counter('kafka_attack_rows_total', 'Total attack rows sent to Kafka')
ATTACK_BY_TYPE = Counter('kafka_attack_type_total', 'Attack type counter', ['type'])
CURRENT_ROW_ATTACK_FLAG = Gauge('kafka_current_row_attack', 'Is current row an attack (1) or not (0)')

# Multi-tag metrics
SENSOR_OUTLIER_DETECTED = Counter('kafka_sensor_outlier_total', 'Sensor outlier events by tag', ['tag'])
HIGH_FLOW_DETECTED = Counter('kafka_high_flow_rate_total_all', 'High flow detections per flow tag', ['flow_tag'])
PUMP_ACTIVITY_01 = Gauge('kafka_pump_status_p1', 'Pump status (0=off, 1=on)', ['pump'])
VALVE_STATUS = Gauge('kafka_valve_status_all', 'Valve status (0=closed, 1=open)', ['valve'])
TANK_LEVEL = Gauge('kafka_tank_level_cm', 'Tank level in cm', ['tank'])
FLOW_RATE = Gauge('kafka_flow_rate_lps', 'Flow rate (liters/sec)', ['flow_tag'])

PUMP_P2_ACTIVITY = Gauge('kafka_pump_status_p2', 'Stage P2 pump status (0=off, 1=on)', ['pump'])
PUMP_P4_ACTIVITY = Gauge('kafka_pump_status_p4', 'Stage P4 pump status (0=off, 1=on)', ['pump'])
FLOW_P5_RATE = Gauge('kafka_flow_rate_p5_lps', 'Stage P5 flow rates (L/min)', ['flow_tag'])

DATA_RATE = Gauge('kafka_data_rows_per_second', 'Producer data rate in rows per second (1s window)')


def stream_csv_to_kafka():
    """Stream SWaT dataset rows to Kafka while exposing Prometheus metrics."""
    print(f"Starting Kafka producer for topic '{TOPIC_NAME}'")
    print(f"Prometheus metrics available at http://localhost:8000/metrics")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    recent_times = deque(maxlen=10)  # Track timestamps for data rate calculation

    with open(CSV_FILE_PATH, "r", encoding="utf-8-sig", newline='') as file:
        reader = csv.DictReader(file)
        print(f"Streaming {CSV_FILE_PATH} to Kafka topic '{TOPIC_NAME}'...")
        rate = None
        for i, row in enumerate(reader, start=1):
            start_time = time.time()

            # Send to Kafka
            producer.send(TOPIC_NAME, row)
            duration = time.time() - start_time

            # Update Prometheus metrics
            MESSAGES_SENT.inc()
            PROCESSING_TIME.observe(duration)

            # Track producer data rate
            recent_times.append(time.time())
            if len(recent_times) >= 2:
                rate = (len(recent_times) - 1) / (recent_times[-1] - recent_times[0])
                DATA_RATE.set(rate)

            # Attack detection (depends on SWaT dataset column name)
            label = row.get("Normal_Attack") or row.get("Label") or row.get("Attack") or ""
            is_attack = str(label).strip().lower() == "attack"
            CURRENT_ROW_ATTACK_FLAG.set(1 if is_attack else 0)
            if is_attack:
                ATTACK_ROWS_SENT.inc()
                attack_type = row.get("Attack Type", "unknown")
                ATTACK_BY_TYPE.labels(type=attack_type).inc()


            # Water level
            for tank_tag in ["LIT101", "LIT401", "LIT301"]:
                try:
                    level = float(row.get(tank_tag, 0))
                    TANK_LEVEL.labels(tank=tank_tag).set(level)
                    if level < 400 or level > 800:
                        SENSOR_OUTLIER_DETECTED.labels(tag=tank_tag).inc()
                except:

                    continue


            # Flow rates for stage 5
            for flow_tag in [f"FIT50{i}" for i in range(1, 5)]:
                try:
                    rate = float(row.get(flow_tag, 0))
                    FLOW_P5_RATE.labels(flow_tag=flow_tag).set(rate)
                except Exception:

                    continue


            # Flow rate spikes
            for flow_tag in ["FIT101", "FIT201", "FIT301", "FIT401", "FIT501", "FIT601"]:
                try:
                    val = float(row.get(flow_tag, 0))
                    if val > 1.5:  # arbitrary threshold for spike
                        HIGH_FLOW_DETECTED.labels(flow_tag=flow_tag).inc()
                except Exception:

                    continue


            # Pump activity status
            for pump_tag in ["P101", "P102"]:
                try:
                    pump_on = int(float(row.get(pump_tag, 0)))
                    PUMP_ACTIVITY_01.labels(pump=pump_tag).set(pump_on)
                except Exception:

                    continue

            for pump_tag in [f"P20{i}" for i in range(1, 7)]:
                try:
                    status = int(float(row.get(pump_tag, 0)))
                    PUMP_P2_ACTIVITY.labels(pump=pump_tag).set(status)
                except Exception:
                    continue

            for pump_tag in [f"P40{i}" for i in range(1, 5)]:
                try:
                    status = int(float(row.get(pump_tag, 0)))
                    PUMP_P4_ACTIVITY.labels(pump=pump_tag).set(status)
                except Exception:
                    continue

            for valve_tag in ["MV101", "MV201", "MV301"]:
                try:
                    valve_open = int(float(row.get(valve_tag, 0)))
                    VALVE_STATUS.labels(valve=valve_tag).set(valve_open)
                except:
                    continue

            status = "ATTACK" if is_attack else "NORMAL"
            rate_display = f"{rate:.2f}/s" if rate is not None else "N/A"
            print(f"[{i:05}] {status} | LIT101={row.get('LIT101', 'N/A')} | Duration={duration:.4f}s | Rate={rate_display}")

            # Simulate real-time ICS stream
            time.sleep(0.3)

    producer.flush()
    print("Finished streaming CSV to Kafka.")


if __name__ == "__main__":
    # Start Prometheus server
    start_http_server(8000)
    # Begin Kafka streaming
    stream_csv_to_kafka()

