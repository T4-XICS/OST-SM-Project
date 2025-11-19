import os
from confluent_kafka import Producer
import json, time, random
from faker import Faker

fake = Faker()

# Default = localhost (like original team code)
# But can be overridden inside Docker using env var
bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

conf = {'bootstrap.servers': bootstrap_servers}
producer = Producer(conf)

SERVICES = ["auth", "payment", "orders", "inventory"]
LEVELS = ["INFO", "WARN", "ERROR"]

def generate_log():
    service = random.choice(SERVICES)
    level = random.choice(LEVELS)
    msg = f"{service} - {level} - {fake.sentence()}"
    anomaly_score = round(random.random(), 3)

    return {
        "timestamp": fake.iso8601(),
        "host": fake.hostname(),
        "service": service,
        "level": level,
        "message": msg,
        "anomaly_score": anomaly_score
    }

topic = "logs"

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Produced record to topic {msg.topic()} "
            f"partition [{msg.partition()}] @ offset {msg.offset()}"
        )

print(
    f"Producing mock logs to Kafka topic '{topic}' "
    f"(bootstrap={bootstrap_servers}) ... Ctrl+C to stop."
)

while True:
    record = generate_log()
    producer.produce(topic, json.dumps(record).encode('utf-8'), callback=delivery_report)
    producer.poll(0)
    time.sleep(1)
