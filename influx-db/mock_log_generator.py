from confluent_kafka import Producer
import json, time, random
from faker import Faker

fake = Faker()

conf = {'bootstrap.servers': 'localhost:9092'}
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
        print(f"Produced record to topic {msg.topic()} partition [{msg.partition()}] @ offset {msg.offset()}")

print(f"Producing mock logs to Kafka topic '{topic}' ... Ctrl+C to stop.")
while True:
    record = generate_log()
    producer.produce(topic, json.dumps(record).encode('utf-8'), callback=delivery_report)
    producer.poll(0)
    time.sleep(1)
