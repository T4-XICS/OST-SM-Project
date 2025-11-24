"""
spark_streaming_consumer.py
---------------------------

Standalone Spark Structured Streaming consumer.

Reads:
    Kafka topic "ics-sensor-data"

Uses:
    inference.start_spark_streaming()

Run with:
    spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
      spark_streaming_consumer.py
"""
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from forecasting.model_inference import start_spark_streaming


if __name__ == "__main__":
    print("Starting Spark Streaming Consumer...")
    start_spark_streaming()
