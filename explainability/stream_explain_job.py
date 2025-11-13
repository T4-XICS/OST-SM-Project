# -*- coding: utf-8 -*-
# Stream Explainability Job
# Author: Soma Tohidinia

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import os
from prometheus_client import start_http_server
start_http_server(9000)  # port for Prometheus metrics
from explainability.shap_explainer import ShapExplainer
from explainability.metrics import (
    EXPLANATIONS_TOTAL,
    EXPLAINED_SAMPLES_TOTAL,
    EXPLANATION_LATENCY_SECONDS,
)


# Environment variables for configuration
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ics-sensor-data")
INFLUX_URL = os.getenv("INFLUX_URL", "http://influxdb:8086")
INFLUX_ORG = os.getenv("INFLUX_ORG", "ucs")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "admintoken123")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "explain_db")


def main():
    # 1️⃣ Create Spark session
    spark = (
        SparkSession.builder
        .appName("ExplainabilityStream")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # 2️⃣ Define schema for incoming data
    schema = StructType([
        StructField("tag", StringType()),
        StructField("value", DoubleType()),
        StructField("status", StringType()),
        StructField("timestamp", TimestampType()),
    ])

    # 3️⃣ Read live Kafka stream
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .load()
    )

    # 4️⃣ Parse JSON payload
    parsed_df = (
        kafka_df.selectExpr("CAST(value AS STRING) as data")
        .select(from_json(col("data"), schema).alias("data"))
        .select("data.*")
    )

    # 5️⃣ Simple aggregation (placeholder for SHAP/LIME)
    agg_df = parsed_df.groupBy("tag").count()

    # 6️⃣ Output to console for testing
    query = (
        agg_df.writeStream
        .format("console")
        .outputMode("complete")
        .option("truncate", False)
        .start()
    )

    query.awaitTermination()

if __name__ == "__main__":
    main()
