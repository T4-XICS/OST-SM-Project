from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from influxdb_client_3 import InfluxDBClient3, InfluxDBError, Point, WritePrecision, WriteOptions, write_client_options
import pandas as pd
import os

# --- Mock Config for now --- 
# TODO: move them to a separate config file
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "logs"

INFLUX_URL = "http://localhost:8181"
INFLUX_TOKEN = "apiv3_S4tahb4bYVgM5bAGO7GYAGaQZkIZKXHMAO_JvJ0zHDMe9x4aF4YS73wgG3uGRPy4k9fMNr6GJ3N5es6e1VgP_g"
INFLUX_ORG = "ik"
INFLUX_BUCKET = "test"

# Schema for Kafka messages
log_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("host", StringType(), True),
    StructField("service", StringType(), True),
    StructField("level", StringType(), True),
    StructField("message", StringType(), True),
    StructField("anomaly_score", DoubleType(), True)
])

spark = SparkSession.builder \
    .appName("KafkaToInflux") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read Kafka stream
df_kafka = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON
df = df_kafka.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), log_schema).alias("data")) \
    .select("data.*")

df = df.withColumn("ts", to_timestamp(col("timestamp")))

# --- Set up callbacks ---
def success(self, data: str):
    print(f"[InfluxDB] Successfully wrote batch: {len(data)} points")

def error(self, data: str, exception: InfluxDBError):
    print(f"[InfluxDB] Failed writing batch due to: {exception}")

def retry(self, data: str, exception: InfluxDBError):
    print(f"[InfluxDB] Retrying batch due to: {exception}")

# Configure batch write behavior (optional)
write_options = WriteOptions(
    batch_size=500,
    flush_interval=10_000,   # ms
    jitter_interval=2_000,
    retry_interval=5_000,
    max_retries=5,
    max_retry_delay=30_000,
    exponential_base=2
)

# Combine with callback options
wco = write_client_options(
    success_callback=success,
    error_callback=error,
    retry_callback=retry,
    write_options=write_options
)

# --- The function used inside foreachBatch ---
def write_to_influx(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    pdf = batch_df.toPandas()
    if pdf.empty:
        return

    print(f"[Spark→InfluxDB] Writing batch {batch_id} with {len(pdf)} records")

    # Create points from each row
    points = []
    for _, row in pdf.iterrows():
        try:
            p = (
                Point("log_anomaly")  # measurement name
                .tag("service", str(row.get("service", "")))
                .tag("level", str(row.get("level", "")))
                .tag("host", str(row.get("host", "")))
                .field("message", str(row.get("message", "")))
                .field("anomaly_score", float(row.get("anomaly_score", 0.0)))
                .time(row.get("ts"), WritePrecision.S)
            )
            points.append(p)
        except Exception as e:
            print(f"[WARN] Failed to build point from row: {e}")

    if not points:
        print("[Spark→InfluxDB] No valid points to write.")
        return

    # Create client (connects per microbatch)
    try:
        with InfluxDBClient3(
            host= INFLUX_URL,
            token=INFLUX_TOKEN,
            database=INFLUX_BUCKET,
            write_client_options=wco
        ) as client:
            client.write(points, write_precision="s")
    except InfluxDBError as e:
        print(f"[InfluxDB] Write failed: {e}")


query = df.writeStream \
    .foreachBatch(write_to_influx) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/spark-checkpoint") \
    .start()

query.awaitTermination()
