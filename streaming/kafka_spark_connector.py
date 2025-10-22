from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create Spark session
spark = SparkSession \
    .builder \
    .appName("KafkaPyTorchConnector") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
    .getOrCreate()

# Define schema for incoming data
schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("device_id", StringType(), True),
    StructField("measurements", ArrayType(FloatType()), True),
    # Add other fields as needed
])

# Read from Kafka
kafka_stream_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "sensor_data") \
    .option("startingOffsets", "latest") \
    .load()

# Parse the JSON data from Kafka
parsed_df = kafka_stream_df \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")