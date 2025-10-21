from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, TimestampType

# 1. Spark session
spark = SparkSession.builder \
    .appName("ICS-Stream-Consumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Schema (adapt based on your CSV header!)
schema = StructType() \
    .add("timestamp", StringType()) \
    .add("LIT101", StringType()) \
    .add("FIT101", StringType()) 

# 3. Read from Kafka topic
df_kafka_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "ics-sensor-data") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Decode Kafka value field
df_json = df_kafka_raw.selectExpr("CAST(value AS STRING)")

df_parsed = df_json.select(from_json(col("value"), schema).alias("data")).select("data.*")

# 5. Output to console for now
query = df_parsed.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
