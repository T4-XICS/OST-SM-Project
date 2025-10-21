from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, TimestampType

#session
spark = SparkSession.builder \
    .appName("ICS-Stream-Consumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType() \
    .add("timestamp", StringType()) \
    .add("LIT101", StringType()) \
    .add("FIT101", StringType()) 

df_kafka_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "ics-sensor-data") \
    .option("startingOffsets", "latest") \
    .load()

df_json = df_kafka_raw.selectExpr("CAST(value AS STRING)")

df_parsed = df_json.select(from_json(col("value"), schema).alias("data")).select("data.*")

query = df_parsed.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
