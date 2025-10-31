from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, from_json, trim, when
import torch
from preprocess_data import preprocess_spark, create_dataloader

spark = (
    SparkSession.builder
    .appName("MockKafkaConsumerWithInference")
    .master("local[*]")
    .config("spark.hadoop.security.authentication", "simple")
    .getOrCreate()
)

# replace the previous typed schema with an all-string schema, then cast later
all_fields = [
    "Timestamp","FIT101","LIT101","MV101","P101","P102","AIT201","AIT202","AIT203",
    "FIT201","MV201","P201","P202","P203","P204","P205","P206","DPIT301","FIT301",
    "LIT301","MV301","MV302","MV303","MV304","P301","P302","AIT401","AIT402","FIT401",
    "LIT401","P401","P402","P403","P404","UV401","AIT501","AIT502","AIT503","AIT504",
    "FIT501","FIT502","FIT503","FIT504","P501","P502","PIT501","PIT502","PIT503",
    "FIT601","P601","P602","P603","Normal_Attack"
]

schema = StructType([StructField(f, StringType(), True) for f in all_fields])
numeric_cols = [c for c in all_fields if c not in ("Timestamp", "Normal_Attack")]

stream_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "ics-sensor-data")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

from network import LSTMVAE, load_model
from evaluate import evaluate_lstm
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
print(f"Using device: {device}")

def run_eval(batch_df, batch_id):
    if batch_df.count() == 0:
        print(f"\n--- Batch {batch_id} is empty. Skipping evaluation. ---")
        return

    print(f"\n--- Batch {batch_id} RAW SAMPLE (value column) ---")
    batch_df.select(col("value").cast("string").alias("raw_value")).show(5, False)

    # parse JSON into string columns
    cleaned = batch_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    # trim and cast numeric string columns -> DoubleType, treat empty strings as null
    parsed = cleaned
    for c in numeric_cols:
        parsed = parsed.withColumn(c, when(trim(col(c)) == "", None).otherwise(col(c).cast(DoubleType())))

    # trim timestamp and label
    parsed = parsed.withColumn("Timestamp", trim(col("Timestamp")))\
                   .withColumn("Normal_Attack", trim(col("Normal_Attack")))

    print(f"\n--- Batch {batch_id} PARSED SAMPLE ---")
    parsed.show(5, False)

    total = parsed.count()
    print(f"Parsed rows: {total}")
    for f in parsed.columns:
        nulls = parsed.filter(col(f).isNull()).count()
        print(f"{f}: nulls={nulls}")

    # only then preprocess and create dataloader
    df_pre = preprocess_spark(parsed)
    dataloader = create_dataloader(df_pre, batch_size=32, sequence_length=30)

    if dataloader is None:
        print(f"Batch {batch_id}: not enough data to run inference yet. Skipping.")
        return

    df_pre.show(5, False)

    model = load_model("weights/lstm_vae_swat.pth", device=device)
    model.eval()

    # Evaluate
    anomalies = evaluate_lstm(model, dataloader, device, 90)

    print(f"\n--- Batch {batch_id} Evaluation ---")
    print(f"Anomalies: {anomalies}")

query = (
    stream_df
    .writeStream
    .foreachBatch(lambda df, batch_id: run_eval(df, batch_id))
    #.option("checkpointLocation", "checkpoints/mock_consumer")
    .start()
)

query.awaitTermination()
