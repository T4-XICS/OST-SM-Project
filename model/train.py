import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, from_json, trim, when
import torch
from preprocess_data import preprocess_spark, create_dataloader

# Configure logging to stdout for Promtail/Loki
SERVICE_NAME = os.getenv("SERVICE_NAME", "spark-train")
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format=f"%(asctime)s %(levelname)s %(name)s app={SERVICE_NAME} %(message)s",
)
logger = logging.getLogger(SERVICE_NAME)

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

from network import LSTMVAE, loss_function, train_model, save_model
from torch.optim import Adam
from torch.optim.lr_scheduler import ReduceLROnPlateau
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
logger.info(f"Using device: {device}")

def train(batch_df, batch_id):
    if batch_df.count() == 0:
        logger.info(f"Batch {batch_id} is empty. Skipping training.")
        return
    
    logger.info(f"Batch {batch_id} RAW SAMPLE (value column)")
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

    logger.info(f"Batch {batch_id} PARSED SAMPLE")
    parsed.show(5, False)

    total = parsed.count()
    logger.info(f"Parsed rows: {total}")
    for f in parsed.columns:
        nulls = parsed.filter(col(f).isNull()).count()
        logger.info(f"{f}: nulls={nulls}")

    # only then preprocess and create dataloader
    df_pre = preprocess_spark(parsed)
    train_loader, val_loader = create_dataloader(df_pre, single=False, batch_size=32, sequence_length=30)

    if train_loader is None or val_loader is None:
        logger.info(f"Batch {batch_id}: not enough data to run training yet. Skipping.")
        return

    df_pre.show(5, False)

    input_dim=train_loader.dataset[0].shape[1]
    hidden_dim=128
    latent_dim=32
    sequence_length=30

    model = LSTMVAE(input_dim=input_dim,
                    hidden_dim=hidden_dim,
                    latent_dim=latent_dim,
                    sequence_length=sequence_length,
                    num_layers=1,
                    device=device).to(device)

    optimizer = Adam(model.parameters(), lr=1e-3)
    scheduler = ReduceLROnPlateau(optimizer, 'min', patience=5, factor=0.1)

    # Train
    torch.cuda.empty_cache()

    logger.info(f"Training on batch: {batch_id}")

    train_model(model, train_loader, val_loader, optimizer, loss_function, scheduler, num_epochs=1, device=device)

    save_model(model, "/weights/lstm_vae_swat", input_dim, latent_dim, hidden_dim, sequence_length)

query = (
    stream_df
    .writeStream
    .foreachBatch(lambda df, batch_id: train(df, batch_id))
    #.option("checkpointLocation", "checkpoints/mock_consumer")
    .start()
)

query.awaitTermination()
