from pyspark.sql import SparkSession
import torch
from preprocess_data import preprocess_spark, create_dataloader

spark = (
    SparkSession.builder
    .appName("MockKafkaConsumerWithInference")
    .master("local[*]")
    .config("spark.hadoop.security.authentication", "simple")
    .getOrCreate()
)

# Simulated input directory for CSV files
input_path = "./datasets/swat/attack"
schema = "Timestamp STRING, FIT101 DOUBLE,LIT101 DOUBLE, MV101 DOUBLE,P101 DOUBLE,P102 DOUBLE, AIT201 DOUBLE,AIT202 DOUBLE,AIT203 DOUBLE,FIT201 DOUBLE, MV201 DOUBLE, P201 DOUBLE, P202 DOUBLE,P203 DOUBLE, P204 DOUBLE,P205 DOUBLE,P206 DOUBLE,DPIT301 DOUBLE,FIT301 DOUBLE,LIT301 DOUBLE,MV301 DOUBLE,MV302 DOUBLE, MV303 DOUBLE,MV304 DOUBLE,P301 DOUBLE,P302 DOUBLE,AIT401 DOUBLE,AIT402 DOUBLE,FIT401 DOUBLE,LIT401 DOUBLE,P401 DOUBLE,P402 DOUBLE,P403 DOUBLE,P404 DOUBLE,UV401 DOUBLE,AIT501 DOUBLE,AIT502 DOUBLE,AIT503 DOUBLE,AIT504 DOUBLE,FIT501 DOUBLE,FIT502 DOUBLE,FIT503 DOUBLE,FIT504 DOUBLE,P501 DOUBLE,P502 DOUBLE, PIT501 DOUBLE,PIT502 DOUBLE,PIT503 DOUBLE,FIT601 DOUBLE,P601 DOUBLE,P602 DOUBLE,P603 DOUBLE,Normal_Attack STRING"

# Read streaming CSV (each new file simulates new producer data)
stream_df = (
    spark.readStream
    .option("header", True)
    .option("ignoreLeadingWhiteSpace", True)
    .option("ignoreTrailingWhiteSpace", True)
    .schema(schema)
    .csv(input_path)
)
# replace with actual Kafka source in production
# .readStream
# .format("kafka")
# .option("kafka.bootstrap.servers", "<broker>:9092")
# .option("subscribe", "<topic>")
# .load()

from network import LSTMVAE, load_model
from evaluate import evaluate_lstm
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

def run_eval(batch_df, batch_id):
    dataloader = create_dataloader(batch_df, batch_size=32, sequence_length=30)

    model = load_model("model/weights/lstm_vae_swat.pth", device=device)
    model.eval()

    # Evaluate
    anomalies = evaluate_lstm(model, dataloader, device, 90)

    print(f"\n--- Batch {batch_id} Evaluation ---")
    print(f"Anomalies: {anomalies}")

query = (
    stream_df
    .writeStream
    .foreachBatch(lambda df, _: run_eval(preprocess_spark(df), _))
    #.option("checkpointLocation", "checkpoints/mock_consumer")
    .start()
)

query.awaitTermination()
