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
input_path = "./datasets/swat/normal"
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

from network import LSTMVAE, loss_function, train_model, save_model
from torch.optim import Adam
from torch.optim.lr_scheduler import ReduceLROnPlateau
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

def train(batch_df, batch_id):
    train_loader, val_loader = create_dataloader(batch_df, single=False, batch_size=32, sequence_length=30)

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

    print("Training on batch:", batch_id)

    train_model(model, train_loader, val_loader, optimizer, loss_function, scheduler, num_epochs=1, device=device)

    save_model(model, "lstm_vae_swat", input_dim, latent_dim, hidden_dim, sequence_length)

query = (
    stream_df
    .writeStream
    .foreachBatch(lambda df, _: train(preprocess_spark(df), _))
    #.option("checkpointLocation", "checkpoints/mock_consumer")
    .start()
)

query.awaitTermination()
