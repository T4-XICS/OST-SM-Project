from pyspark.sql import functions as F

def preprocess_spark(df):
    """
    df: Spark DataFrame (streaming or static)
    Returns: cleaned Spark DataFrame (ready for PyTorch sequence creation)
    """

    # drop the first and last column if it's non-numeric (e.g., timestamp)
    first_col = df.columns[0]
    if dict(df.dtypes)[first_col] not in ['double', 'float', 'int', 'bigint']:
        df = df.drop(first_col)
    
    last_col = df.columns[-1]
    if dict(df.dtypes)[last_col] not in ['double', 'float', 'int', 'bigint']:
        df = df.drop(last_col)

    # Convert all columns to numeric
    for c in df.columns:
        df = df.withColumn(c, F.col(c).cast("double"))

    # Fill or interpolate missing values
    df = df.fillna(0)

    # Compute variance per column
    var_df = df.select([F.variance(F.col(c)).alias(c) for c in df.columns]).collect()[0].asDict()
    low_var_cols = [c for c, v in var_df.items() if v is not None and v < 1e-6]

    if low_var_cols:
        print(f"Columns with very low variance: {low_var_cols}")
        #df = df.drop(*low_var_cols)

    return df

from torch.utils.data import DataLoader
from sklearn.model_selection import train_test_split
import numpy as np

def create_dataloader(pdf, single=True, batch_size=32, sequence_length=30):
    df = pdf.toPandas()
    data = df.to_numpy(dtype=np.float32)

    print(f"Total data points: {data.shape[0]}, Features: {data.shape[1]}")

    # build sliding windows
    sequences = [
        data[i:i + sequence_length]
        for i in range(len(data) - sequence_length + 1)
    ]

    if single:
        return DataLoader(sequences, batch_size=batch_size, shuffle=False)

    train_data, val_data = train_test_split(sequences, test_size=0.3, random_state=42, shuffle=False) # 70% train, 30% temp
    train_loader = DataLoader(train_data, batch_size=batch_size, shuffle=True)
    val_loader = DataLoader(val_data, batch_size=batch_size, shuffle=False)
    return train_loader, val_loader
     
