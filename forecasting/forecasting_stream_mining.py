"""
forecasting_stream_mining.py
----------------------------

Pure inference module for the SWAT LSTM-VAE model.
This version is FIXED to work with your existing preprocess_data.py.

It adds:
    - preprocess_numpy()
    - create_numpy_window()

These replicate the Spark preprocessing pipeline using NumPy/Pandas.
"""

import numpy as np
import pandas as pd
import torch

import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from forecasting.model_inference import load_model_safe, predict_single

DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")


# ----------------------------------------------------------------------
# 1. NumPy/Pandas equivalent of preprocess_spark()
# ----------------------------------------------------------------------
def preprocess_numpy(df: pd.DataFrame):
    """
    Replicates preprocess_spark(), but for Pandas.
    Steps:
        - Drop first/last column if non-numeric
        - Convert all to float
        - Fill NaN with 0
        - Detect low-variance columns (warn only)
    """

    df_clean = df.copy()

    # ---- Drop first / last column if non-numeric ----
    first_col = df_clean.columns[0]
    if not np.issubdtype(df_clean[first_col].dtype, np.number):
        df_clean = df_clean.drop(columns=[first_col])

    last_col = df_clean.columns[-1]
    if not np.issubdtype(df_clean[last_col].dtype, np.number):
        df_clean = df_clean.drop(columns=[last_col])

    # ---- Convert all columns to numeric ----
    df_clean = df_clean.apply(pd.to_numeric, errors='coerce')

    # ---- Fill NaN ----
    df_clean = df_clean.fillna(0.0)

    # ---- Variance detection ----
    variances = df_clean.var().to_dict()
    low_var_cols = [c for c, v in variances.items() if v < 1e-6]

    if len(low_var_cols) > 0:
        print(f"[preprocess_numpy] Columns with very low variance: {low_var_cols}")

    # (Optional) Drop low-variance columns
    # df_clean = df_clean.drop(columns=low_var_cols)

    return df_clean.to_numpy(dtype=np.float32)


# ----------------------------------------------------------------------
# 2. Window builder for NumPy inference
# ----------------------------------------------------------------------
def create_numpy_window(arr, seq_len=30):
    """
    arr shape: (N, D)
    output shape: (1, seq_len, D)
    """

    if len(arr) < seq_len:
        raise ValueError("Not enough samples to build sequence window.")

    window = arr[-seq_len:]       # last seq_len rows
    return window[np.newaxis, ...]  # add batch dimension


# ----------------------------------------------------------------------
# 3. High-level inference function
# ----------------------------------------------------------------------
def run_local_inference(raw_dataframe, seq_len=30):
    """
    Perform local batch inference on a Pandas DataFrame.
    """

    # Convert df → normalized NumPy
    arr = preprocess_numpy(raw_dataframe)  # (N, D)

    if arr is None or len(arr) < seq_len:
        raise ValueError("Not enough rows for inference window.")

    # Build window (1, seq_len, D)
    input_tensor = create_numpy_window(arr, seq_len)
    input_tensor = torch.tensor(input_tensor, dtype=torch.float32)

    # Load model
    model = load_model_safe()
    if model is None:
        raise RuntimeError("Model could not be loaded.")

    # Forward pass
    reconstruction = predict_single(model, input_tensor)

    return {
        "input_shape": list(input_tensor.shape),
        "reconstruction_shape": list(reconstruction.shape),
        "reconstruction": reconstruction
    }


if __name__ == "__main__":
    print("This script provides run_local_inference().")
    print("Import and call it from Python.")
