"""
build_surrogate_dataset.py

Goal:
    - Load a SWaT CSV file (normal or attack, offline).
    - Run the LSTM-VAE model on it.
    - For each sample, compute an anomaly score (reconstruction loss).
    - Save a new CSV that contains:
        original features + model_score column.

Later we will use this CSV to train a surrogate tree model + SHAP.
"""

import os
import sys
import numpy as np
import pandas as pd
import torch

# --------------------------------------------------------------------------
# 1) Make sure we can import from the model folder
# --------------------------------------------------------------------------
THIS_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(THIS_DIR, ".."))
MODEL_DIR = os.path.join(REPO_ROOT, "model")

if MODEL_DIR not in sys.path:
    sys.path.insert(0, MODEL_DIR)

# imports from model code (same as in inference.py)
from network import LSTMVAE, load_model, loss_function  # type: ignore

# if you have a preprocessing module, we can import it later
# from preprocess_data import preprocess_offline   # TODO: if it exists


# --------------------------------------------------------------------------
# 2) CONFIG – ***YOU MAY NEED TO ADJUST THESE PATHS***
# --------------------------------------------------------------------------
# Guess for the normal SWaT dataset path – we will fix it if it is wrong.
INPUT_CSV = os.path.join(
    REPO_ROOT,
    "datasets",
    "swat",
    "normal",
    "SWaT_Dataset_Normal_v1.csv",  # <-- change if your file has another name
)

# Where to save the surrogate training data
OUTPUT_CSV = os.path.join(
    REPO_ROOT,
    "explainability",
    "surrogate_training_data.csv",
)

# LSTM-VAE weights (same file that inference.py uses)
MODEL_WEIGHTS = os.path.join(
    REPO_ROOT,
    "model",
    "weights",
    "lstm_vae_swat.pth",
)

DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")


# --------------------------------------------------------------------------
# 3) Helper: load CSV and basic numeric preprocessing
#    (simple version, we can make it smarter later)
# --------------------------------------------------------------------------
def load_raw_swat(csv_path: str) -> pd.DataFrame:
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    df = pd.read_csv(csv_path)

    # Drop obvious non-numeric columns if they exist
    for col in ["Timestamp", "Normal/Attack", "Normal_Attack", "Label"]:
        if col in df.columns:
            df = df.drop(columns=[col])

    # Keep only numeric columns
    df = df.select_dtypes(include=["number"]).copy()

    # Simple normalization (z-score) – same kind of scaling the AE expects
    df = (df - df.mean()) / (df.std() + 1e-8)

    df = df.replace([np.inf, -np.inf], 0.0).fillna(0.0)

    return df


# --------------------------------------------------------------------------
# 4) Build sequences for LSTM-VAE
# --------------------------------------------------------------------------
def build_sequences(data: np.ndarray, seq_len: int = 30) -> np.ndarray:
    """
    data: shape (N, D)
    returns: shape (num_seqs, seq_len, D)
    """
    sequences = []
    for i in range(len(data) - seq_len + 1):
        seq = data[i : i + seq_len]
        sequences.append(seq)
    return np.stack(sequences, axis=0)


# --------------------------------------------------------------------------
# 5) Compute anomaly scores with the LSTM-VAE
# --------------------------------------------------------------------------
def compute_scores(model: LSTMVAE, sequences: np.ndarray) -> np.ndarray:
    model.eval()
    scores = []

    with torch.no_grad():
        for i in range(sequences.shape[0]):
            seq = sequences[i : i + 1]  # shape (1, T, D)
            x = torch.tensor(seq, dtype=torch.float32, device=DEVICE)
            recon, mean, logvar = model(x)
            loss = loss_function(recon, x, mean, logvar)
            scores.append(float(loss.item()))

    return np.array(scores)


def main():
    print(">>> Loading data from:", INPUT_CSV)
    df = load_raw_swat(INPUT_CSV)
    print("    Raw shape:", df.shape)

    data = df.to_numpy().astype("float32")

    print(">>> Building sequences for LSTM-VAE...")
    sequences = build_sequences(data, seq_len=30)
    print("    Sequences shape:", sequences.shape)

    print(">>> Loading LSTM-VAE model from:", MODEL_WEIGHTS)
    if not os.path.exists(MODEL_WEIGHTS):
        raise FileNotFoundError(f"Model weights not found: {MODEL_WEIGHTS}")

    model = load_model(MODEL_WEIGHTS, device=DEVICE)
    model.to(DEVICE)

    print(">>> Computing anomaly scores...")
    scores = compute_scores(model, sequences)
    print("    Scores shape:", scores.shape)

    # We align features with the *last* time step of each sequence
    # so that we have a per-time-step feature vector + one score.
    aligned_features = data[29:]  # drop first 29 rows so lengths match
    assert aligned_features.shape[0] == scores.shape[0]

    out_df = pd.DataFrame(aligned_features, columns=df.columns)
    out_df["model_score"] = scores

    print(">>> Saving surrogate training data to:", OUTPUT_CSV)
    out_df.to_csv(OUTPUT_CSV, index=False)
    print("Done.")


if __name__ == "__main__":
    main()
