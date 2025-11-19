# explainability/build_surrogate_dataset.py
#
# Offline script to build a surrogate-training dataset for SWaT
# using the LSTM-VAE anomaly scores as features.

import os
import sys
from pathlib import Path
from typing import Tuple, List

import numpy as np
import pandas as pd
import torch

# -----------------------------------------------------------------------------
# Paths
# -----------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parents[1]

# SWaT CSVs (normal + attack)
NORMAL_CSV = REPO_ROOT / "datasets" / "swat" / "normal" / "SWaT_Dataset_Normal_v0_1.csv"
ATTACK_CSV = REPO_ROOT / "datasets" / "swat" / "attack" / "SWaT_Dataset_Attack_v0_1.csv"

# LSTM-VAE weights (same file inference.py uses)
MODEL_WEIGHTS = REPO_ROOT / "model" / "weights" / "lstm_vae_swat.pth"

# Output surrogate dataset
OUT_CSV = REPO_ROOT / "explainability" / "swat_surrogate.csv"

# Make sure we can import from model/
sys.path.insert(0, str(REPO_ROOT / "model"))
from network import load_model, loss_function  # type: ignore


# -----------------------------------------------------------------------------
# 1. Load and basic preprocess SWaT CSV
# -----------------------------------------------------------------------------
def load_raw_swat(csv_path: Path) -> pd.DataFrame:
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    df = pd.read_csv(csv_path)

    # Drop obvious non-numeric columns if they exist
    for col in ["Timestamp", "Normal/Attack", "Normal_Attack", "Label"]:
        if col in df.columns:
            df = df.drop(columns=[col])

    # Keep only numeric columns
    df = df.select_dtypes(include=["number"]).copy()

    # Simple normalization (z-score) – similar scale as the AE expects
    df = (df - df.mean()) / (df.std() + 1e-8)

    # Replace NaN/inf with 0
    df = df.replace([np.inf, -np.inf], 0.0).fillna(0.0)

    return df


# -----------------------------------------------------------------------------
# 2. Make sliding windows (sequences) for LSTM-VAE
# -----------------------------------------------------------------------------
def make_sequences(
    df: pd.DataFrame,
    seq_len: int = 30,
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Build overlapping sequences of length seq_len.

    Returns:
        sequences: (num_seq, seq_len, num_features)
        last_steps: (num_seq, num_features)  # last row of each window
    """
    values = df.values.astype(np.float32)
    num_rows, num_features = values.shape

    if num_rows <= seq_len:
        raise ValueError(
            f"Not enough rows ({num_rows}) for sequence length {seq_len}"
        )

    sequences: List[np.ndarray] = []
    last_steps: List[np.ndarray] = []

    for start in range(0, num_rows - seq_len + 1):
        window = values[start : start + seq_len, :]
        sequences.append(window)
        last_steps.append(window[-1])

    sequences_arr = np.stack(sequences)  # (N, seq_len, F)
    last_steps_arr = np.stack(last_steps)  # (N, F)

    return sequences_arr, last_steps_arr


# -----------------------------------------------------------------------------
# 3. Compute anomaly scores with LSTM-VAE
# -----------------------------------------------------------------------------
def compute_lstm_scores(
    model: torch.nn.Module,
    sequences: np.ndarray,
    device: torch.device,
    batch_size: int = 512,
) -> List[float]:
    """
    sequences: (num_seq, seq_len, num_features)
    Returns one anomaly score (reconstruction loss) per sequence.
    Uses batching for speed.
    """
    model.eval()
    scores: List[float] = []

    with torch.no_grad():
        tensor = torch.tensor(sequences, dtype=torch.float32).to(device)
        num_seq = tensor.shape[0]

        for start in range(0, num_seq, batch_size):
            end = min(start + batch_size, num_seq)
            batch = tensor[start:end]  # shape (B, seq_len, F)

            recon, mean, logvar = model(batch)

            batch_losses = loss_function(recon, batch, mean, logvar)

        
            if isinstance(batch_losses, torch.Tensor) and batch_losses.ndim == 0:
                batch_losses = batch_losses.repeat(batch.shape[0])

            for l in batch_losses:
                scores.append(float(l.item()))

    return scores



# -----------------------------------------------------------------------------
# 4. Build surrogate features for a single CSV
# -----------------------------------------------------------------------------
def build_surrogate_for_csv(
    csv_path: Path,
    label: int,
    model: torch.nn.Module,
    device: torch.device,
    seq_len: int = 30,
) -> pd.DataFrame:
    """
    - Loads and preprocesses raw SWaT CSV
    - Builds sequences
    - Runs LSTM-VAE to get anomaly scores
    - Builds a tabular dataset: last-step features + lstm_score + label
    """
    print(f"[+] Loading and preprocessing: {csv_path}")
    df_raw = load_raw_swat(csv_path)

    print(f"    Raw shape after preprocess: {df_raw.shape}")
    sequences, last_steps = make_sequences(df_raw, seq_len=seq_len)
    print(f"    Num sequences: {sequences.shape[0]}")

    scores = compute_lstm_scores(model, sequences, device)
    if len(scores) != last_steps.shape[0]:
        raise RuntimeError("Scores and features length mismatch")

    # Tabular features = last step of each sequence
    out_df = pd.DataFrame(last_steps, columns=df_raw.columns)
    out_df["lstm_score"] = scores
    out_df["label"] = label

    return out_df


# -----------------------------------------------------------------------------
# 5. Main
# -----------------------------------------------------------------------------
def main() -> None:
    print("=== Building SWaT surrogate dataset using LSTM-VAE ===")
    print(f"Repository root : {REPO_ROOT}")
    print(f"Normal CSV      : {NORMAL_CSV}")
    print(f"Attack CSV      : {ATTACK_CSV}")
    print(f"Model weights   : {MODEL_WEIGHTS}")
    print(f"Output CSV      : {OUT_CSV}")

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"Using device    : {device}")

    if not MODEL_WEIGHTS.exists():
        raise FileNotFoundError(f"Model weights not found: {MODEL_WEIGHTS}")

    print("[+] Loading LSTM-VAE model...")
    model = load_model(str(MODEL_WEIGHTS), device=device)
    model.eval()
    print("[+] Model loaded.")

    # Build surrogate features for each class
    df_normal = build_surrogate_for_csv(NORMAL_CSV, label=0, model=model, device=device)
    df_attack = build_surrogate_for_csv(ATTACK_CSV, label=1, model=model, device=device)

    # Combine & shuffle
    surrogate_df = pd.concat([df_normal, df_attack], ignore_index=True)
    surrogate_df = surrogate_df.sample(frac=1.0, random_state=42).reset_index(drop=True)

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    surrogate_df.to_csv(OUT_CSV, index=False)

    print(f"\n[✓] Surrogate dataset saved to: {OUT_CSV}")
    print(f"[✓] Final shape: {surrogate_df.shape}")
    print("[✓] Columns:", list(surrogate_df.columns))


if __name__ == "__main__":
    main()
