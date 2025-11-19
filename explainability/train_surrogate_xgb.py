# explainability/train_surrogate_xgb.py
#
# Train an XGBoost surrogate model to approximate the LSTM-VAE output (lstm_score)
# using the SWaT surrogate dataset we built in swat_surrogate.csv.

import os
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
from xgboost import XGBRegressor
import joblib  # for saving the model

REPO_ROOT = Path(__file__).resolve().parents[1]
SURROGATE_CSV = REPO_ROOT / "explainability" / "swat_surrogate.csv"
MODEL_OUT = REPO_ROOT / "explainability" / "xgb_surrogate_lstm_score.pkl"


def load_data() -> tuple[pd.DataFrame, pd.Series, pd.Series]:
    """
    Load the surrogate dataset and split into:
        X : sensor features
        y : lstm_score (regression target)
        y_label : true normal/attack label (for analysis only)
    """
    if not SURROGATE_CSV.exists():
        raise FileNotFoundError(f"Surrogate CSV not found: {SURROGATE_CSV}")

    df = pd.read_csv(SURROGATE_CSV)

    if "lstm_score" not in df.columns or "label" not in df.columns:
        raise RuntimeError("Expected columns 'lstm_score' and 'label' not found.")

    # Features = all columns except lstm_score and label
    feature_cols = [c for c in df.columns if c not in ["lstm_score", "label"]]
    X = df[feature_cols].astype(np.float32)

    # Target for surrogate = LSTM output
    y = df["lstm_score"].astype(np.float32)

    # Ground-truth normal/attack label (for later analysis, not the training target)
    y_label = df["label"].astype(int)

    return X, y, y_label


def train_xgb_surrogate(X: pd.DataFrame, y: pd.Series) -> XGBRegressor:
    """
    Train an XGBoost regressor to approximate lstm_score from sensor values.
    """
    # Train/validation split
    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.2,
        random_state=42,
        shuffle=True,
    )

    # A reasonably small model (you can tune later if needed)
    model = XGBRegressor(
        n_estimators=200,
        max_depth=6,
        learning_rate=0.1,
        subsample=0.8,
        colsample_bytree=0.8,
        objective="reg:squarederror",
        n_jobs=4,
        tree_method="hist",  # faster on CPUs
    )

    print("[+] Training XGBoost surrogate...")
    model.fit(X_train, y_train)

    print("[+] Evaluating on validation set...")
    y_pred = model.predict(X_test)
    mse = mean_squared_error(y_test, y_pred)
    rmse = float(np.sqrt(mse))
    r2 = r2_score(y_test, y_pred)

    print(f"[✓] Validation RMSE: {rmse:.4f}")
    print(f"[✓] Validation R^2 : {r2:.4f}")

    return model


def main() -> None:
    print("=== Training XGBoost surrogate for LSTM-VAE (SWaT) ===")
    print(f"Surrogate CSV : {SURROGATE_CSV}")
    print(f"Model output  : {MODEL_OUT}")

    X, y, y_label = load_data()
    print(f"[+] Loaded data: X shape = {X.shape}, y length = {len(y)}")

    model = train_xgb_surrogate(X, y)

    MODEL_OUT.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, MODEL_OUT)
    print(f"[✓] Surrogate model saved to: {MODEL_OUT}")


if __name__ == "__main__":
    main()
