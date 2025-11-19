# Fast SHAP for Surrogate Model (sampled subset)
# explainability/compute_shap_surrogate.py

import pandas as pd
import numpy as np
import joblib
from pathlib import Path
import shap
import matplotlib.pyplot as plt

REPO_ROOT = Path(__file__).resolve().parents[1]

SURROGATE_CSV = REPO_ROOT / "explainability" / "swat_surrogate.csv"
MODEL_PATH = REPO_ROOT / "explainability" / "xgb_surrogate_lstm_score.pkl"

OUT_DIR = REPO_ROOT / "explainability" / "shap_outputs"
OUT_DIR.mkdir(exist_ok=True)


def main():
    print("=== FAST SHAP for Surrogate XGBoost ===")
    print("Loading dataset + model...")

    df = pd.read_csv(SURROGATE_CSV)
    model = joblib.load(MODEL_PATH)

    feature_cols = [c for c in df.columns if c not in ["lstm_score", "label"]]

    # ---- FAST SUBSAMPLING (CRITICAL) ----
    print("[+] Sampling 3000 rows for SHAP...")
    df_small = df.sample(n=3000, random_state=42)

    X = df_small[feature_cols].astype(np.float32)
    shap_labels = df_small["label"].values

    print("[+] Computing SHAP values on subset...")
    explainer = shap.TreeExplainer(model)
    shap_values = explainer.shap_values(X)

    # --- Global bar plot
    plt.figure(figsize=(12, 8))
    shap.summary_plot(shap_values, X, plot_type='bar', show=False)
    plt.savefig(OUT_DIR / "shap_global_bar.png")
    plt.close()

    # --- Global summary
    plt.figure(figsize=(12, 8))
    shap.summary_plot(shap_values, X, show=False)
    plt.savefig(OUT_DIR / "shap_global_summary.png")
    plt.close()

    # --- Feature importance CSV
    importance_df = pd.DataFrame({
        "feature": feature_cols,
        "importance_mean_abs": np.abs(shap_values).mean(axis=0),
    }).sort_values("importance_mean_abs", ascending=False)
    importance_df.to_csv(OUT_DIR / "shap_feature_importance.csv", index=False)

    print("[✓] Global SHAP files saved!")

    # --- Local example: one normal
    normal_i = np.where(shap_labels == 0)[0][0]
    shap.force_plot(
        explainer.expected_value,
        shap_values[normal_i],
        X.iloc[normal_i, :],
        matplotlib=True,
        show=False,
    )
    plt.savefig(OUT_DIR / "shap_local_normal.png")
    plt.close()

    # --- Local example: one attack
    attack_i = np.where(shap_labels == 1)[0][0]
    shap.force_plot(
        explainer.expected_value,
        shap_values[attack_i],
        X.iloc[attack_i, :],
        matplotlib=True,
        show=False,
    )
    plt.savefig(OUT_DIR / "shap_local_attack.png")
    plt.close()

    print("\n[✓] ALL SHAP FILES SAVED to shap_outputs/")


if __name__ == "__main__":
    main()
