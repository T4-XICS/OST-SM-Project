# explainability/local_demo.py
# Real SHAP (TreeSHAP) over rolling features, lightweight & streaming-friendly.

import time
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
import shap

from explainability.features import rolling_features, proxy_anomaly_score
from explainability.metrics_exporter import start as start_exporter, publish_shap_mean, publish_topk

# ---------- Config ----------
CSV = "datasets/swat/normal/SWaT_Dataset_Normal_v0_1.csv"
WINDOW = 30
TOPK = 5
PORT = 9109
REFIT_EVERY = 300            
EXPLAIN_INTERVAL_SEC = 5    
N_TRAIN_ROWS = 5000           
N_BACKGROUND = 200           

SENSOR_COLS = [
    "LIT101", "FIT101", "P102", "MV101",
    "LIT301", "FIT301", "MV303", "P601",
    "AIT202", "AIT203"
]

def load_numeric(path):
    df = pd.read_csv(path)
    numeric = df.select_dtypes("number")
    return numeric

def fit_model(feats, y):
    # light moddel TreeSHAP
    model = RandomForestRegressor(
        n_estimators=100,
        max_depth=6,
        n_jobs=-1,
        random_state=42
    )
    # 
    X_tr = feats.iloc[-N_TRAIN_ROWS:] if len(feats) > N_TRAIN_ROWS else feats
    y_tr = y.iloc[-len(X_tr):]
    model.fit(X_tr, y_tr)
    return model

def make_explainer(model, feats):
    # background   SHAP
    bg = feats.sample(min(N_BACKGROUND, len(feats)), random_state=42)
    explainer = shap.TreeExplainer(model, data=bg, feature_names=list(feats.columns))
    return explainer

def shap_mean_abs(explainer, X_last):
    # TreeExplainer → shap values
    sv = explainer.shap_values(X_last)
    
    if isinstance(sv, list):   # multi-output
        sv = sv[0]
    mean_abs = np.abs(sv).mean(axis=0)
    return mean_abs

def main():
    start_exporter(PORT)

    df = load_numeric(CSV)
    use_cols = [c for c in SENSOR_COLS if c in df.columns]
    if not use_cols:
        raise ValueError(f"No expected sensor columns found. First columns: {list(df.columns)[:10]}")

    feats = rolling_features(df, use_cols, window=WINDOW)
    y = proxy_anomaly_score(feats)

    
    model = fit_model(feats, y)
    explainer = make_explainer(model, feats)
    last_refit = time.time()

    print(f"[demo] features={feats.shape}, sensors={len(use_cols)}, window={WINDOW}")
    print(f"[metrics] Prometheus at http://localhost:{PORT}/metrics")

    while True:
        if time.time() - last_refit > REFIT_EVERY:
            model = fit_model(feats, y)
            explainer = make_explainer(model, feats)
            last_refit = time.time()
            print("[demo] model re-fitted")

        
        X_last = feats.tail(WINDOW)
        mean_map_np = shap_mean_abs(explainer, X_last)

        # map: feature_name -> mean_abs
        mean_map = {feat: float(val) for feat, val in zip(feats.columns, mean_map_np)}

        
        top_pairs = sorted(mean_map.items(), key=lambda x: x[1], reverse=True)[:TOPK]

        
        publish_shap_mean(mean_map)
        publish_topk(top_pairs)

        print(f"[demo] SHAP pushed → top1={top_pairs[0] if top_pairs else None}")
        time.sleep(EXPLAIN_INTERVAL_SEC)

if __name__ == "__main__":
    main()
