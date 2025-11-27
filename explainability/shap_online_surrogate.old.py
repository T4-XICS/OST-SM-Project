import time
import pandas as pd
import numpy as np
import logging
from explainability.surrogate_explainer import SurrogateExplainer
from explainability import metrics_exporter as metrics
import joblib

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# ---------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------
WINDOW = 256               # batch size
FEATURES = None           # automatic from CSV
SLEEP = 0.3               # simulate realtime batch arrival
CSV_PATH = "explainability/shap_data_surrogate.csv"
MODEL_PATH = "explainability/xgb_surrogate_lstm_score.pkl"


# ---------------------------------------------------------------------
# LOAD surrogate model
# ---------------------------------------------------------------------
def load_surrogate():
    logger.info("[shap_online] Loading surrogate model...")
    model = joblib.load(MODEL_PATH)
    logger.info("[shap_online] Loaded model from %s", MODEL_PATH)
    return model


# ---------------------------------------------------------------------
# MAIN LOOP
# ---------------------------------------------------------------------
def main():
    metrics.start(9109)

    # load CSV dataset (simulating live batches)
    df = pd.read_csv(CSV_PATH)
    logger.info("[offline_test] Loaded surrogate CSV, shape = %s", df.shape)

    global FEATURES
    FEATURES = [c for c in df.columns if c != "score"]

    # load model
    surrogate = SurrogateExplainer()
    surrogate.model = load_surrogate()
    surrogate.feature_names = FEATURES
    surrogate.explainer = surrogate.make_explainer()

    # sliding window buffer
    window_list = []

    # simulate live incoming batches
    for start in range(0, len(df), WINDOW):
        batch = df.iloc[start:start + WINDOW]

        if len(batch) < WINDOW:
            break

        X = batch[FEATURES]
        y = batch["score"]

        # explain
        abs_mean, top_pairs = surrogate.explain_last(X)

        # publish global batch mean
        metrics.publish_shap_mean(abs_mean)

        # sliding window update
        window_list.append(abs_mean)
        if len(window_list) > 10:   # sliding window of 10 batches
            window_list.pop(0)

        # compute sliding average
        window_avg = {
            f: float(np.mean([w[f] for w in window_list]))
            for f in FEATURES
        }
        metrics.publish_shap_window(window_avg)

        # publish top-k
        metrics.publish_topk(top_pairs)

        logger.info("[shap_online] Explained batch (%d samples)" % len(batch))

        time.sleep(SLEEP)

    logger.info("[offline_test] DONE. Check Prometheus/Grafana now.")


if __name__ == "__main__":
    main()
