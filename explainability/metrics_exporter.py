from prometheus_client import Gauge, start_http_server


# ---------------------------
# SHAP mean per batch (0-1)
# ---------------------------
GAUGE_SHAP_MEAN = Gauge(
    "explain_shap_mean_abs",
    "Mean |SHAP| value per feature per batch (normalized 0-1)",
    ["feature", "batch"]
)


def start_metrics_server(port: int):
    """
    Expose /metrics endpoint for Prometheus.
    """
    start_http_server(port)
    print(f"[metrics] Prometheus exposed at http://0.0.0.0:{port}/metrics")


def publish_shap_mean(shap_dict: dict, batch_id: int):
    """
    Publish mean SHAP values per feature.
    shap_dict example:
        {"FIT101__zlast": 0.55, "P301__zlast": 0.12, ...}
    """
    for feat, val in shap_dict.items():
        GAUGE_SHAP_MEAN.labels(
            feature=feat,
            batch=str(batch_id)
        ).set(float(val))


# ---------------------------
# OPTIONAL: Top-K features
# ---------------------------
GAUGE_TOPK = Gauge(
    "explain_topk_features",
    "Top-K SHAP features for last window",
    ["rank", "feature"]
)


def publish_topk(top_pairs):
    """
    Publish SHAP top-k features.
    top_pairs example:
        [("FIT101__zlast", 0.9), ("P302__zlast", 0.7), ...]
    """
    for rank, (feat, val) in enumerate(top_pairs, start=1):
        GAUGE_TOPK.labels(
            rank=str(rank),
            feature=feat
        ).set(float(val))
# ---------------------------
# ANOMALY SCORE GAUGE (0-1)
# ---------------------------
GAUGE_ANOMALY_SCORE = Gauge(
    "inference_anomaly_score",
    "Latest LSTM anomaly score (0-1) per batch",
    ["batch"]
)

def publish_anomaly_score(score: float, batch_id: int):
    GAUGE_ANOMALY_SCORE.labels(batch=str(batch_id)).set(float(score))
