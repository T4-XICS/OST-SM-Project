from prometheus_client import start_http_server, Gauge, Histogram, Counter

# Define Prometheus metrics
GAUGE_SHAP_MEAN = Gauge(
    "explain_shap_mean_abs_importance",
    "Mean |SHAP| per feature",
    ["feature"]
)

GAUGE_TOPK = Gauge(
    "explain_top_feature_weight",
    "Weight of top-k features (latest row)",
    ["rank", "feature"]
)

HIST_LATENCY = Histogram("explain_latency_seconds", "Explanation latency")
COUNTER_ERR = Counter("explain_errors_total", "Explanation failures")

def start(port: int = 9109):
    """Start the Prometheus metrics server."""
    start_http_server(port)
    print(f"[metrics] Prometheus running at http://localhost:{port}/metrics")

def publish_shap_mean(abs_mean: dict):
    """Publish average absolute SHAP values."""
    for f, v in abs_mean.items():
        GAUGE_SHAP_MEAN.labels(feature=str(f)).set(float(v))

def publish_topk(pairs):
    """Publish top-K feature weights of the latest observation."""
    for i, (f, w) in enumerate(pairs, start=1):
        GAUGE_TOPK.labels(rank=str(i), feature=str(f)).set(float(w))
