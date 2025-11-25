import logging
from prometheus_client import start_http_server, Gauge, Histogram, Counter

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Metrics definitions
# -----------------------------------------------------------------------------
GAUGE_SHAP_MEAN = Gauge(
    "explain_shap_mean_abs_importance",
    "Mean |SHAP| value per feature (normalized to [0, 1])",
    ["feature"],
)

GAUGE_TOPK = Gauge(
    "explain_top_feature_weight",
    "Weight of top-k features for the latest explained window",
    ["rank", "feature"],
)

HIST_LATENCY = Histogram(
    "explain_latency_seconds",
    "Latency of SHAP explanation per batch",
)

COUNTER_ERR = Counter(
    "explain_errors_total",
    "Number of SHAP explanation failures",
)

_METRICS_STARTED = False


def start_metrics_server(port: int = 9109) -> None:
    """
    Correct name: start_metrics_server (called by stream_explain_job)
    """
    global _METRICS_STARTED
    if _METRICS_STARTED:
        logger.info("[metrics] already running on port %s", port)
        return

    start_http_server(port)
    _METRICS_STARTED = True
    logger.info("[metrics] Prometheus exposed at http://0.0.0.0:%s/metrics", port)


def publish_shap_mean(abs_mean: dict) -> None:
    if not abs_mean:
        return
    for f, v in abs_mean.items():
        GAUGE_SHAP_MEAN.labels(feature=str(f)).set(float(v))


def publish_topk(pairs) -> None:
    if not pairs:
        return
    for i, (f, w) in enumerate(pairs, start=1):
        GAUGE_TOPK.labels(rank=str(i), feature=str(f)).set(float(w))
