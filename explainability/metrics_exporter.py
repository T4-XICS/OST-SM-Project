import logging
from prometheus_client import start_http_server, Gauge, Histogram, Counter

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Metrics definitions
# -----------------------------------------------------------------------------
GAUGE_SHAP_MEAN = Gauge(
    "explain_shap_mean_abs_importance",
    "Mean |SHAP| value per feature",
    ["feature"],
)

GAUGE_TOPK = Gauge(
    "explain_top_feature_weight",
    "Weight of top-k features for the latest explained sample",
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

_METRICS_STARTED = False  # internal flag to avoid starting server twice


def start(port: int = 9109) -> None:
    """
    Start the Prometheus metrics HTTP server on the given port.

    This function is idempotent: calling it multiple times will not
    start multiple HTTP servers.
    """
    global _METRICS_STARTED
    if _METRICS_STARTED:
        logger.info("[metrics] Prometheus server already running on port %s", port)
        return

    start_http_server(port)
    _METRICS_STARTED = True
    logger.info(
        "[metrics] Prometheus running at http://0.0.0.0:%s/metrics",
        port,
    )


def publish_shap_mean(abs_mean: dict) -> None:
    """
    Publish mean absolute SHAP values per feature.

    abs_mean: dict[feature_name -> float_value]
    """
    if not abs_mean:
        return

    for f, v in abs_mean.items():
        GAUGE_SHAP_MEAN.labels(feature=str(f)).set(float(v))


def publish_topk(pairs) -> None:
    """
    Publish top-k feature importances for the latest explanation.

    pairs: iterable of (feature_name, weight) for the current sample.
    """
    if not pairs:
        return

    for i, (f, w) in enumerate(pairs, start=1):
        GAUGE_TOPK.labels(rank=str(i), feature=str(f)).set(float(w))
