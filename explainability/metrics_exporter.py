import logging
from typing import Dict, Iterable, Tuple

from prometheus_client import start_http_server, Gauge, Histogram, Counter

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Metrics definitions
# -----------------------------------------------------------------------------

# 1) Global / per-batch mean |SHAP| per feature
GAUGE_SHAP_MEAN = Gauge(
    "explain_shap_mean_abs_importance",
    "Mean |SHAP| value per feature for the current batch",
    ["feature"],
)

# 2) Multi-window mean |SHAP| per feature
#    window label is one of: "5", "10", "20", "50"
GAUGE_SHAP_WINDOW = Gauge(
    "explain_shap_window_mean",
    "Mean |SHAP| value per feature over sliding windows of different sizes",
    ["feature", "window"],
)

# 3) Normal vs Attack SHAP (optional – if label is available)
GAUGE_SHAP_NORMAL = Gauge(
    "explain_shap_normal",
    "Mean |SHAP| value per feature on NORMAL samples",
    ["feature"],
)

GAUGE_SHAP_ATTACK = Gauge(
    "explain_shap_attack",
    "Mean |SHAP| value per feature on ATTACK / anomalous samples",
    ["feature"],
)

# 4) Top-K features for the latest batch
GAUGE_TOPK = Gauge(
    "explain_top_feature_weight",
    "Weight of top-k features for the latest explained batch",
    ["rank", "feature"],
)

# 5) Latency and errors
HIST_LATENCY = Histogram(
    "explain_latency_seconds",
    "Latency of SHAP explanation per batch",
)

COUNTER_ERR = Counter(
    "explain_errors_total",
    "Number of SHAP explanation failures",
)

_METRICS_STARTED = False  # internal flag to avoid starting server twice


# -----------------------------------------------------------------------------
# Server start
# -----------------------------------------------------------------------------
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


# -----------------------------------------------------------------------------
# Publish helpers
# -----------------------------------------------------------------------------
def publish_shap_mean(abs_mean: Dict[str, float]) -> None:
    """
    Publish mean absolute SHAP values per feature for the current batch.

    abs_mean: dict[feature_name -> float_value]
    """
    if not abs_mean:
        return

    for f, v in abs_mean.items():
        GAUGE_SHAP_MEAN.labels(feature=str(f)).set(float(v))


def publish_shap_windows(
    windows: Dict[str, Dict[str, float]],
) -> None:
    """
    Publish multi-window mean |SHAP| per feature.

    windows: dict[window_label -> dict[feature_name -> float_value]]
             e.g. {
                "5":  {"FIT101__mean": 0.02, ...},
                "10": {"FIT101__mean": 0.03, ...},
                "20": {...},
                "50": {...},
             }
    """
    if not windows:
        return

    for window_label, mean_map in windows.items():
        if not mean_map:
            continue
        for f, v in mean_map.items():
            GAUGE_SHAP_WINDOW.labels(
                feature=str(f),
                window=str(window_label),
            ).set(float(v))


def publish_shap_normal_attack(
    normal_mean: Dict[str, float],
    attack_mean: Dict[str, float],
) -> None:
    """
    Publish SHAP means for NORMAL vs ATTACK subsets (optional).
    """
    # Normal
    if normal_mean:
        for f, v in normal_mean.items():
            GAUGE_SHAP_NORMAL.labels(feature=str(f)).set(float(v))

    # Attack
    if attack_mean:
        for f, v in attack_mean.items():
            GAUGE_SHAP_ATTACK.labels(feature=str(f)).set(float(v))


def publish_topk(pairs: Iterable[Tuple[str, float]]) -> None:
    """
    Publish top-k feature importances for the latest explained batch.

    pairs: iterable of (feature_name, weight) for the current batch.
    """
    if not pairs:
        return

    for i, (f, w) in enumerate(pairs, start=1):
        GAUGE_TOPK.labels(rank=str(i), feature=str(f)).set(float(w))
