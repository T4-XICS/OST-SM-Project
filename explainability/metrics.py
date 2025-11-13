from prometheus_client import Counter, Histogram

# Total number of SHAP explanations computed
EXPLANATIONS_TOTAL = Counter(
    "explainability_explanations_total",
    "Total number of SHAP explanations computed"
)

# Total number of items explained
EXPLAINED_SAMPLES_TOTAL = Counter(
    "explainability_explained_samples_total",
    "Total number of samples processed by explainer"
)

# Time taken for each SHAP explanation call
EXPLANATION_LATENCY_SECONDS = Histogram(
    "explainability_explanation_latency_seconds",
    "SHAP explanation latency in seconds"
)
