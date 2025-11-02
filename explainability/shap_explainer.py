"""SHAP-based explanations for anomaly scores."""
import numpy as np

def explain_shap(model, X_batch, feature_names):
    """Return per-feature contribution for each sample.
    TODO: integrate shap.TreeExplainer / KernelExplainer depending on model.
    """
    contrib = np.zeros_like(X_batch, dtype=float)
    return {"contrib": contrib, "features": feature_names}
