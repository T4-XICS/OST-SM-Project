# -*- coding: utf-8 -*-
# SHAP Explainer Module

import time
import shap

from explainability.metrics import (
    EXPLANATIONS_TOTAL,
    EXPLAINED_SAMPLES_TOTAL,
    EXPLANATION_LATENCY_SECONDS,
)


class ShapExplainer:
    def __init__(self, model):
        self.model = model
        self.explainer = shap.Explainer(model)

    def explain_instance(self, instance):
        """
        Compute SHAP values for a single instance (or a small batch)
        and update Prometheus metrics.
        """

        # Start timer
        start = time.perf_counter()

        # Run SHAP
        shap_values = self.explainer(instance)

        # Stop timer
        duration = time.perf_counter() - start

        # Try to infer how many samples we explained
        try:
            n_samples = len(instance)
        except TypeError:
            n_samples = 1

        # Update metrics
        EXPLANATIONS_TOTAL.inc()
        EXPLAINED_SAMPLES_TOTAL.inc(n_samples)
        EXPLANATION_LATENCY_SECONDS.observe(duration)

        # Return as Python list (same as before)
        return shap_values.values.tolist()
