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

        
        start = time.perf_counter()

        
        shap_values = self.explainer(instance)

        
        duration = time.perf_counter() - start

        
        try:
            n_samples = len(instance)
        except TypeError:
            n_samples = 1

        
        EXPLANATIONS_TOTAL.inc()
        EXPLAINED_SAMPLES_TOTAL.inc(n_samples)
        EXPLANATION_LATENCY_SECONDS.observe(duration)

        
        return shap_values.values.tolist()
