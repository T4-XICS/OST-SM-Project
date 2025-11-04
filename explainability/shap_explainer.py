# -*- coding: utf-8 -*-
# SHAP Explainer Module
import shap

class ShapExplainer:
    def __init__(self, model):
        self.model = model
        self.explainer = shap.Explainer(model)

    def explain_instance(self, instance):
        shap_values = self.explainer(instance)
        return shap_values.values.tolist()
