# -*- coding: utf-8 -*-
# LIME Explainer Module
from lime import lime_tabular

class LimeExplainer:
    def __init__(self, model, feature_names):
        self.explainer = lime_tabular.LimeTabularExplainer(
            training_data=None,
            feature_names=feature_names,
            mode="regression"
        )
        self.model = model

    def explain_instance(self, instance):
        explanation = self.explainer.explain_instance(
            data_row=instance,
            predict_fn=self.model.predict
        )
        return explanation.as_list()
