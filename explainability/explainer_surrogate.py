import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
import shap

class SurrogateExplainer:
    """
    Train a surrogate model to approximate anomaly score behavior,
    then use SHAP (TreeExplainer) to interpret feature contributions.
    """
    def __init__(self, n_estimators=200, random_state=42):
        self.model = RandomForestRegressor(n_estimators=n_estimators, random_state=random_state)
        self.explainer = None
        self.feature_names = None

    def fit(self, X: pd.DataFrame, y: pd.Series):
        """Train the surrogate model."""
        self.feature_names = list(X.columns)
        self.model.fit(X.values, y.values)
        self.explainer = shap.TreeExplainer(self.model)

    def explain_last(self, X: pd.DataFrame, top_k: int = 5):
        """Get SHAP values for the latest window."""
        if self.explainer is None:
            raise ValueError("You must call fit() before explain_last().")

        shap_values = self.explainer.shap_values(X.values)
        abs_mean = np.abs(shap_values).mean(axis=0)
        mean_map = dict(zip(self.feature_names, abs_mean.tolist()))

        last_row = shap_values[-1]
        idx = np.argsort(np.abs(last_row))[::-1][:top_k]
        top_pairs = [(self.feature_names[i], float(last_row[i])) for i in idx]

        return mean_map, top_pairs
