import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
import shap


class SurrogateExplainer:
    """
    Surrogate model + SHAP explainer for the LSTM anomaly scores.

    - Surrogate: RandomForestRegressor (fast, tree-based, مناسب برای SHAP TreeExplainer)
    - Target     : REAL LSTM anomaly scores (lstm_score)
    - explain_last(X):
        * برمی‌گرداند:
            mean_map  : dict[feature -> normalized mean |SHAP|]  در بازه 0..1
            top_pairs : list[(feature, normalized_shap)]  برای آخرین window
    """

    def __init__(self, n_estimators: int = 200, random_state: int = 42):
        self.model = RandomForestRegressor(
            n_estimators=n_estimators,
            random_state=random_state,
            n_jobs=-1,
        )
        self.explainer = None
        self.feature_names = None

    def fit(self, X: pd.DataFrame, y: pd.Series) -> None:
        """
        Train (or re-train) the surrogate model on the given batch.

        X : feature matrix (windows × features)
        y : REAL LSTM anomaly scores for the same windows
        """
        self.feature_names = list(X.columns)
        self.model.fit(X.values, y.values)
        # TreeExplainer روی مدل درختی
        self.explainer = shap.TreeExplainer(self.model)

    def explain_last(self, X: pd.DataFrame, top_k: int = 5):
        """
        Compute SHAP values for ALL rows in X, then:

        1) mean_map  : normalized mean |SHAP| per feature in [0, 1]
        2) top_pairs : top-k features for the LAST window, SHAP نرمال در [-1, 1]

        این خروجی‌ها مستقیم برای Prometheus/Grafana استفاده می‌شوند.
        """
        if self.explainer is None:
            raise ValueError("You must call fit() before explain_last().")

        # ---- 1) SHAP values for the whole batch ----
        shap_values = self.explainer.shap_values(X.values)  # shape: (n_samples, n_features)
        shap_values = np.array(shap_values)

        # اطمینان از 2D بودن
        if shap_values.ndim == 1:
            shap_values = shap_values.reshape(1, -1)

        # ---- 2) میانگین قدر مطلق ─ و نرمال‌سازی به 0..1 ----
        abs_vals = np.abs(shap_values)               # (n_samples, n_features)
        abs_mean = abs_vals.mean(axis=0)             # (n_features,)

        max_mean = float(abs_mean.max())
        if max_mean > 0.0:
            abs_mean_norm = abs_mean / max_mean
        else:
            abs_mean_norm = abs_mean

        mean_map = dict(zip(self.feature_names, abs_mean_norm.tolist()))

        # ---- 3) آخرین window ─ نرمال‌سازی برای top-k ----
        last_row = shap_values[-1]                   # (n_features,)
        abs_last = np.abs(last_row)
        max_last = float(abs_last.max())
        if max_last > 0.0:
            last_norm = last_row / max_last          # در بازه [-1, 1]
        else:
            last_norm = last_row

        idx = np.argsort(np.abs(last_norm))[::-1][:top_k]
        top_pairs = [
            (self.feature_names[i], float(last_norm[i]))
            for i in idx
        ]

        return mean_map, top_pairs
