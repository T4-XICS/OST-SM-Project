import numpy as np
import pandas as pd
import shap
from sklearn.ensemble import RandomForestRegressor


class SurrogateExplainer:
    """
    Surrogate model to approximate anomaly-score behavior and provide SHAP
    explanations for feature contributions.
    """

    def __init__(self, n_estimators: int = 200, random_state: int = 42) -> None:
        # RandomForest surrogate (could be any tree-based regressor)
        self.model = RandomForestRegressor(
            n_estimators=n_estimators,
            random_state=random_state,
            n_jobs=-1,
        )
        self.explainer: shap.TreeExplainer | None = None
        self.feature_names: list[str] | None = None

    def fit(self, X: pd.DataFrame, y: pd.Series) -> None:
        """
        Train the surrogate model on given features X and target y.

        X: pandas DataFrame (n_samples × n_features)
        y: pandas Series (n_samples,) – anomaly-like score
        """
        if not isinstance(X, pd.DataFrame):
            raise TypeError("X must be a pandas DataFrame.")

        if not isinstance(y, (pd.Series, pd.DataFrame)):
            raise TypeError("y must be a pandas Series or 1-column DataFrame.")

        if isinstance(y, pd.DataFrame):
            if y.shape[1] != 1:
                raise ValueError("y DataFrame must have exactly one column.")
            y = y.iloc[:, 0]

        self.feature_names = list(X.columns)
        self.model.fit(X.values, y.values)

        # TreeExplainer works well for tree-based models
        self.explainer = shap.TreeExplainer(self.model)

    def explain_last(
        self,
        X: pd.DataFrame,
        top_k: int = 5,
    ) -> tuple[dict[str, float], list[tuple[str, float]]]:
        """
        Compute SHAP values and return:

        - mean_map: mean |SHAP| per feature over all rows
        - top_pairs: list of (feature_name, shap_value) for last row, sorted by |value|
        """
        if self.explainer is None or self.feature_names is None:
            raise ValueError("You must call fit() before explain_last().")

        if not isinstance(X, pd.DataFrame):
            raise TypeError("X must be a pandas DataFrame.")

        X_arr = X[self.feature_names].astype(float).values

        shap_values = self.explainer.shap_values(X_arr)

        # global mean |SHAP| per feature
        abs_mean = np.abs(shap_values).mean(axis=0)
        mean_map = dict(zip(self.feature_names, abs_mean.tolist()))

        # top-k features for the last sample
        last_row = shap_values[-1]
        idx = np.argsort(np.abs(last_row))[::-1][:top_k]
        top_pairs = [(self.feature_names[i], float(last_row[i])) for i in idx]

        return mean_map, top_pairs


__all__ = ["SurrogateExplainer"]
