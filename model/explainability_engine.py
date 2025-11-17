import logging
import threading
from collections import deque
from typing import Dict, Iterable, List, Optional

import numpy as np
import shap
import torch
from lime.lime_tabular import LimeTabularExplainer

from network import loss_function


class ExplainabilityEngine:
    """
    Provides SHAP and LIME explanations for anomaly scores that come out of the
    LSTM-VAE model. The engine keeps a rolling background window so that both
    explainers can be fit incrementally while Spark micro-batches keep streaming.
    """

    def __init__(
        self,
        feature_names: List[str],
        sequence_length: int,
        background_window: int = 128,
        min_background: int = 24,
        shap_nsamples: int = 80,
        lime_num_features: int = 8,
    ) -> None:
        self.feature_names = feature_names
        self.sequence_length = sequence_length
        self.background_window = background_window
        self.min_background = min_background
        self.shap_nsamples = shap_nsamples
        self.lime_num_features = lime_num_features

        self._background = deque(maxlen=background_window)
        self._background_version = 0
        self._shap_explainer: Optional[shap.KernelExplainer] = None
        self._lime_explainer: Optional[LimeTabularExplainer] = None
        self._shap_version = -1
        self._lime_version = -1
        self._lock = threading.Lock()
        self._logger = logging.getLogger("ExplainabilityEngine")

    def add_background_rows(self, rows: Iterable[np.ndarray]) -> None:
        """
        Feed latest (single timestep) feature vectors so the explainers can
        build representative baselines. Typically invoked with the trailing row
        of each sequence that went through inference.
        """
        added = False
        for row in rows:
            if row is None:
                continue
            arr = np.asarray(row, dtype=np.float32)
            if arr.shape[0] != len(self.feature_names):
                continue
            self._background.append(arr)
            added = True

        if added:
            self._background_version += 1

    def _score_samples(
        self, model: torch.nn.Module, device: torch.device, samples: np.ndarray
    ) -> np.ndarray:
        """
        Converts a batch of flattened sensor readings into synthetic sequences
        so the trained model can produce anomaly scores.
        """
        if samples.ndim == 1:
            samples = samples.reshape(1, -1)

        with torch.no_grad():
            batch = torch.tensor(samples, dtype=torch.float32, device=device)
            # Tile each snapshot to match the temporal dimension expected by the model.
            sequences = batch.unsqueeze(1).repeat(1, self.sequence_length, 1)
            recon, mean, logvar = model(sequences)
            losses = loss_function(recon, sequences, mean, logvar)
            if isinstance(losses, torch.Tensor):
                losses = losses.detach().cpu().numpy()
        return np.asarray(losses, dtype=np.float32)

    def _ensure_shap_explainer(
        self, model: torch.nn.Module, device: torch.device
    ) -> bool:
        if len(self._background) < self.min_background:
            return False

        if self._shap_explainer is not None and self._shap_version == self._background_version:
            return True

        with self._lock:
            if self._shap_explainer is not None and self._shap_version == self._background_version:
                return True
            background = np.stack(self._background)
            model_fn = lambda data: self._score_samples(model, device, data)
            self._shap_explainer = shap.KernelExplainer(model_fn, background)
            self._shap_version = self._background_version
            self._logger.info(
                "Initialized SHAP explainer with %d background samples.",
                background.shape[0],
            )
        return True

    def _ensure_lime_explainer(self) -> bool:
        if len(self._background) < self.min_background:
            return False

        if self._lime_explainer is not None and self._lime_version == self._background_version:
            return True

        with self._lock:
            if self._lime_explainer is not None and self._lime_version == self._background_version:
                return True
            background = np.stack(self._background)
            self._lime_explainer = LimeTabularExplainer(
                training_data=background,
                feature_names=self.feature_names,
                verbose=False,
                mode="regression",
            )
            self._lime_version = self._background_version
            self._logger.info(
                "Initialized LIME explainer with %d background samples.",
                background.shape[0],
            )
        return True

    def generate(
        self,
        model: torch.nn.Module,
        device: torch.device,
        sample: np.ndarray,
    ) -> Dict[str, Optional[Dict[str, float]]]:
        """
        Returns SHAP and LIME contributions for a single sensor snapshot vector.
        """
        result: Dict[str, Optional[Dict[str, float]]] = {"shap": None, "lime": None}
        vector = np.asarray(sample, dtype=np.float32).reshape(1, -1)

        if vector.shape[1] != len(self.feature_names):
            self._logger.warning(
                "Explainability sample has %d features but expected %d.",
                vector.shape[1],
                len(self.feature_names),
            )
            return result

        try:
            if self._ensure_shap_explainer(model, device) and self._shap_explainer:
                shap_values = self._shap_explainer.shap_values(
                    vector, nsamples=self.shap_nsamples
                )
                if isinstance(shap_values, list):
                    shap_values = shap_values[0]
                result["shap"] = {
                    feature: float(weight)
                    for feature, weight in zip(self.feature_names, shap_values.flatten())
                }
        except Exception as exc:
            self._logger.error("SHAP explanation failed: %s", exc, exc_info=True)

        try:
            if self._ensure_lime_explainer() and self._lime_explainer:
                lime_exp = self._lime_explainer.explain_instance(
                    vector.flatten(),
                    lambda data: self._score_samples(model, device, data),
                    num_features=min(self.lime_num_features, len(self.feature_names)),
                )
                weights: Dict[str, float] = {}
                local_map = lime_exp.local_exp
                if isinstance(local_map, dict) and local_map:
                    _, tuples = next(iter(local_map.items()))
                    for feature_idx, weight in tuples:
                        feature_name = self.feature_names[feature_idx]
                        weights[feature_name] = float(weight)
                result["lime"] = weights
        except Exception as exc:
            self._logger.error("LIME explanation failed: %s", exc, exc_info=True)

        return result
