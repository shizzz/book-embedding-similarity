import os
import joblib
import numpy as np
from app.settings.config import RERANKER_FILE
from .reranker import Reranker

class LightGBMReranker(Reranker):
    def __init__(self,):
        self.model = None

        if os.path.exists(RERANKER_FILE):
            try:
                self.model = joblib.load(RERANKER_FILE)
            except Exception:
                self.model = None

    def predict(self, X: np.ndarray) -> np.ndarray | None:
        if self.model is None:
            return None

        raw_scores = self.model.predict(X)

        # Минимум и максимум для нормализации
        min_score = raw_scores.min()
        max_score = raw_scores.max()

        # Защита от случая, когда все scores одинаковые
        if max_score == min_score:
            norm_scores = np.full_like(raw_scores, 50.0)  # середина шкалы
        else:
            # Нормализация в диапазон 0-100
            norm_scores = (raw_scores - min_score) / (max_score - min_score)

        return norm_scores