import os
import joblib
import numpy as np
from app.settings.config import RERANKER_FILE

class LightGBMReranker:
    def __init__(self,):
        self.model = None

        if os.path.exists(RERANKER_FILE):
            try:
                self.model = joblib.load(RERANKER_FILE)
            except Exception:
                self.model = None

    def predict(self, X: np.ndarray) -> np.ndarray | None:
        return self.model.predict(X)
