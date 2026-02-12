from typing import Protocol
import numpy as np

class Reranker(Protocol):
    model: object | None
    def predict(self, X: np.ndarray) -> np.ndarray:
        ...