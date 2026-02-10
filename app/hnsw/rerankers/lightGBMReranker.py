import joblib
import numpy as np
from typing import Sequence
from app.models import Book
from app.settings.config import RERANKER_FILE

class LightGBMReranker:
    def __init__(self,):
        self.model = joblib.load(f"{RERANKER_FILE}")

    def predict(
        self,
        source_emb: np.ndarray,
        candidates_emb: Sequence[np.ndarray],
        candidates_books: Sequence[Book],
        source_book: Book
    ) -> np.ndarray:
        X = []

        for emb, book in zip(candidates_emb, candidates_books):
            # косинусная похожесть
            cos = float(
                np.dot(source_emb, emb) /
                (np.linalg.norm(source_emb) * np.linalg.norm(emb) + 1e-8)
            )
            # same_author фича
            same_author = int(book.author == source_book.author)

            X.append([cos, same_author])

        X_arr = np.array(X, dtype=np.float32)
        scores = self.model.predict(X_arr)

        return scores
