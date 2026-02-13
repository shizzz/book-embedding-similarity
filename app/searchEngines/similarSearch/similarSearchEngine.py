import numpy as np
from typing import List, Tuple
from app.hnsw.rerankers import Reranker
from app.models import Book, Embedding

class SimilarSearchEngine:
    def __init__(self, exclude_same_authors: bool, reranker: Reranker = None):
        self._exclude_same_authors = exclude_same_authors
        self._reranker = reranker

    def _should_skip(
            self,
            source: Book,
            candidate_name: str,
            candidate_title: str,
            seen: set[tuple[str, tuple[str, ...]]],
            candidate_authors: list[str] = None,
        ) -> bool:
        if source.file_name is not None and source.file_name == candidate_name:
            return True

        if source.title is not None and source.title == candidate_title:
            return True

        key = (
            candidate_title,
            tuple(sorted(candidate_authors)) if candidate_authors else ()
        )

        if key in seen:
            return True
        
        seen.add(key)

        return False

    def _rerank(
        self,
        candidates: list[tuple[float, Book]],
    ):
        # Если модели нет или кандидатов нет — возвращаем исходные
        if not self._reranker or not self._reranker.model or not candidates:
            return candidates

        X = []
        valid = []

        for sim, book in candidates:
            # Фича только исходный sim
            X.append([sim])
            valid.append(book)

        X = np.array(X, dtype=np.float32)
        if X.ndim == 1:
            X = X.reshape(1, -1)

        try:
            scores = self._reranker.model.predict(X, raw_score=False)

            if scores is None or np.all(np.isnan(scores)) or np.all(scores < 1e-6):
                scores = np.array([sim for sim, _ in candidates], dtype=np.float32)
        except Exception:
            scores = np.array([sim for sim, _ in candidates], dtype=np.float32)

        reranked = list(zip(scores, valid))
        reranked.sort(key=lambda x: x[0], reverse=True)
        return reranked

    
    def search(
        self,
        source: Book,
        embedding: Embedding,
        progress_callback=None
    ) -> List[Tuple[float, int, int]]:
        raise NotImplementedError()

