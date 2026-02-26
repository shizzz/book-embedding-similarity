import numpy as np
from typing import List, Tuple
from app.hnsw.rerankers import Reranker
from app.models import Book, BookRegistry

class SimilarSearchEngine:
    def __init__(self, exclude_same_authors: bool, reranker: Reranker = None):
        self._exclude_same_authors = exclude_same_authors
        self._reranker = reranker

    def _should_skip(
        self,
        source: Book,
        candidate: Book,
        seen: set[tuple[str, tuple[str, ...]]]
    ) -> bool:
        # 1. тот же файл
        if source.file_name and source.file_name == candidate.file_name:
            return True

        # 2. то же название
        if source.title and source.title == candidate.title:
            return True

        # 3. исключение по авторам
        if self._exclude_same_authors and source.authors and candidate.authors:
            if source.authors & candidate.authors:
                return True

        # 4. проверка уникальности (title + authors)
        key = (candidate.title, candidate.authors_key)
        if key in seen:
            return True

        seen.add(key)
        return False

    def _rerank(
        self,
        candidates: list[tuple[float, Book]],
    ):
        # Если модели нет или кандидатов нет — возвращаем исходные
        if not candidates:
            return candidates
        
        if not self._reranker or not self._reranker.model:
            candidates.sort(key=lambda x: x[0], reverse=True)
            return candidates

        X = []
        valid = []

        for sim, book in candidates:
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
        sources: BookRegistry,
        progress_callback=None
    ) -> List[Tuple[float, int, int]]:
        raise NotImplementedError()

