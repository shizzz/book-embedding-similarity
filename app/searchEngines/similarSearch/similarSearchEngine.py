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

    def _apply_reranker_delta(self, sims: np.ndarray, ids: np.ndarray, alpha: float = 0.2):
        """
        Применяет дельту модели к FAISS score.
        Возвращает просто order индексов для сортировки.
        """
        ranks = np.arange(len(sims), dtype=np.float32)
        features = np.column_stack([sims, ranks]).astype(np.float32)

        delta = self.model.predict(features)
        final_scores = sims + alpha * delta

        # возвращаем только индексы в порядке сортировки
        order = np.argsort(-final_scores)
        return order
    
    def _rerank(self, candidates: list[tuple[float, Book]]):
        if not candidates:
            return candidates

        if not self._reranker or not self._reranker.model:
            candidates.sort(key=lambda x: x[0], reverse=True)
            return candidates

        sims = np.array([s for s, _ in candidates], dtype=np.float32)
        ids  = np.array([c.id for _, c in candidates], dtype=np.int32)

        order = self._apply_reranker_delta(sims, ids, alpha=0.2)

        id_to_candidate = {c.id: c for _, c in candidates}
        reranked = [id_to_candidate[i] for i in ids[order]]

        return reranked

    def search(
        self,
        sources: BookRegistry,
        progress_callback=None
    ) -> List[Tuple[float, int, int]]:
        raise NotImplementedError()

