import numpy as np
from typing import List, Sequence, Tuple
from app.models import Book, Embedding
from app.models.feedback import Feedback
from .similarSearchEngine import SimilarSearchEngine

class IndexSimilarSearchEngine(SimilarSearchEngine):
    def __init__(
        self,
        index,
        books: Sequence[Book],
        limit: int,
        exclude_same_authors: bool = False,
        step_percent: int = 5,
        logger = None,
    ):
        super().__init__(exclude_same_authors)
        self.index = index
        self.books = list[Book](books)
        self._limit = limit
        self._step_percent = step_percent
        self.logger = logger

    def search(
        self,
        source: Book,
        embedding: Embedding,
        feedbacks: List[Feedback],
        progress_callback=None
    ) -> List[Tuple[float, int, int]]:
        if self.index is None or self.index.ntotal == 0:
            return []

        step = max(1, self.index.ntotal * self._step_percent // 100)

        query = embedding.vec.reshape(1, -1).astype(np.float32)
        k = min(self._limit * 10 + 200, self.index.ntotal)
        scores, indices = self.index.search(query, k)

        candidates: List[Tuple[float, Book]] = []

        for score_raw, idx in zip(scores[0], indices[0]):
            if idx == -1 or idx < 0 or idx >= len(self.books):
                continue

            candidate = self.books[idx]

            if self._should_skip(source=source, candidate_name=candidate.file_name, candidate_title=candidate.title):
                continue

            similarity = float(score_raw)
            boost = feedbacks.get_boost(source.id, candidate.id)
            total_score = similarity + boost

            candidates.append((total_score, candidate))

            if progress_callback and idx % step == 0:
                percent = min(99, idx * 100 // self.index.ntotal)
                progress_callback(percent)

        candidates.sort(key=lambda x: x[0], reverse=True)
        top = candidates[: self._limit]

        if not top:
            return []

        result: List[Tuple[float, int, int]] = []
        for score, candidate in top:
            result.append((score, source.id, candidate.id))

        return result

