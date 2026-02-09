import numpy as np
from typing import List, Sequence, Tuple
from app.models import Book, Similar, Embedding, Feedbacks
from .baseSearchEngine import BaseSearchEngine
from app.settings.config import FEEDBACK_BOOST_FACTOR

class IndexSearchEngine(BaseSearchEngine):
    def __init__(
        self,
        index,
        books: Sequence[Book],
        feedbacks: Feedbacks,
        limit: int,
        exclude_same_authors: bool = False,
        logger = None,
    ):
        super().__init__(limit, exclude_same_authors)
        self.index = index
        self.books = list(books)
        self.feedbacks = feedbacks
        self.logger = logger

    def search(
        self,
        source: Book,
        embedding: Embedding
    ) -> List[Similar]:
        if self.index is None or self.index.ntotal == 0:
            return []

        query = embedding.vec.reshape(1, -1).astype(np.float32)
        k = min(self.limit * 10 + 200, self.index.ntotal)
        scores, indices = self.index.search(query, k)

        candidates: List[Tuple[float, Book]] = []

        for score_raw, idx in zip(scores[0], indices[0]):
            if idx == -1 or idx < 0 or idx >= len(self.books):
                continue

            candidate = self.books[idx]

            if self._should_skip(source, candidate):
                continue

            similarity = float(score_raw)
            boost = self.feedbacks.get_boost(source.id, candidate.id, FEEDBACK_BOOST_FACTOR)
            total_score = similarity + boost

            candidates.append((total_score, candidate))

        candidates.sort(key=lambda x: x[0], reverse=True)
        top = candidates[: self.limit]

        if not top:
            return []

        result: List[Similar] = []
        for score, candidate in top:
            result.append(Similar.from_books(score, source, candidate))

        return result

