import numpy as np
from typing import List, Sequence, Tuple
from app.models import Book, BookRegistry
from app.hnsw.rerankers import Reranker
from .similarSearchEngine import SimilarSearchEngine

class IndexSimilarSearchEngine(SimilarSearchEngine):
    def __init__(
        self,
        index,
        books: Sequence[Book],
        limit: int,
        reranker: Reranker = None,
        exclude_same_authors: bool = False,
        step_percent: int = 5,
        logger = None,
    ):
        super().__init__(exclude_same_authors, reranker)
        self.index = index
        self.books = BookRegistry(books)
        self._limit = limit
        self.reranker = reranker
        self._step_percent = step_percent
        self.logger = logger
        
    def search(
        self,
        source: Book,
        embedding: np.ndarray,
        progress_callback=None
    ) -> List[Tuple[float, int, int]]:
        seen_books: set[tuple[str, tuple[str, ...]]] = set()
        if self.index is None or self.index.ntotal == 0:
            return []

        step = max(1, self.index.ntotal * self._step_percent // 100)

        query = embedding.reshape(1, -1).astype(np.float32)
        k = min(self._limit * 10 + 200, self.index.ntotal)
        scores, indices = self.index.search(query, k)

        candidates: List[Tuple[float, Book]] = []

        for score_raw, book_id in zip(scores[0], indices[0]):
            if book_id == -1:
                continue

            candidate = self.books.get(book_id)

            if candidate is None:
                continue

            if self._should_skip(
                source=source,
                candidate_name=candidate.file_name,
                candidate_title=candidate.title,
                candidate_authors=candidate.authors,
                seen=seen_books
            ):
                continue

            candidates.append((score_raw, candidate))

            if progress_callback and book_id % step == 0:
                percent = min(99, book_id * 100 // self.index.ntotal)
                progress_callback(percent)

        reranked = self._rerank(candidates=candidates)
        top = reranked[: self._limit]

        if not top:
            return []

        result: List[Tuple[float, int, int]] = []
        for score, candidate in top:
            result.append((float(score), source.id, candidate.id))

        return result

