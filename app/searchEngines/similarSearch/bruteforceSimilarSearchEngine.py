import numpy as np
from typing import List, Tuple
from app.models import BookRegistry, Book
from app.hnsw.rerankers import Reranker
from app.db import DBRouter
from app.db.repositories import BookRepository
from .similarSearchEngine import SimilarSearchEngine

class BruteforceSimilarSearchEngine(SimilarSearchEngine):
    def __init__(
        self,
        limit: int,
        exclude_same_authors: bool = False,
        step_percent: int = 5,
        reranker: Reranker = None,
    ):
        super().__init__(exclude_same_authors, reranker)
        self._limit = limit
        self._step_percent = step_percent

    def search(
        self,
        sources: BookRegistry,
        progress_callback=None
    ) -> List[Tuple[float, int, int]]:
        result: List[Tuple[float, int, int]] = []
        with DB() as conn:
            for source in sources:
                candidates = []
                seen_books: set[tuple[str, tuple[str, ...]]] = set()
                current = 0
                total = BookRepository.count_embeddings(conn)
                step = max(1, total * self._step_percent // 100)

                for row in BookRepository.get_all_with_embeddings(conn):
                    current += 1

                    source = Book.from_row(row)

                    if self._should_skip(
                        source=source,
                        candidate_name=source.file_name,
                        candidate_title=source.title,
                        seen=seen_books,
                        candidate_authors=source.authors
                    ):
                        continue

                    try:
                        score = np.dot(source.embedding, source.embedding)
                        candidates.append((score, source.id))
                    except Exception:
                        continue

                    if progress_callback and current % step == 0:
                        percent = min(99, current * 100 // total)
                        progress_callback(percent)


            reranked = self._rerank(candidates=candidates,)
            top = reranked[: self._limit]

            if not top:
                return []

            for score, candidate in top:
                result.append((score, source.id, candidate))

        return result