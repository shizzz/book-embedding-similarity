import numpy as np
from typing import List, Tuple
from app.models import Book, Embedding
from app.hnsw.rerankers import Reranker
from app.db import db, BookRepository
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
        source: Book,
        embedding: Embedding,
        progress_callback=None
    ) -> List[Tuple[float, int, int]]:
        with db() as conn:
            candidates = []
            seen_books: set[tuple[str, tuple[str, ...]]] = set()
            current = 0
            total = BookRepository.count_embeddings(conn)
            step = max(1, total * self._step_percent // 100)

            for row in BookRepository().get_all_with_embeddings(conn):
                current += 1

                book_id, _, book, title, embedding_bytes = row

                if self._should_skip(
                    source=source,
                    candidate_name=book,
                    candidate_title=title,
                    seen=seen_books
                ):
                    continue

                try:
                    emb_norm = Embedding.from_db(embedding_bytes)
                    score = np.dot(emb_norm.vec, embedding.vec)

                    candidates.append((score, book_id))

                except Exception:
                    continue

                if progress_callback and current % step == 0:
                    percent = min(99, current * 100 // total)
                    progress_callback(percent)


        reranked = self._rerank(
            source=source,
            candidates=candidates,
        )
        top = reranked[: self._limit]

        if not top:
            return []

        result: List[Tuple[float, int, int]] = []
        for score, candidate in top:
            result.append((score, source.id, candidate))

        return result