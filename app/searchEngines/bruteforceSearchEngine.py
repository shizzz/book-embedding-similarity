import numpy as np
from typing import List
from app.models import Book, Similar, Embedding
from app.db import db, BookRepository, FeedbackRepository, EmbeddingsRepository
from .baseSearchEngine import BaseSearchEngine

class BruteforceSearchEngine(BaseSearchEngine):
    def __init__(
        self,
        limit: int,
        exclude_same_authors: bool = False,
        step_percent: int = 5,
    ):
        super().__init__(limit, exclude_same_authors)
        self._step_percent = step_percent

    def search(
        self,
        source: Book,
        embedding: Embedding,
        progress_callback=None
    ) -> List[Similar]:
        with db() as conn:
            candidates = []
            current = 0
            total = BookRepository.count_embeddings(conn)
            step = max(1, total * self._step_percent // 100)

            feedbacks = FeedbackRepository().get(conn, source.id)

            for row in BookRepository().get_all_with_embeddings(conn):
                current += 1

                book_id, archive, book, title, embedding_bytes = row
                candidate = Book(
                    id=book_id,
                    archive_name=archive,
                    file_name=book,
                    title=title
                )

                if self._should_skip(source, candidate):
                    continue

                try:
                    emb_norm = Embedding.from_db(embedding_bytes)
                    similarity = np.dot(emb_norm.vec, embedding.vec)

                    boost = feedbacks.get_boost(source.id, candidate.id)
                    score = similarity + boost

                    candidates.append((score, candidate))

                except Exception:
                    continue

                if progress_callback and current % step == 0:
                    percent = min(99, current * 100 // total)
                    progress_callback(percent)

        candidates.sort(key=lambda x: x[0], reverse=True)
        top = candidates[:self.limit]

        if not top:
            return []

        result: List[Similar] = []
        for score, candidate in top:
            result.append(Similar.from_books(score, source, candidate))

        return result