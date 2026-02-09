from .baseSearchEngine import BaseSearchEngine
from typing import List
from app.db import db, EmbeddingsRepository, FeedbackRepository, BookRepository
from models import Book, Similar, Embedding

class BruteforceSearchEngine(BaseSearchEngine):
    def search(
        self,
        source_book: Book,
        source_embedding: bytes,
        progress_callback=None,
    ) -> List[Similar]:
        source_emb = Embedding.from_db(source_embedding)

        with db() as conn:
            total = EmbeddingsRepository().count(conn)
            step = max(1, total * self._step_percent // 100)

            feedbacks = FeedbackRepository().get(conn, source_book.id)
            candidates = []
            current = 0

            for book_id, embedding_bytes in EmbeddingsRepository().get_all(conn):
                current += 1

                try:
                    emb_norm = Embedding.from_db(embedding_bytes)
                    similarity = float(np.dot(emb_norm.vec, source_emb.vec))

                    boost = feedbacks.get_boost(source_book.id, book_id)
                    score = similarity + boost

                    candidates.append((score, book_id))
                except Exception:
                    continue

                if progress_callback and current % step == 0:
                    percent = min(99, current * 100 // total)
                    progress_callback(percent)

        candidates.sort(key=lambda x: x[0], reverse=True)
        top = candidates[:self._limit]

        return self._build_result(source_book, top)

    def _build_result(
        self,
        source: Book,
        scored_ids: List[tuple[float, int]],
    ) -> List[Similar]:
        result = []
        ids = [book_id for _, book_id in scored_ids]

        with db() as conn:
            books = BookRepository().get_many(conn, ids)

        for score, book_id in scored_ids:
            candidate = books.get(book_id)
            if candidate:
                result.append(
                    Similar.from_books(score, source, candidate)
                )

        return result
