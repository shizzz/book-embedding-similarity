from .baseSearchEngine import BaseSearchEngine
from typing import List
from app.db import db, EmbeddingsRepository, FeedbackRepository, BookRepository
from models import Book, Similar, Embedding

class IndexSearchEngine(BaseSearchEngine):
    def __init__(
        self,
        index,
        limit: int,
    ):
        super().__init__(limit)
        self._index = index

    def search(
        self,
        source_book: Book,
        source_embedding: bytes,
        progress_callback=None,
    ) -> List[Similar]:
        embedding = Embedding.from_db(source_embedding)

        # Пример: faiss / annoy / hnsw
        scores, ids = self._index.search(embedding.vec, self._limit)

        scored = list(zip(scores, ids))
        return self._build_result(source_book, scored)

    def _build_result(self, source, scored):
        result = []
        with db() as conn:
            books = BookRepository().get_many(
                conn, [book_id for _, book_id in scored]
            )

        for score, book_id in scored:
            candidate = books.get(book_id)
            if candidate:
                result.append(
                    Similar.from_books(score, source, candidate)
                )

        return result
