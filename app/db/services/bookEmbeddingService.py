import numpy as np
from app.db import DBRouter
from app.db.repositories import EmbeddingsRepository

class BookEmbeddingService:
    def __init__(self, router: DBRouter):
        self._repo = EmbeddingsRepository(router)

    def get_mean_embeddings(self, book_ids: list[int]) -> dict[int, np.ndarray]:
        rows = self._repo.get_embeddings_by_book_ids(book_ids)

        from collections import defaultdict
        groups = defaultdict(list)

        for r in rows:
            groups[r.book_id].append(r.data)

        return {
            book_id: np.mean(chunks, axis=0)
            for book_id, chunks in groups.items()
        }