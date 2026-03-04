import numpy as np
from app.infrastructure.providers import EmbeddingProvider

class BookEmbeddingService:
    def __init__(
            self, 
            provider: EmbeddingProvider
        ):
        self._repo = provider

    def get_mean_embeddings(
            self, 
            book_ids: list[int]
        ) -> dict[int, np.ndarray]:
        rows = self._repo.get_by_book_ids(book_ids)

        from collections import defaultdict
        groups = defaultdict(list)

        for _, (vec, book_id) in rows.items():
            groups[book_id].append(vec)

        return {
            book_id: np.mean(chunks, axis=0)
            for book_id, chunks in groups.items()
        }