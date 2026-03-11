import numpy as np
from app.infrastructure.providers import EmbeddingProvider
from app.infrastructure.models import ChunkType

class BookEmbeddingService:
    def __init__(
            self, 
            provider: EmbeddingProvider
        ):
        self._repo = provider

    def get_mean_embeddings(
            self, 
            book_ids: list[int]
        ) -> dict[int, dict[ChunkType, np.ndarray]]:
        rows = self._repo.get_by_book_ids(book_ids)

        from collections import defaultdict
        groups: dict[int, dict[ChunkType, list[np.ndarray]]] = defaultdict(lambda: defaultdict(list))

        for _, (vec, book_id, type) in rows.items():
            try:
                chunk_type = ChunkType(type)
            except Exception:
                continue
            if chunk_type in (ChunkType.TEXT, ChunkType.TITLE, ChunkType.DESCRIPTION):
                groups[book_id][chunk_type].append(vec)

        result: dict[int, dict[ChunkType, np.ndarray]] = {}
        for book_id, by_type in groups.items():
            means: dict[ChunkType, np.ndarray] = {}
            for chunk_type, chunks in by_type.items():
                if chunks:
                    means[chunk_type] = np.mean(chunks, axis=0)
            if means:
                result[book_id] = means

        return result