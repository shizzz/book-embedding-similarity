import numpy as np
from typing import Tuple, Dict, List
from app.infrastructure.db import DBRouter
from app.infrastructure.db.repositories import EmbeddingsRepository
from app.infrastructure.providers import EmbeddingProvider
from app.infrastructure.models import ChunkType

class DBEmbeddingProvider(EmbeddingProvider):
    SQLITE_MAX_VARS = 1000

    def __init__(self, router: DBRouter):
        self._repo = EmbeddingsRepository(router)

    def _chunks(self, items: List[int], size: int):
        for i in range(0, len(items), size):
            yield items[i:i + size]

    def get_by_ids(
        self,
        embedding_ids: List[int]
    ) -> Dict[int, Tuple[np.ndarray, int, int]]:
        result = {}

        for chunk in self._chunks(embedding_ids, self.SQLITE_MAX_VARS):
            result.update(self._repo.get_by_ids(chunk))

        return result

    def get_by_book_ids(
        self,
        book_ids: List[int],
        type: ChunkType = None
    ) -> Dict[int, Tuple[np.ndarray, int, int]]:
        result = {}

        for chunk in self._chunks(book_ids, self.SQLITE_MAX_VARS):
            result.update(self._repo.get_by_book_ids(chunk, type))

        return result