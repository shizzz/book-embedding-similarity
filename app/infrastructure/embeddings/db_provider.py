import numpy as np
from typing import Tuple, Dict, List
from app.infrastructure.db import DBRouter
from app.infrastructure.db.repositories import EmbeddingsRepository
from app.infrastructure.providers import EmbeddingProvider

class DBEmbeddingProvider(EmbeddingProvider):
    def __init__(
            self, 
            router: DBRouter
        ):
        self._repo = EmbeddingsRepository(router)

    def get_by_ids(
        self,
        book_ids: List[int],
    ) -> Dict[int, Tuple[np.ndarray, int]]:
        return self._repo.get_by_ids(book_ids)

    def get_by_book_ids(
        self,
        embedding_ids: List[int],
    ) -> Dict[int, Tuple[np.ndarray, int]]:
        return self._repo.get_by_book_ids(embedding_ids)