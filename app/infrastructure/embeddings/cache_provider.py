import numpy as np
from collections import defaultdict
from typing import Tuple, Dict, List
from app.infrastructure.providers import EmbeddingProvider

class CacheEmbeddingProvider(EmbeddingProvider):
    def __init__(
        self,
        data: Dict[int, Tuple[np.ndarray, int]]
    ):
        # embedding_id -> (vector, book_id)
        self._data = data

        # строим индекс
        self._book_index = defaultdict(list)
        for emb_id, (_, book_id) in data.items():
            self._book_index[book_id].append(emb_id)

    def get_by_ids(
        self,
        embedding_ids: List[int],
    ) -> Dict[int, Tuple[np.ndarray, int]]:
        return {
            emb_id: self._data[emb_id]
            for emb_id in embedding_ids
            if emb_id in self._data
        }

    def get_by_book_ids(
        self,
        book_ids: List[int],
    ) -> Dict[int, Tuple[np.ndarray, int]]:
        result = {}

        for book_id in book_ids:
            for emb_id in self._book_index.get(book_id, []):
                result[emb_id] = self._data[emb_id]

        return result