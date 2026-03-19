import numpy as np
from collections import defaultdict
from typing import Tuple, Dict, List
from app.infrastructure.providers import EmbeddingProvider

class CacheEmbeddingProvider(EmbeddingProvider):
    def __init__(
        self,
        data: Dict[int, Tuple[np.ndarray, int, int]]
    ):
        # embedding_id -> (vector, source_id)
        self._data = data

        # строим индекс
        self._book_index = defaultdict(list)
        for emb_id, (_, source_id) in data.items():
            self._book_index[source_id].append(emb_id)

    def get_by_ids(
        self,
        embedding_ids: List[int],
    ) -> Dict[int, Tuple[np.ndarray, int, int]]:
        return {
            emb_id: self._data[emb_id]
            for emb_id in embedding_ids
            if emb_id in self._data
        }

    def get_by_source_ids(
        self,
        source_ids: List[int],
    ) -> Dict[int, Tuple[np.ndarray, int, int]]:
        result = {}

        for source_id in source_ids:
            for emb_id in self._book_index.get(source_id, []):
                result[emb_id] = self._data[emb_id]

        return result