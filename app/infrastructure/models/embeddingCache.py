from collections import defaultdict
from typing import Dict, List, Tuple
import numpy as np


class EmbeddingCache:
    """
    Хранит:
        embedding_id -> (vector, book_id)

    И строит индекс:
        book_id -> list[embedding_id]
    """
    def __init__(self, data: Dict[int, Tuple[np.ndarray, int]]):
        self._data = data
        self._book_index: Dict[int, List[int]] = defaultdict(list)

        for emb_id, (_, book_id) in data.items():
            self._book_index[book_id].append(emb_id)

    # --------- поиск по embedding_id ---------
    def get_by_ids(
        self,
        embedding_ids: List[int],
    ) -> Dict[int, Tuple[np.ndarray, int]]:
        return {
            emb_id: self._data[emb_id]
            for emb_id in embedding_ids
            if emb_id in self._data
        }

    # --------- поиск по book_id ---------
    def get_by_book_ids(
        self,
        book_ids: List[int],
    ) -> Dict[int, Tuple[np.ndarray, int]]:
        result = {}

        for book_id in book_ids:
            for emb_id in self._book_index.get(book_id, []):
                result[emb_id] = self._data[emb_id]

        return result