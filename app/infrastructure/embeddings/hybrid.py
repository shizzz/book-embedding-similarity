import numpy as np
from collections import defaultdict
from typing import Dict, List, Tuple
from app.infrastructure.db import DBRouter
from app.infrastructure.providers import EmbeddingProvider
from app.infrastructure.models import ChunkType
from .db_provider import DBEmbeddingProvider

class HybridEmbeddingProvider(EmbeddingProvider):
    def __init__(
        self,
        db_router: DBRouter,
        cache_data: Dict[int, Tuple[np.ndarray, int, int]] | None = None
    ):
        # Провайдер БД
        self._db_provider = DBEmbeddingProvider(db_router)

        # Кэш
        self._cache_data = cache_data or {}
        self._book_index = defaultdict(list)
        for emb_id, (_, book_id) in self._cache_data.items():
            self._book_index[book_id].append(emb_id)

    def get_by_ids(
        self,
        embedding_ids: List[int]
    ) -> Dict[int, Tuple[np.ndarray, int, int]]:
        result = {}

        # 1. Сначала проверяем кэш
        cache_hits = {eid: self._cache_data[eid] for eid in embedding_ids if eid in self._cache_data}
        result.update(cache_hits)

        # 2. Недостающие берем из БД
        missing_ids = [eid for eid in embedding_ids if eid not in result]
        if missing_ids:
            db_hits = self._db_provider.get_by_ids(missing_ids)
            result.update(db_hits)

        return result

    def get_by_book_ids(
        self,
        book_ids: List[int],
        type: ChunkType = None
    ) -> Dict[int, Tuple[np.ndarray, int, int]]:
        result = {}

        # 1. Сначала кэш
        for book_id in book_ids:
            for emb_id in self._book_index.get(book_id, []):
                emb = self._cache_data[emb_id]
                if emb[2] == type or type is None:
                    result[emb_id] = self._cache_data[emb_id]

        # 2. Проверяем, какие книги отсутствуют
        cached_book_ids = {self._cache_data[eid][1] for eid in result}
        missing_books = [bid for bid in book_ids if bid not in cached_book_ids]
        if missing_books:
            db_hits = self._db_provider.get_by_book_ids(missing_books, type)
            result.update(db_hits)

        return result