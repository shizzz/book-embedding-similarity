import numpy as np
from collections import defaultdict
from typing import Dict, List, Tuple, Optional
from app.infrastructure.db import DBRouter
from app.infrastructure.providers import EmbeddingProvider
from app.infrastructure.models import ChunkType
from .db_provider import DBEmbeddingProvider

class HybridEmbeddingProvider(EmbeddingProvider):

    def __init__(
        self,
        db_router: DBRouter,
        cache_data: Dict[int, Tuple[np.ndarray, int, int]] | None = None,
        cache_meta: Dict[int, Tuple[None, int, int]] | None = None
    ):
        self._db_provider = DBEmbeddingProvider(db_router)

        self._cache_data = cache_data or {}
        self._cache_meta = cache_meta or {}

        # source_id -> type -> [embedding_ids]
        self._book_index = defaultdict(lambda: defaultdict(list))

        for emb_id, (_, source_id, type_) in self._cache_data.items():
            self._book_index[source_id][type_].append(emb_id)

    def get_by_ids(
        self,
        embedding_ids: List[int]
    ) -> Dict[int, Tuple[np.ndarray, int, int]]:

        result = {
            eid: self._cache_data[eid]
            for eid in embedding_ids
            if eid in self._cache_data
        }

        missing = [eid for eid in embedding_ids if eid not in result]

        if missing:
            db_hits = self._db_provider.get_by_ids(missing)
            result.update(db_hits)

        return result

    def get_by_ids_meta(
        self,
        embedding_ids: List[int]
    ) -> Dict[int, Tuple[None, int, int]]:

        result: Dict[int, Tuple[None, int, int]] = {}

        # meta cache
        for eid in embedding_ids:
            if eid in self._cache_meta:
                result[eid] = self._cache_meta[eid]

        missing = [eid for eid in embedding_ids if eid not in result]

        if missing:
            db_hits = self._db_provider.get_by_ids_meta(missing)
            result.update(db_hits)

        return result

    def get_by_source_ids(
        self,
        source_ids: List[int],
        type: Optional[ChunkType] = None
    ) -> Dict[int, Tuple[np.ndarray, int, int]]:

        result = {}
        cached_books = set()

        for source_id in source_ids:
            if source_id not in self._book_index:
                continue

            if type is None:
                emb_ids = (
                    eid
                    for ids in self._book_index[source_id].values()
                    for eid in ids
                )
            else:
                emb_ids = self._book_index[source_id].get(type, [])

            found = False

            for emb_id in emb_ids:
                result[emb_id] = self._cache_data[emb_id]
                found = True

            if found:
                cached_books.add(source_id)

        missing_books = [bid for bid in source_ids if bid not in cached_books]

        if missing_books:
            db_hits = self._db_provider.get_by_source_ids(missing_books, type)
            result.update(db_hits)

        return result