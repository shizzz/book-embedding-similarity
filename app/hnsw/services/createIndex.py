import faiss
import numpy as np
from collections import defaultdict
from typing import Optional, Dict
from app.db import DBRouter
from app.ui import BaseUI
from app.utils import EmbeddingsBatchIterable
from app.db.repositories import EmbeddingsRepository, ModelRepository
from app.settings.config import (
    HNSW_M, 
    HNSW_EF_CONSTRUCTION, 
    HNSW_EF_SEARCH, 
    MODEL_NAME, 
    CHUNK_ID_DIVISOR,
    BUILD_INDEX_LEVEL,
    IndexLevel,
    DATA_DIR
)

class BookEmbeddingIndexer:
    def __init__(
        self,
        db_router: DBRouter,
        ui: BaseUI,
        batch_size: int = 5000,
        logger: Optional[any] = None
    ):
        self.db_router = db_router
        self.ui = ui
        self.index_file = str(DATA_DIR / "")
        self.batch_size = batch_size
        self.logger = logger
        self._index: Optional[faiss.IndexIDMap] = None
        self.embedding_dim: Optional[int] = None
        
    def build_index(self):
        if BUILD_INDEX_LEVEL in (IndexLevel.CHUNK, IndexLevel.BOTH):
            self._build_index_chunk()

        if BUILD_INDEX_LEVEL in (IndexLevel.DOCUMENT, IndexLevel.BOTH):
            self._build_index_document()

    def _build_index_document(self):
        model_uid = ModelRepository(self.db_router).get_latest_uid(MODEL_NAME)
        repo = EmbeddingsRepository(self.db_router, model_uid)

        book_sums: Dict[int, np.ndarray] = {}
        book_counts: Dict[int, int] = {}

        # ---- 1. streaming accumulation ----
        for batch in self.ui.tqdm(
            EmbeddingsBatchIterable(repo, self.batch_size),
            desc="Слияние embeddings (streaming)"
        ):
            for r in batch:
                vec = r.data.astype(np.float32)

                if self.embedding_dim is None:
                    self.embedding_dim = vec.shape[0]

                elif vec.shape[0] != self.embedding_dim:
                    raise ValueError(
                        f"Chunk {r.chunk_id} имеет dim={vec.shape[0]}, ожидается {self.embedding_dim}"
                    )

                if r.book_id not in book_sums:
                    book_sums[r.book_id] = vec.copy()
                    book_counts[r.book_id] = 1
                else:
                    book_sums[r.book_id] += vec
                    book_counts[r.book_id] += 1

        if not book_sums:
            raise ValueError("Нет embeddings для построения merge индекса")

        # ---- 2. создаём FAISS индекс ----
        base_index = faiss.IndexHNSWFlat(
            self.embedding_dim,
            HNSW_M,
            faiss.METRIC_INNER_PRODUCT
        )

        base_index.hnsw.efConstruction = HNSW_EF_CONSTRUCTION
        base_index.hnsw.efSearch = HNSW_EF_SEARCH

        index = faiss.IndexIDMap(base_index)

        # ---- 3. вычисляем mean и добавляем ----
        book_ids = []
        merged_vectors = []

        for book_id in self.ui.tqdm(book_sums.keys(), desc="Добавление в HNSW индекс DOCUMENT"):

            merged_vec = book_sums[book_id] / book_counts[book_id]

            # normalize (ВАЖНО для cosine/IP)
            norm = np.linalg.norm(merged_vec)
            if norm > 0:
                merged_vec /= norm

            book_ids.append(book_id)
            merged_vectors.append(merged_vec)

        vectors_np = np.stack(merged_vectors)
        ids_np = np.array(book_ids, dtype=np.int64)

        index.add_with_ids(vectors_np, ids_np)

        # ---- 4. сохраняем ----
        faiss.write_index(index, str(DATA_DIR / f"{MODEL_NAME}.{IndexLevel.DOCUMENT.value}.faiss"))

        self._index = index

        if self.logger:
            self.logger.info(
                f"Merge индекс построен: {len(book_ids)} книг, dim={self.embedding_dim}"
            )

        return index
    
    def _build_index_chunk(self):
        model_uid = ModelRepository(self.db_router).get_latest_uid(MODEL_NAME)
        repo = EmbeddingsRepository(self.db_router, model_uid)

        # создаём индекс после первого батча
        first_batch = next(repo.get_all_batch(self.batch_size))
        self.embedding_dim = first_batch[0].shape
        base_index = faiss.IndexHNSWFlat(self.embedding_dim, HNSW_M, faiss.METRIC_INNER_PRODUCT)
        base_index.hnsw.efConstruction = HNSW_EF_CONSTRUCTION
        base_index.hnsw.efSearch = HNSW_EF_SEARCH
        index = faiss.IndexIDMap(base_index)

        book_chunk_counters: Dict[int, int] = defaultdict(int)  # seq для каждой книги

        # прогресс бар по батчам
        for batch in self.ui.tqdm(EmbeddingsBatchIterable(repo, self.batch_size), desc="Добавление в HNSW индекс CHUNK"):
            batch_embeddings = []
            batch_ids = []

            for r in batch:
                vec = r.data
                if vec.shape[0] != self.embedding_dim:
                    raise ValueError(f"Chunk {r.chunk_id} имеет неправильную размерность {vec.shape[0]}, ожидается {self.embedding_dim}")
                
                # Создаём composite ID: book_id * DIV + seq
                seq = book_chunk_counters[r.book_id]
                index_chunk_id = r.book_id * CHUNK_ID_DIVISOR + seq
                batch_embeddings.append(vec)
                batch_ids.append(index_chunk_id)
                
                book_chunk_counters[r.book_id] += 1

            batch_embeddings_np = np.stack(batch_embeddings)
            batch_ids_np = np.array(batch_ids, dtype=np.int64)
            index.add_with_ids(batch_embeddings_np, batch_ids_np)

        faiss.write_index(index, str(DATA_DIR / f"{MODEL_NAME}.{IndexLevel.CHUNK.value}.faiss"))
        self._index = index
        if self.logger:
            total_vectors = sum(book_chunk_counters.values())
            self.logger.info(f"Индекс построен и сохранён: {total_vectors} векторов (из {len(book_chunk_counters)} книг), dim={self.embedding_dim}")
        return index