import faiss
import numpy as np
from typing import Optional, Dict
from app.infrastructure.db import DBRouter
from app.ui import BaseUI
from app.utils import EmbeddingsBatchIterable
from app.infrastructure.db.repositories import EmbeddingsRepository, ModelRepository
from app.settings import PathsConfig, ProcessConfig, IndexConfig, IndexLevel

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
        self.batch_size = batch_size
        self.logger = logger
        self._index: Optional[faiss.IndexIDMap] = None
        self.embedding_dim: Optional[int] = None

    def build_index(self):
        if IndexConfig.BUILD_INDEX_LEVEL in (IndexLevel.DOCUMENT, IndexLevel.BOTH):
            self._build_index_document()
        if IndexConfig.BUILD_INDEX_LEVEL in (IndexLevel.CHUNK, IndexLevel.BOTH):
            self._build_index_chunk()

    def _build_index_document(self):
        model_uid = ModelRepository(self.db_router).get_latest_uid(ProcessConfig.MODEL_NAME)
        repo = EmbeddingsRepository(self.db_router, model_uid)

        book_sums: Dict[int, np.ndarray] = {}
        book_counts: Dict[int, int] = {}

        # --- accumulate embeddings ---
        for batch in self.ui.tqdm(
            EmbeddingsBatchIterable(repo, self.batch_size, ["book_id", "chunk_id"]),
            desc="Слияние embeddings (DOCUMENT)"
        ):
            for r in batch:
                vec = r.data.astype(np.float32)
                if self.embedding_dim is None:
                    self.embedding_dim = vec.shape[0]
                elif vec.shape[0] != self.embedding_dim:
                    raise ValueError(f"Chunk {r.chunk_id} имеет dim={vec.shape[0]}, ожидается {self.embedding_dim}")

                if r.book_id not in book_sums:
                    book_sums[r.book_id] = vec.copy()
                    book_counts[r.book_id] = 1
                else:
                    book_sums[r.book_id] += vec
                    book_counts[r.book_id] += 1

        if not book_sums:
            raise ValueError("Нет embeddings для построения DOCUMENT индекса")

        base_index = faiss.IndexHNSWFlat(
            self.embedding_dim,
            IndexConfig.HNSW_M,
            faiss.METRIC_INNER_PRODUCT
        )
        base_index.hnsw.efConstruction = IndexConfig.HNSW_EF_CONSTRUCTION
        base_index.hnsw.efSearch = IndexConfig.HNSW_EF_SEARCH
        index = faiss.IndexIDMap(base_index)

        book_ids = []
        merged_vectors = []

        for book_id, vec_sum in self.ui.tqdm(book_sums.items(),desc="Добавление в HNSW индекс (DOCUMENT)"):
            merged_vec = vec_sum / book_counts[book_id]
            norm = np.linalg.norm(merged_vec)
            if norm > 0:
                merged_vec /= norm
            book_ids.append(book_id)
            merged_vectors.append(merged_vec)

        vectors_np = np.stack(merged_vectors)
        ids_np = np.array(book_ids, dtype=np.int64)
        index.add_with_ids(vectors_np, ids_np)

        faiss.write_index(index, str(PathsConfig.DATA_DIR / f"{ProcessConfig.MODEL_NAME}.{IndexLevel.DOCUMENT.value}.faiss"))
        self._index = index
        if self.logger:
            self.logger.info(f"DOCUMENT индекс построен: {len(book_ids)} книг, dim={self.embedding_dim}")

        return index

    def _build_index_chunk(self):
        model_uid = ModelRepository(self.db_router).get_latest_uid(ProcessConfig.MODEL_NAME)
        repo = EmbeddingsRepository(self.db_router, model_uid)

        # --- первый батч для определения embedding_dim ---
        first_batch = next(repo.get_all_batch(self.batch_size))
        self.embedding_dim = first_batch[0].shape

        base_index = faiss.IndexHNSWFlat(
            self.embedding_dim,
            IndexConfig.HNSW_M,
            faiss.METRIC_INNER_PRODUCT
        )
        base_index.hnsw.efConstruction = IndexConfig.HNSW_EF_CONSTRUCTION
        base_index.hnsw.efSearch = IndexConfig.HNSW_EF_SEARCH
        index = faiss.IndexIDMap(base_index)

        # --- добавляем все embeddings ---
        for batch in self.ui.tqdm(EmbeddingsBatchIterable(repo, self.batch_size, ["book_id", "chunk_id"]), desc="Добавление в HNSW индекс CHUNK"):
            batch_vectors = []
            batch_ids = []

            for r in batch:
                vec = r.data.astype(np.float32)
                if vec.shape[0] != self.embedding_dim:
                    raise ValueError(f"Chunk {r.chunk_id} имеет неправильную размерность {vec.shape[0]}, ожидается {self.embedding_dim}")

                # index_id = embeddings.id
                batch_vectors.append(vec)
                batch_ids.append(r.emb_id)

            vectors_np = np.stack(batch_vectors)
            ids_np = np.array(batch_ids, dtype=np.int64)
            index.add_with_ids(vectors_np, ids_np)

        faiss.write_index(index, str(PathsConfig.DATA_DIR / f"{ProcessConfig.MODEL_NAME}.{IndexLevel.CHUNK.value}.faiss"))
        self._index = index
        if self.logger:
            self.logger.info(f"CHUNK индекс построен и сохранён: {len(batch_ids)} векторов, dim={self.embedding_dim}")

        return index