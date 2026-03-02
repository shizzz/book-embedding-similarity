import faiss
import numpy as np
from collections import defaultdict
from typing import Optional, Dict
from app.db import DBRouter
from app.ui import BaseUI
from app.utils import EmbeddingsBatchIterable
from app.db.repositories import EmbeddingsRepository, ModelRepository
from app.settings.config import HNSW_M, HNSW_EF_CONSTRUCTION, HNSW_EF_SEARCH, INDEX_FILE, MODEL_NAME, CHUNK_ID_DIVISOR

class BookEmbeddingIndexer:
    def __init__(
        self,
        db_router: DBRouter,
        ui: BaseUI,
        index_file: str = INDEX_FILE,
        batch_size: int = 5000,
        logger: Optional[any] = None
    ):
        self.db_router = db_router
        self.ui = ui
        self.index_file = index_file
        self.batch_size = batch_size
        self.logger = logger
        self._index: Optional[faiss.IndexIDMap] = None
        self.embedding_dim: Optional[int] = None

    def build_index(self):
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
        for batch in self.ui.tqdm(EmbeddingsBatchIterable(repo, self.batch_size), desc="Добавление в HNSW индекс"):
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

        faiss.write_index(index, self.index_file)
        self._index = index
        if self.logger:
            total_vectors = sum(book_chunk_counters.values())
            self.logger.info(f"Индекс построен и сохранён: {total_vectors} векторов (из {len(book_chunk_counters)} книг), dim={self.embedding_dim}")
        return index