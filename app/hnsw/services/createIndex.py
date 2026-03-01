from tqdm import tqdm
import faiss
import numpy as np
from typing import Optional, Dict, List, Tuple
from app.db import DBRouter
from app.ui import BaseUI
from app.db.repositories import EmbeddingsRepository, ModelRepository
from app.settings.config import HNSW_M, HNSW_EF_CONSTRUCTION, HNSW_EF_SEARCH, INDEX_FILE, MODEL_NAME

class BookEmbeddingIndexer:
    def __init__(
        self,
        db_router: DBRouter,
        ui: BaseUI,
        index_file: str = INDEX_FILE,
        batch_size: int = 2048,
        logger: Optional[any] = None
    ):
        self.db_router = db_router
        self.ui = ui
        self.index_file = index_file
        self.batch_size = batch_size
        self.logger = logger
        self._index: Optional[faiss.IndexIDMap] = None
        self.embedding_dim: Optional[int] = None
        self.chunk_to_book: Dict[int, int] = {}
        self.id_to_pos: Dict[int, int] = {}

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

        pos_counter = 0

        # прогресс бар по батчам
        for batch in self.ui.tqdm(repo.get_all_batch(self.batch_size), desc="Добавление в HNSW индекс"):
            batch_embeddings = []
            batch_ids = []

            for r in batch:
                vec = r.data
                if vec.shape[0] != self.embedding_dim:
                    raise ValueError(f"Chunk {r.chunk_id} имеет неправильную размерность {vec.shape[0]}, ожидается {self.embedding_dim}")
                batch_embeddings.append(vec)
                batch_ids.append(r.chunk_id)

                self.chunk_to_book[r.chunk_id] = r.book_id
                self.id_to_pos[r.chunk_id] = pos_counter
                pos_counter += 1

            batch_embeddings_np = np.stack(batch_embeddings)
            batch_ids_np = np.array(batch_ids, dtype=np.int64)
            index.add_with_ids(batch_embeddings_np, batch_ids_np)

        faiss.write_index(index, self.index_file)
        self._index = index
        if self.logger:
            self.logger.info(f"Индекс построен и сохранён: {len(self.chunk_to_book)} векторов, dim={self.embedding_dim}")
        return index

    def get_embeddings_by_book_id(self, book_id: int, repo: Optional[EmbeddingsRepository] = None) -> np.ndarray:
        """
        Возвращает все embeddings по book_id.
        Если не было построено, можно получить напрямую из базы через repo
        """
        if self._index is None:
            # fallback через репозиторий
            if repo is None:
                model_uid = ModelRepository(self.db_router).get_latest_uid(MODEL_NAME)
                repo = EmbeddingsRepository(self.db_router, model_uid)
            rows = [r for r in repo.get_all() if r.book_id == book_id]
            if not rows:
                return np.array([])
            return np.stack([r.data for r in rows])

        relevant_chunk_ids = [cid for cid, b_id in self.chunk_to_book.items() if b_id == book_id]
        if not relevant_chunk_ids:
            return np.array([])

        positions = [self.id_to_pos[cid] for cid in relevant_chunk_ids]
        embeddings = []
        for pos in positions:
            # FAISS хранит их в порядке добавления
            # Чтобы избежать полного хранения в RAM, можно сделать ленивый доступ, но тут просто возвращаем массив
            embeddings.append(self._index.reconstruct(self._index.id_map.at(pos)))
        return np.stack(embeddings)

    def search_similar_chunks(self, query_embeddings: np.ndarray, top_k: int = 5) -> List[List[Tuple[int, float]]]:
        if self._index is None:
            raise ValueError("Индекс ещё не построен")
        distances, indices = self._index.search(query_embeddings, top_k)
        return [list(zip(inds.tolist(), dists.tolist())) for inds, dists in zip(indices, distances)]