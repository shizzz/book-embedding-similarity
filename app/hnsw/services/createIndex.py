import faiss
import numpy as np
from typing import Optional, List, Tuple, Dict
from app.db import DBRouter
from app.db.repositories import EmbeddingsRepository, ModelRepository
from app.settings.config import HNSW_M, HNSW_EF_CONSTRUCTION, HNSW_EF_SEARCH, INDEX_FILE, MODEL_NAME

class BookEmbeddingIndexer:
    """
    Сервис построения и поиска HNSW индекса.
    Сама достает все embeddings через DBRouter.
    """

    def __init__(
        self,
        db_router: DBRouter,
        index_file: str = INDEX_FILE,
        batch_size: int = 2048,
        logger: Optional[any] = None
    ):
        self.db_router = db_router
        self.index_file = index_file
        self.batch_size = batch_size
        self.logger = logger
        self._index = None
        self.embeddings = None
        self.ids = None
        self.embedding_dim = None
        self.chunk_to_book: Dict[int, int]

    # ------------------------
    # Основной метод: строим индекс
    # ------------------------
    def build_index(self):
        """Загружает embeddings из базы и строит индекс"""
        self.embeddings, self.ids = self._load_embeddings()
        return self._generate_and_save()

    # ------------------------
    # Загрузка всех эмбеддингов через репозитории
    # ------------------------
    def _load_embeddings(self) -> Tuple[np.ndarray, np.ndarray]:
        model_uid = ModelRepository(self.db_router).get_latest_uid(MODEL_NAME)
        rows = EmbeddingsRepository(self.db_router, model_uid).get_all()
        if not rows:
            raise ValueError("Нет эмбеддингов для построения индекса")

        embeddings = []
        ids = []

        for r in rows:
            vec = r.data
            if self.embedding_dim is None:
                self.embedding_dim = r.shape  # берем shape из первого эмбеддинга
            elif vec.shape[0] != self.embedding_dim:
                raise ValueError(f"Chunk {r.chunk_id} имеет неправильную размерность {vec.shape[0]}, ожидается {self.embedding_dim}")

            embeddings.append(vec)
            ids.append(r.chunk_id)

        embeddings = np.stack(embeddings)
        ids = np.array(ids, dtype=np.int64)

        if self.logger:
            self.logger.info(f"Загружено {len(ids)} embeddings из репозитория, dim={self.embedding_dim}")

        return embeddings, ids

    # ------------------------
    # Генерация и сохранение HNSW
    # ------------------------
    def _generate_and_save(self):
        if self.embeddings.shape[0] == 0:
            raise ValueError("Попытка построить индекс с пустым списком векторов")

        # создаём HNSW индекс
        base_index = faiss.IndexHNSWFlat(self.embedding_dim, HNSW_M, faiss.METRIC_INNER_PRODUCT)
        base_index.hnsw.efConstruction = HNSW_EF_CONSTRUCTION
        base_index.hnsw.efSearch = HNSW_EF_SEARCH

        index = faiss.IndexIDMap(base_index)

        n_total = self.embeddings.shape[0]
        if self.logger:
            self.logger.info(f"Генерация HNSW: {n_total:,} векторов, dim={self.embedding_dim}")

        # словарь для chunk_id → book_id
        self.chunk_to_book = {chunk.chunk_id: chunk.book_id for chunk in self._all_chunks_objects}

        # добавление батчами
        for i in range(0, n_total, self.batch_size):
            end = min(i + self.batch_size, n_total)
            batch = self.embeddings[i:end]
            batch_ids = self.ids[i:end]
            index.add_with_ids(batch, batch_ids)
            if self.logger:
                self.logger.info(f"Добавлено {end}/{n_total} векторов в индекс")

        # сохраняем индекс на диск
        faiss.write_index(index, self.index_file)
        if self.logger:
            self.logger.info(f"Индекс сохранён в '{self.index_file}'")

        self._index = index
        return index

    # ------------------------
    # Поиск похожих
    # ------------------------
    def search_similar_chunks(self, query_embeddings: np.ndarray, top_k: int = 5) -> List[List[Tuple[int, float]]]:
        if self._index is None:
            raise ValueError("Индекс ещё не построен")

        distances, indices = self._index.search(query_embeddings, top_k)
        results = []
        for inds, dists in zip(indices, distances):
            results.append(list(zip(inds.tolist(), dists.tolist())))
        return results
    
    def get_embeddings_by_book_id(self, book_id: int) -> np.ndarray:
        """
        Возвращает все embeddings из индекса, принадлежащие книге book_id
        """
        if self._index is None:
            raise ValueError("Индекс ещё не построен")

        # собираем chunk_id, которые относятся к книге
        relevant_chunk_ids = [chunk_id for chunk_id, b_id in self.chunk_to_book.items() if b_id == book_id]
        
        if not relevant_chunk_ids:
            return np.array([])

        # получаем их позиции в индексе
        positions = [self.ids.tolist().index(cid) for cid in relevant_chunk_ids]
        
        # возвращаем массив embeddings
        return self.embeddings[positions]