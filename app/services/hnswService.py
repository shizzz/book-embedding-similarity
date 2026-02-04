import os
import faiss
import time
import numpy as np
from tqdm import tqdm
from app.settings.config import BASE_DIR, HNSW_M, HNSW_EF_CONSTRUCTION, HNSW_EF_SEARCH

class HNSWService:
    def __init__(
        self,
        embeddings: list | np.ndarray,  # Вектора для генерации (если файла нет)
        embedding_dim: int,
        index_file: str = f"{BASE_DIR}/data/hnsw_index.index",  # Путь к файлу
        batch_size: int = None,        # Для батчевого добавления
        logger = None
    ):
        self.embeddings = np.ascontiguousarray(embeddings).astype(np.float32)
        self.embedding_dim = embedding_dim
        self.index_file = index_file
        self._index = None
        self.logger = logger
        if not batch_size:
            self.batch_size = len(embeddings) // 100
        else:
            self.batch_size = batch_size

    def __estimate_hnsw_memory_gb(self, ntotal: int, dim: int, overhead_factor: float = 1.12) -> float:
        bytes_per_vector = (dim * 4) + (HNSW_M * 8)
        total_bytes = ntotal * bytes_per_vector
        total_bytes_with_overhead = total_bytes * overhead_factor
        gb = total_bytes_with_overhead / (1024 ** 3)
        return gb

    def get_index(self) -> faiss.IndexHNSWFlat:
        if self._index is not None:
            return self._index  # Если уже загружен в память — отдаём сразу

        if os.path.exists(self.index_file):
            if self.logger: self.logger.info(f"Файл '{self.index_file}' найден. Загружаем...")
            self._index = self._load_from_file()
        else:
            if self.logger: self.logger.info(f"Файл '{self.index_file}' не найден. Генерируем и сохраняем...")
            self._index = self._generate_and_save()

        return self._index

    def _generate_and_save(self) -> faiss.IndexHNSWFlat:
        start_build = time.perf_counter()
        if self.embeddings.shape[1] != self.embedding_dim:
            raise ValueError(f"Размерность embeddings ({self.embeddings.shape[1]}) не совпадает с embedding_dim ({self.embedding_dim})")

        index = faiss.IndexHNSWFlat(self.embedding_dim, HNSW_M, faiss.METRIC_INNER_PRODUCT)
        index.hnsw.efConstruction = HNSW_EF_CONSTRUCTION
        index.hnsw.efSearch = HNSW_EF_SEARCH

        n_total = self.embeddings.shape[0]
        if self.logger: self.logger.info(f"Генерация HNSW: {n_total:,} векторов, dim={self.embedding_dim}, M={HNSW_M}, efConstruction={HNSW_EF_CONSTRUCTION}")

        with tqdm(total=n_total, desc="Добавление векторов в HNSW", unit="vec", unit_scale=True) as pbar:
            for i in range(0, n_total, self.batch_size):
                end = min(i + self.batch_size, n_total)
                batch = self.embeddings[i:end]
                index.add(batch)
                pbar.update(end - i)
        
        mem_gb = self.__estimate_hnsw_memory_gb(
            ntotal=index.ntotal,
            dim=self.embedding_dim,
            overhead_factor=1.10          # консервативно 10%, можно 1.15
        )
        build_time = time.perf_counter() - start_build

        if self.logger: self.logger.info(
            "HNSW индекс построен:\n"
            f"  • время построения          : {build_time:.2f} сек\n"
            f"  • количество векторов       : {index.ntotal:,}\n"
            f"  • размерность               : {index.d}\n"
            f"  • M (связи на узел)         : {HNSW_M}\n"
            f"  • efConstruction            : {HNSW_EF_CONSTRUCTION}\n"
            f"  • efSearch (по умолчанию)   : {HNSW_EF_SEARCH}\n"
            f"  • память                    : ~ {mem_gb:.1f}–{mem_gb*1.15:.1f} GB"
        )
            
        # Сохранение на диск
        faiss.write_index(index, self.index_file)
        if self.logger: self.logger.info(f"Индекс сохранён в '{self.index_file}' (размер: {os.path.getsize(self.index_file) / (1024**2):.2f} MB)")

        return index

    def _load_from_file(self) -> faiss.IndexHNSWFlat:
        if not os.path.exists(self.index_file):
            raise FileNotFoundError(f"Файл '{self.index_file}' не существует")

        index = faiss.read_index(self.index_file)
        if not isinstance(index, faiss.IndexHNSWFlat):
            raise TypeError("Загруженный индекс не является HNSWFlat")
        if index.d != self.embedding_dim:
            raise ValueError(f"Размерность загруженного индекса ({index.d}) не совпадает с embedding_dim ({self.embedding_dim})")

        index.hnsw.efSearch = HNSW_EF_SEARCH

        if self.logger: self.logger.info(f"Индекс загружен из '{self.index_file}' (ntotal: {index.ntotal:,})")
        return index

    def delete_index_file(self, force: bool = False) -> bool:
        if not os.path.exists(self.index_file):
            if not force:
                if self.logger: self.logger.info("Файл '{self.index_file}' не существует — ничего не удаляем.")
                return False
        else:
            os.remove(self.index_file)
            if self.logger: self.logger.info(f"Файл '{self.index_file}' удалён.")
            self._index = None
            return True