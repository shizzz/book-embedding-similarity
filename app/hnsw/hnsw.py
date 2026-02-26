import os
import faiss
import numpy as np
from typing import List
from app.models import BookRegistry, Book
from app.settings.config import HNSW_M, HNSW_EF_CONSTRUCTION, HNSW_EF_SEARCH, INDEX_FILE
from .trainers.rerankerTrainer import RerankerTrainer
try:
    from tqdm import tqdm
except ImportError:
    def tqdm(iterable, *args, **kwargs):
        return iterable
    
class IndexManager:
    def __init__(
        self,
        index_file: str = f"{INDEX_FILE}",
        batch_size: int = None,
        reranker_trainer: RerankerTrainer | None = None,
        logger=None,
    ):
        self.index_file = index_file
        self.batch_size = batch_size
        self.reranker_trainer = reranker_trainer
        self.logger = logger

        self._index = None
        self.embeddings = []
        self.embedding_dim = 0

    def __estimate_hnsw_memory_gb(self, ntotal, dim, overhead_factor=1.1):
        mem_bytes = ntotal * dim * 4
        mem_bytes *= overhead_factor
        return mem_bytes / (1024**3)

    def load_emb(self, embeddings: list[tuple[int, np.ndarray]]):
        valid_embeddings = []
        valid_ids = []

        expected_dim = None
        with tqdm(total=len(embeddings), desc="Загружаем embeddings", unit="строк", unit_scale=True) as pbar:
            for book_id, vec in embeddings:
                if expected_dim is None:
                    expected_dim = vec.shape[0]
                if vec.shape[0] != expected_dim:
                    print(f"Книга={book_id} с размерностью {vec.shape[0]} не соответствует общей {expected_dim}")
                    pbar.update(1)
                    continue
                if vec is None:
                    pbar.update(1)
                    continue
                valid_embeddings.append(vec)
                valid_ids.append(book_id)
                pbar.update(1)

        if not valid_embeddings:
            self.embeddings = np.empty((0, 0), dtype=np.float32)
            self.ids = np.empty((0,), dtype=np.int64)
            self.embedding_dim = 0
            return

        self.embeddings = np.ascontiguousarray(np.stack(valid_embeddings)).astype(np.float32)
        self.ids = np.array(valid_ids, dtype=np.int64)
        self.embedding_dim = self.embeddings.shape[1]

        del valid_embeddings, valid_ids

    def get_index(self) -> faiss.IndexHNSWFlat:
        if len(self.embeddings) == 0:
            raise ValueError(f"Попытка сохранить индекс с пустым списокм векторов")
        
        if not self.batch_size:
            self.batch_size = len(self.embeddings) // 100

        if self._index is not None:
            return self._index

        if os.path.exists(self.index_file):
            if self.logger: self.logger.info(f"Файл '{self.index_file}' найден. Загружаем...")
            self._index = self.load_from_file()
        else:
            if self.logger: self.logger.info(f"Файл '{self.index_file}' не найден. Генерируем и сохраняем...")
            self._index = self.generate_and_save()

        return self._index
    
    def check_index(self):
        if os.path.exists(self.index_file):
            return True
        else:
            return False

    def generate_and_save(self):
        if self.embeddings.shape[0] == 0:
            raise ValueError("Попытка сохранить индекс с пустым списком векторов")

        if self.embeddings.shape[1] != self.embedding_dim:
            raise ValueError(f"Размерность embeddings ({self.embeddings.shape[1]}) не совпадает с embedding_dim ({self.embedding_dim})")

        # создаём базовый HNSW
        base_index = faiss.IndexHNSWFlat(self.embedding_dim, HNSW_M, faiss.METRIC_INNER_PRODUCT)
        base_index.hnsw.efConstruction = HNSW_EF_CONSTRUCTION
        base_index.hnsw.efSearch = HNSW_EF_SEARCH

        # оборачиваем в IndexIDMap для хранения book_id
        index = faiss.IndexIDMap(base_index)

        n_total = self.embeddings.shape[0]
        if self.logger:
            self.logger.info(f"Генерация HNSW: {n_total:,} векторов, dim={self.embedding_dim}, M={HNSW_M}, efConstruction={HNSW_EF_CONSTRUCTION}")

        # добавляем батчами
        with tqdm(total=n_total, desc="Добавление векторов в HNSW", unit="vec", unit_scale=True) as pbar:
            for i in range(0, n_total, self.batch_size):
                end = min(i + self.batch_size, n_total)
                batch = self.embeddings[i:end]
                batch_ids = self.ids[i:end]
                index.add_with_ids(batch, batch_ids)
                pbar.update(end - i)

        mem_gb = self.__estimate_hnsw_memory_gb(ntotal=index.ntotal, dim=self.embedding_dim, overhead_factor=1.10)

        if self.logger:
            self.logger.info(
                "HNSW индекс построен:\n"
                f"  • количество векторов       : {index.ntotal:,}\n"
                f"  • размерность               : {index.d}\n"
                f"  • M (связи на узел)         : {HNSW_M}\n"
                f"  • efConstruction            : {HNSW_EF_CONSTRUCTION}\n"
                f"  • efSearch (по умолчанию)   : {HNSW_EF_SEARCH}\n"
                f"  • память                    : ~ {mem_gb:.1f}–{mem_gb*1.15:.1f} GB"
            )

        faiss.write_index(index, self.index_file)
        if self.logger:
            self.logger.info(f"Индекс сохранён в '{self.index_file}' (размер: {os.path.getsize(self.index_file) / (1024**2):.2f} MB)")

        self._index = index
        return index

    def load_from_file(self) -> faiss.IndexIDMap:
        if not os.path.exists(self.index_file):
            raise FileNotFoundError(f"Файл '{self.index_file}' не существует")

        index = faiss.read_index(self.index_file)

        # Проверяем, что это IndexIDMap с HNSW внутри
        if not isinstance(index, faiss.IndexIDMap):
            # Если это HNSW без ID, оборачиваем в IndexIDMap (legacy)
            if isinstance(index, faiss.IndexHNSWFlat):
                index = faiss.IndexIDMap(index)
                if self.logger:
                    self.logger.warning("Загружен HNSW без ID, обернут в IndexIDMap")
            else:
                raise TypeError("Загруженный индекс не является IndexIDMap или HNSWFlat")

        # Обновляем efSearch
        try:
            if isinstance(index.index, faiss.IndexHNSWFlat):
                index.index.hnsw.efSearch = HNSW_EF_SEARCH
            elif isinstance(index, faiss.IndexHNSWFlat):
                index.hnsw.efSearch = HNSW_EF_SEARCH
            else:
                if self.logger:
                    self.logger.warning("Не удалось обновить efSearch — неизвестный тип индекса")
        except Exception as e:
            if self.logger:
                self.logger.warning(f"Ошибка при установке efSearch: {e}")

        # --- перенос на GPU если доступен ---
        self._gpu_res = None

        try:
            ngpu = faiss.get_num_gpus()
        except Exception:
            ngpu = 0

        if ngpu > 0:
            try:
                if self.logger:
                    self.logger.info(f"Обнаружено GPU: {ngpu}, переносим индекс на GPU")

                self._gpu_res = faiss.StandardGpuResources()

                # переносим IndexIDMap целиком
                index = faiss.index_cpu_to_gpu(self._gpu_res, 0, index)

                if self.logger:
                    self.logger.info("Индекс успешно перенесён на GPU")

            except Exception as e:
                self._gpu_res = None
                if self.logger:
                    self.logger.warning(f"Не удалось перенести индекс на GPU, используем CPU: {e}")
        else:
            if self.logger:
                self.logger.info("GPU не обнаружен, используем CPU индекс")

        if self.logger:
            device = "GPU" if self._gpu_res else "CPU"
            self.logger.info(
                f"Индекс загружен из '{self.index_file}' "
                f"(ntotal: {index.ntotal:,}, device: {device})"
            )

        self._index = index
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

    def rebuild_trainer(
            self,
            feedbacks=None,
            books:List[Book]=None,
    ):
        if self.logger:
            self.logger.info("Обучаем reranker по feedback")

        self.reranker_trainer.train(
            feedbacks=feedbacks,
            index=self._index,
            books=books
        )
        
    def rebuild(
        self,
        feedbacks=None,
        books: BookRegistry=None,
        train_reranker: bool = True,
    ):
        if (
            train_reranker
            and self.reranker_trainer
            and books
        ):
            self.rebuild_trainer(feedbacks, books)

        if self.logger:
            self.logger.info("Запущен rebuild HNSW")

        self.delete_index_file(force=True)
        self._index = self.generate_and_save()

        if self.logger:
            self.logger.info("Rebuild завершён")
