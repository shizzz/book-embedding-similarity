import os
import faiss
from typing import List
from app.models import BookRegistry, Book
from app.settings.config import DATA_DIR, HNSW_EF_SEARCH, MODEL_NAME, IndexLevel
from .trainers.rerankerTrainer import RerankerTrainer
try:
    from tqdm import tqdm
except ImportError:
    def tqdm(iterable, *args, **kwargs):
        return iterable
    
class IndexManager:
    def __init__(
        self,
        batch_size: int = None,
        reranker_trainer: RerankerTrainer | None = None,
        logger=None,
    ):
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
    
    def check_index(self):
        if os.path.exists(self.index_file):
            return True
        else:
            return False

    def generate_and_save(self):
        pass

    def load_from_file(self, indexLevel: IndexLevel) -> faiss.IndexIDMap:
        if not os.path.exists(self.index_file):
            raise FileNotFoundError(f"Файл '{self.index_file}' не существует")

        index = faiss.read_index(str(DATA_DIR / f"{MODEL_NAME}.{indexLevel.value}.faiss"))

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
