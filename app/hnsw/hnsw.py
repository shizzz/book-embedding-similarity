import os
import faiss
import numpy as np
from tqdm import tqdm
from typing import List, Tuple
from app.models import Embedding
from app.settings.config import HNSW_M, HNSW_EF_CONSTRUCTION, HNSW_EF_SEARCH, INDEX_FILE
from .trainers.rerankerTrainer import RerankerTrainer

class HNSW:
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

    def __estimate_hnsw_memory_gb(self, ntotal: int, dim: int, overhead_factor: float = 1.12) -> float:
        bytes_per_vector = (dim * 4) + (HNSW_M * 8)
        total_bytes = ntotal * bytes_per_vector
        total_bytes_with_overhead = total_bytes * overhead_factor
        gb = total_bytes_with_overhead / (1024 ** 3)
        return gb

    def load_emb(self, embeddings: List[Tuple[int, bytes]]):     
        valid_embeddings = []

        # можно и из памяти, но сейчас влом. Важно, чтобы сохранился индекс
        with tqdm(total=len(embeddings), desc="Загружаем ембеддинги", unit=" строк\с", unit_scale=True) as pbar:
            for embedding in embeddings:
                emb = Embedding.from_db(embedding[1]).vec
                valid_embeddings.append(emb)
                pbar.update(1)

        self.embeddings = np.ascontiguousarray(valid_embeddings).astype(np.float32)
        self.embedding_dim = self.embeddings.shape[1]  # [кол-во_строк, размерность_вектора]
        del valid_embeddings

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

    def generate_and_save(self) -> faiss.IndexHNSWFlat:
        if len(self.embeddings) == 0:
            raise ValueError(f"Попытка сохранить индекс с пустым списокм векторов")
        
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

        if self.logger: self.logger.info(
            "HNSW индекс построен:\n"
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

    def load_from_file(self) -> faiss.IndexHNSWFlat:
        if not os.path.exists(self.index_file):
            raise FileNotFoundError(f"Файл '{self.index_file}' не существует")

        index = faiss.read_index(self.index_file)
        if not isinstance(index, faiss.IndexHNSWFlat):
            raise TypeError("Загруженный индекс не является HNSWFlat")

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

    def rebuild_trainer(
            self,
            feedbacks=None,
            books=None,
    ):
        if self.logger:
            self.logger.info("Обучаем reranker по feedback")

        self.reranker_trainer.train(
            feedbacks=feedbacks,
            embeddings=self.embeddings,
            books=books
        )
        
    def rebuild(
        self,
        feedbacks=None,
        books=None,
        train_reranker: bool = True,
    ):
        if (
            train_reranker
            and self.reranker_trainer
            and feedbacks
            and books
        ):
            self.rebuild_trainer(feedbacks, books)

        if self.logger:
            self.logger.info("Запущен rebuild HNSW")

        self.delete_index_file(force=True)
        self._index = self.generate_and_save()

        if self.logger:
            self.logger.info("Rebuild завершён")
