import time
import numpy as np
from typing import List
from app.services.hnswService import HNSWService
from app.models import Book, Feedbacks, Similar
from app.db import DBManager
from app.settings.config import SIMILARS_PER_BOOK

class BulkSimilarSearchService:
    def __init__(
        self,
        limit: int = SIMILARS_PER_BOOK,
        exclude_same_authors: bool = False,
        ef_search: int = 64,
        ef_construction: int = 200,
        m: int = 32,
        boos_factor: float = 0.4,
        logger = None
    ):
        self.__limit = limit
        self.__exclude_same_authors = exclude_same_authors
        self.__ef_search = ef_search
        self.__ef_construction = ef_construction
        self.__m = m
        self.__boos_factor = boos_factor

        self.__db = DBManager()
        self.index = None
        self.embedding_dim = None
        self.valid_books: List[Book] = []
        self.valid_embeddings: np.ndarray = None

        self.logger = logger
        self.feedbacks: Feedbacks = None  # будет установлен в prepare_index
        self.__prepare_index()
            
    def __prepare_index(self):
        start_total = time.perf_counter()

        # 1. Загрузка фидбека (один раз при старте сервиса)
        self.feedbacks = self.__db.fetch_feedbacks_all()
        if self.logger: self.logger.info(f"Загружено {len(self.feedbacks.feedbacks)} записей фидбека")

        # 2. Загрузка книг
        if self.logger: self.logger.info("Загрузка книг из базы...")
        start_load = time.perf_counter()
        raw_books = self.__db.load_books_with_embeddings()
        load_time = time.perf_counter() - start_load
        if self.logger: self.logger.info(f"Загружено {len(raw_books)} книг за {load_time:.2f} сек")

        valid_emb_list = []
        self.valid_books = []

        start_filter = time.perf_counter()
        skipped_no_emb = skipped_dim = skipped_zero = 0

        for book in raw_books:
            if book.embedding is None:  # используем свойство из модели Book
                skipped_no_emb += 1
                continue

            emb = book.embedding

            if self.embedding_dim is None:
                self.embedding_dim = emb.shape[0]
            elif emb.shape[0] != self.embedding_dim:
                skipped_dim += 1
                continue

            valid_emb_list.append(emb)
            self.valid_books.append(book)

        filter_time = time.perf_counter() - start_filter
        if self.logger: self.logger.info(f"Фильтрация: {filter_time:.2f} сек, валидных: {len(self.valid_books)} "
                    f"(пропущено: {skipped_no_emb} без эмб., {skipped_dim} размерность, {skipped_zero} нулевая норма)")

        if not valid_emb_list:
            if self.logger: self.logger.warning("Нет валидных эмбеддингов")
            return

        self.valid_embeddings = np.vstack(valid_emb_list).astype(np.float32)
        if self.logger: self.logger.info(f"Массив: {self.valid_embeddings.shape}, ~{self.valid_embeddings.nbytes / 1024**2:.1f} MiB")

        if self.logger: self.logger.info(f"Построение HNSW (M={self.__m}, efSearch={self.__ef_search})...")
        hnswService = HNSWService(
            m=self.__m,
            ef_construction=self.__ef_construction,
            ef_search=self.__ef_search,
            embedding_dim=self.embedding_dim,
            embeddings=self.valid_embeddings,
            logger=self.logger
        )
        self.index = hnswService.get_index()

        total_time = time.perf_counter() - start_total
        if self.logger: self.logger.info(f"Инициализация сервиса: {total_time:.2f} сек")

    def __should_skip(self, source: Book, candidate: Book) -> bool:
        if candidate.file_name == source.file_name or candidate.title == source.title:
            return True

        if self.__exclude_same_authors and source.authors and candidate.authors:
            if set(source.authors) & set(candidate.authors):
                return True
        return False

    def run(self, source: Book) -> List[Similar]:
        if self.index is None:
            return []

        query_emb = source.embedding
        if query_emb is None or query_emb.shape[0] != self.embedding_dim:
            return []

        query = query_emb.reshape(1, -1).astype(np.float32)

        k = min(self.__limit * 20 + 200, self.index.ntotal)
        distances, indices = self.index.search(query, k)

        candidates = []

        for dist, idx in zip(distances[0], indices[0]):
            if idx == -1 or idx >= len(self.valid_books):
                continue

            candidate = self.valid_books[idx]
            
            if self.__should_skip(source, candidate):
                continue

            similarity = float(dist)  # cosine

            # Фидбек — быстрый доступ по словарю
            boost = self.feedbacks.get_boost(source.file_name, candidate.file_name, self.__boos_factor)
            adjusted = similarity + boost

            candidates.append((adjusted, source, candidate))

            if len(candidates) >= self.__limit * 3:
                break

        candidates.sort(key=lambda x: x[0], reverse=True)
        top = candidates[:self.__limit]
        
        result = []
        for score, source, candidate in top:
            result.append(Similar.from_books(score, source, candidate))
        return result