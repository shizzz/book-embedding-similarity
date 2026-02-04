import time
import logging
import numpy as np
from typing import List, Tuple
from app.models import Book, Feedbacks, Similar
from app.db import DBManager
import faiss

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
logger.addHandler(handler)

class BulkSimilarSearchService:
    def __init__(
        self,
        limit: int = 100,
        exclude_same_authors: bool = False,
        ef_search: int = 64,
        ef_construction: int = 200,
        m: int = 32,
        boos_factor: float = 0.4
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

        self.feedbacks: Feedbacks = None  # будет установлен в prepare_index
        self.prepare_index()

    def prepare_index(self):
        start_total = time.perf_counter()

        # 1. Загрузка фидбека (один раз при старте сервиса)
        self.feedbacks = self.__db.fetch_feedbacks_all()
        logger.info(f"Загружено {len(self.feedbacks.feedbacks)} записей фидбека")

        # 2. Загрузка книг
        logger.info("Загрузка книг из базы...")
        start_load = time.perf_counter()
        raw_books = self.__db.load_books_with_embeddings()
        load_time = time.perf_counter() - start_load
        logger.info(f"Загружено {len(raw_books)} книг за {load_time:.2f} сек")

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
        logger.info(f"Фильтрация: {filter_time:.2f} сек, валидных: {len(self.valid_books)} "
                    f"(пропущено: {skipped_no_emb} без эмб., {skipped_dim} размерность, {skipped_zero} нулевая норма)")

        if not valid_emb_list:
            logger.warning("Нет валидных эмбеддингов")
            return

        self.valid_embeddings = np.vstack(valid_emb_list).astype(np.float32)
        logger.info(f"Массив: {self.valid_embeddings.shape}, ~{self.valid_embeddings.nbytes / 1024**2:.1f} MiB")

        # Индекс
        start_build = time.perf_counter()
        logger.info(f"Построение HNSW (M={self.__m}, efSearch={self.__ef_search})...")
        self.index = faiss.IndexHNSWFlat(self.embedding_dim, self.__m)
        self.index.hnsw.efConstruction = self.__ef_construction
        self.index.hnsw.efSearch = self.__ef_search
        self.index.add(self.valid_embeddings)
        build_time = time.perf_counter() - start_build
        logger.info(f"Индекс готов за {build_time:.2f} сек, векторов: {self.index.ntotal}")

        total_time = time.perf_counter() - start_total
        logger.info(f"Инициализация сервиса: {total_time:.2f} сек")

    def run(self, source: Book) -> List[Tuple[float, Book]]:
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

            book = self.valid_books[idx]

            if book.file_name == source.file_name or book.title == source.title:
                continue

            if self.__exclude_same_authors and source.authors and book.authors:
                if set(source.authors) & set(book.authors):
                    continue

            similarity = float(dist)  # cosine

            # Фидбек — быстрый доступ по словарю
            boost = self.feedbacks.get_boost(source.file_name, book.file_name, self.__boos_factor)
            adjusted = similarity + boost

            candidates.append((adjusted, source, book))

            if len(candidates) >= self.__limit * 3:
                break

        candidates.sort(key=lambda x: x[0], reverse=True)
        top = candidates[:self.__limit]
        
        result = []
        for score, source, book in top:
            result.append(Similar.from_books(score, source, book))
        return result