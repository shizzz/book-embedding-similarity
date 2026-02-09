import time
import numpy as np
from typing import List, Tuple
from app.services.hnswService import HNSWService
from app.models import Book, Feedbacks, Similar, Embedding
from app.db import db, FeedbackRepository
from app.settings.config import SIMILARS_PER_BOOK, FEEDBACK_BOOST_FACTOR

class BulkSimilarSearchService:
    def __init__(
        self,
        books: List[Book],
        embeddings: List[Tuple[int, bytes]],
        limit: int = SIMILARS_PER_BOOK,
        exclude_same_authors: bool = False,
        logger = None, 
    ):
        self.books = books
        self.embeddings = embeddings
        self.__limit = limit
        self.__exclude_same_authors = exclude_same_authors

        self.index = None

        self.logger = logger
        self.feedbacks: Feedbacks = None  # будет установлен в prepare_index
        self.__prepare_index()
            
    def __prepare_index(self):
        start_total = time.perf_counter()

        with db() as conn:
            self.feedbacks = FeedbackRepository().get_all(conn)
            if self.logger: self.logger.info(f"Загружено {len(self.feedbacks.feedbacks)} записей фидбека")

        hnswService = HNSWService(logger=self.logger)
        if hnswService.check_index():
            self.index = hnswService.load_from_file()
        else:
            hnswService.load_emb(self.embeddings)
            self.index = hnswService.generate_and_save()

        total_time = time.perf_counter() - start_total
        if self.logger: self.logger.info(f"Инициализация сервиса: {total_time:.2f} сек")

    def _should_skip(self, source: Book, candidate: Book) -> bool:
        if source.title != None and candidate.title != None and source.title == candidate.title:
            return True

        if self.__exclude_same_authors and source.authors and candidate.authors:
            if set[str](source.authors) & set[str](candidate.authors):
                return True
        return False

    def run(self, source_book: Book, source_embedding: bytes) -> List[Similar]:
        if self.index is None:
            return []

        query_emb = Embedding.from_db(source_embedding)
        query = query_emb.vec.reshape(1, -1).astype(np.float32)

        k = min(self.__limit * 20 + 200, self.index.ntotal)
        scores, indices = self.index.search(query, k)

        candidates = []

        for score_raw, idx in zip(scores[0], indices[0]):
            if idx == -1 or idx >= len(self.books):
                continue

            candidate = self.books[idx]
            
            if self._should_skip(source_book, candidate):
                continue

            similarity = float(score_raw)
            boost = self.feedbacks.get_boost(source_book.id, candidate.id, FEEDBACK_BOOST_FACTOR)
            total_score = similarity + boost

            candidates.append((total_score, source_book, candidate))

        candidates.sort(key=lambda x: x[0], reverse=True)
        top = candidates[:self.__limit]
        
        result = []
        for score, source_book, candidate in top:
            result.append(Similar.from_books(score, source_book, candidate))
        return result