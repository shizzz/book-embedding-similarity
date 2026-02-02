import pickle
import numpy as np
from typing import List, Tuple
from app.models import BookRegistry, BookTask, QueueRecord
from app.db import DBManager

class SimilarSearchService:
    def __init__(
        self,
        source: BookTask,
        registry: BookRegistry,
        limit: int,
        queue: QueueRecord = None,
        exclude_same_authors: bool = False,
        step_percent: int = 5,
    ):
        self.__source = source
        self.__queue = queue
        self.__registry = registry
        self.__limit = limit
        self.__exclude_same_authors = exclude_same_authors
        self.__candidates = []
        self.__current = 0
        self.__step_percent = step_percent
        self.__step = registry.bookCount() * step_percent // 100
        self.__db = DBManager()

    def cosine_similarity(self, a: np.ndarray, b: np.ndarray) -> float:
        # нормализуем оба вектора
        a_norm = a / np.linalg.norm(a)
        b_norm = b / np.linalg.norm(b)
        return float(np.dot(a_norm, b_norm))

    def get_result(self) -> List[Tuple[BookTask, float]]:
        self.__candidates.sort(key=lambda x: x[1], reverse=True)
        return self.__candidates[:self.__limit]
    
    def run(self):
        for book in self.__registry.books:
            self.__current += 1
            if book.embedding is None:
                continue

            if self.__source.file_name == book.file_name:
                continue

            if self.__source.title == book.title:
                continue

            if self.__exclude_same_authors and self.__source.authors and book.authors:
                if set(self.__source.authors) & set(book.authors):
                    continue
                
            query_emb = pickle.loads(self.__source.embedding)
            book_emb  = pickle.loads(book.embedding)

            score = self.cosine_similarity(query_emb, book_emb)

            self.__candidates.append((book, score))

            if self.__queue is not None:
                if (self.__current + 1) % self.__step == 0:
                    percent = self.__current * self.__step_percent // self.__step
                    self.__db.update_process_percent(self.__queue.book, percent)
