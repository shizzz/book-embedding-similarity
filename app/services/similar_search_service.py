import numpy as np
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
        self.__step_percent = step_percent
        self.__db = DBManager()

    def run(self):
        query_norm = self.__source.norm_embedding
        if query_norm is None:
            return []

        candidates = []

        current = 0
        total = len(self.__registry.books)
        step = max(1, total * self.__step_percent // 100)

        for book in self.__registry.books:
            current += 1
            book_norm = book.norm_embedding
            if book_norm is None:
                continue

            if self.__source.file_name == book.file_name:
                continue
            if self.__source.title == book.title:
                continue
            if self.__exclude_same_authors and self.__source.authors and book.authors:
                if set(self.__source.authors) & set(book.authors):
                    continue

            score = float(np.dot(book_norm, query_norm))

            candidates.append((book, score))

            if self.__queue and current % step == 0:
                percent = min(99, current * 100 // total)
                self.__db.update_process_percent(self.__queue.book, percent)

        candidates.sort(key=lambda x: x[1], reverse=True)
        return candidates[:self.__limit]