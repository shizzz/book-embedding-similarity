import pickle
import numpy as np
from typing import List, Tuple
from app.workers import BaseWorker
from app.models import Book, Task

def cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    a_norm = a / np.linalg.norm(a)
    b_norm = b / np.linalg.norm(b)
    return float(np.dot(a_norm, b_norm))

class SimilarSearchWorker(BaseWorker):
    def __init__(
            self, 
            source: Book, 
            top_k: int, 
            exclude_same_authors: bool, 
            **kwargs):
        super().__init__(**kwargs)
        self.__source = source
        self.__top_k = top_k
        self.__exclude_same_authors = exclude_same_authors
        self.__candidates = []
        self.__current = 0

    def get_result(self) -> List[Tuple[Book, float]]:
        self.__candidates.sort(key=lambda x: x[1], reverse=True)
        return self.__candidates[:self.__top_k]
   
    async def stat_books(self):
        return

    def process_book(self, task: Task):
        self.__current += 1
        if task.book.embedding is None:
            return

        if self.__source.file_name == task.book.file_name:
            return

        if self.__source.title == task.book.title:
            return

        if self.__exclude_same_authors and self.__source.author and task.book.author:
            if set(self.__source.author) & set(task.book.author):
                return
            
        query_emb = pickle.loads(self.__source.embedding)
        book_emb  = pickle.loads(task.book.embedding)

        score = cosine_similarity(query_emb, book_emb)

        self.__candidates.append((task.book, score))