import numpy as np
import pickle
from typing import List, Tuple
from app.models import Book
from app.db import DBManager

class SimilarSearchService:
    def __init__(
        self,
        source: Book,
        limit: int,
        exclude_same_authors: bool = False,
        step_percent: int = 5,
    ):
        self.__source = source
        self.__limit = limit
        self.__exclude_same_authors = exclude_same_authors
        self.__step_percent = step_percent

        db = DBManager()
        self.__db = db
        self.__total = db.finished_books_count()

    def run(self, progress_callback=None) -> List[Tuple[float, Book]]:
        if self.__source.embedding is None:
            return []

        feedbacks = self.__db.fetch_feedbacks(self.__source.file_name)

        candidates = []
        current = 0
        step = max(1, self.__total * self.__step_percent // 100)

        with self.__db.connection() as conn:
            cursor = conn.cursor()
            cursor.execute(self.__db.embeddings_query)

            for row in cursor:
                current += 1

                archive, book, title, embedding, authors_csv = row
                authors = authors_csv.split(',') if authors_csv else []

                if book == self.__source.file_name or title == self.__source.title:
                    continue
                if self.__exclude_same_authors and self.__source.authors and authors:
                    if set(self.__source.authors) & set(authors):
                        continue

                try:
                    emb = pickle.loads(embedding)
                    norm = np.linalg.norm(emb) + 1e-12
                    emb_norm = (emb / norm).astype(np.float32)
                    similarity = np.dot(emb_norm, self.__source.embedding)

                    boost = feedbacks.get_boost(self.__source.file_name, book)
                    adjusted = similarity + boost

                    candidates.append((adjusted, Book(
                        archive_name=archive,
                        file_name=book,
                        title=title,
                        authors=authors
                    )))

                except Exception:
                    continue

                if progress_callback and current % step == 0:
                    percent = min(99, current * 100 // self.__total)
                    progress_callback(percent)

        # Финальная сортировка уже по adjusted_score
        candidates.sort(key=lambda x: x[0], reverse=True)

        top_candidates = candidates[:self.__limit]

        if progress_callback:
            progress_callback(100)

        return top_candidates