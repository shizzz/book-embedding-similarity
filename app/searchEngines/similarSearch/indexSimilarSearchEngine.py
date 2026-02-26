import numpy as np
from faiss import IndexIDMap
from typing import List, Sequence, Tuple
from app.models import Book, BookRegistry
from app.hnsw.rerankers import Reranker
from .similarSearchEngine import SimilarSearchEngine

class IndexSimilarSearchEngine(SimilarSearchEngine):
    def __init__(
        self,
        index,
        books: Sequence[Book],
        limit: int,
        reranker: Reranker = None,
        exclude_same_authors: bool = False,
        step_percent: int = 5,
        logger = None,
    ):
        super().__init__(exclude_same_authors, reranker)
        self.index: IndexIDMap = index
        self.books = BookRegistry(books)
        self._limit = limit
        self.reranker = reranker
        self._step_percent = step_percent
        self.logger = logger
        
    def search(self, sources: BookRegistry, progress_callback=None) -> list[tuple[float, int, int]]:
        if self.index is None or self.index.ntotal == 0 or not sources:
            return []

        n = len(sources)
        dim = sources.books[0].embedding.shape[0]

        # Подготовка эмбеддингов
        embeddings = np.empty((n, dim), dtype=np.float32)
        for i, book in enumerate(sources):
            emb = book.embedding
            embeddings[i] = emb if emb is not None else 0.0
        embeddings = np.ascontiguousarray(embeddings, dtype=np.float32)

        # FAISS поиск
        k = min(self._limit * 10, self.index.ntotal)
        scores, indices = self.index.search(embeddings, k)

        results: list[tuple[float, int, int]] = []

        for src_i, source in enumerate(sources):
            seen_books: set[tuple[str, tuple[str, ...]]] = set()

            # Получаем всех кандидатов
            candidates_books = [
                self.books.get(cid) for cid in indices[src_i] if cid != -1
            ]

            if not candidates_books:
                continue

            # Векторная фильтрация по title и file_name
            titles = np.array([b.title or "" for b in candidates_books])
            file_names = np.array([b.file_name or "" for b in candidates_books])

            mask = (titles != (source.title or "")) & (file_names != (source.file_name or ""))

            # Фильтрация по авторам через Python list comprehension
            if self._exclude_same_authors and source.authors:
                mask_authors = np.array([
                    not bool(source.authors & c.authors) if c.authors else True
                    for c in candidates_books
                ])
                mask &= mask_authors

            filtered_candidates = [
                (float(score), b)
                for score, b, m in zip(scores[src_i], candidates_books, mask) if m
            ]

            # Отсекаем повторяющиеся title+authors_key
            unique_candidates = []
            for score, b in filtered_candidates:
                key = (b.title, b.authors_key)
                if key not in seen_books:
                    seen_books.add(key)
                    unique_candidates.append((score, b))
                if len(unique_candidates) >= self._limit * 10:
                    break

            # Ререйк и добавление в результаты
            reranked = self._rerank(unique_candidates)
            for score, b in reranked[:self._limit]:
                results.append((score, source.id, b.id))

        return results