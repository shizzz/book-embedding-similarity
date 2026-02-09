import time
import numpy as np
from typing import List, Tuple
from app.models import Book, Similar, Embedding
from app.db import db, BookRepository, FeedbackRepository, EmbeddingsRepository
from app.services.hnswService import HNSWService
from app.settings.config import FEEDBACK_BOOST_FACTOR

class SimilarSearchService:
    def __init__(
        self,
        source: Book,
        embedding: Embedding,
        limit: int,
        exclude_same_authors: bool = False,
        step_percent: int = 5,
        mode: str = "bruteforce",
    ):
        self._source = source
        self._limit = limit
        self._exclude_same_authors = exclude_same_authors
        self._step_percent = step_percent
        self._embedding = embedding
        self._mode = mode
        self.last_run_seconds = None
        
        with db() as conn:
            self.__total = BookRepository.count_embeddings(conn)

    def run(self, progress_callback=None) -> List[Similar]:
        if self._embedding is None:
            return []

        started_at = time.perf_counter()

        if self._mode == "index":
            result = self.__run_index()
        else:
            result = self.__run_bruteforce(progress_callback)

        self.last_run_seconds = time.perf_counter() - started_at

        if progress_callback:
            progress_callback(100)

        return result

    def _should_skip(self, source: Book, candidate: Book) -> bool:
        if source.title != None and candidate.title != None and source.title == candidate.title:
            return True

        return False

    def __run_bruteforce(self, progress_callback=None) -> List[Similar]:
        with db() as conn:
            candidates = []
            current = 0
            step = max(1, self.__total * self._step_percent // 100)

            feedbacks = FeedbackRepository().get(conn, self._source.id)

            for row in EmbeddingsRepository().get_all(conn):
                current += 1

                book_id, embedding_bytes = row

                try:
                    emb_norm = Embedding.from_db(embedding_bytes)
                    similarity = np.dot(emb_norm.vec, self._embedding.vec)

                    boost = feedbacks.get_boost(self._source.id, book_id)
                    score = similarity + boost

                    candidates.append((score, self._source.id, book_id))

                except Exception as e:
                    continue

                if progress_callback and current % step == 0:
                    percent = min(99, current * 100 // self.__total)
                    progress_callback(percent)

        candidates.sort(key=lambda x: x[0], reverse=True)
        top = candidates[:self._limit]

        result = []
        candidate_ids = [candidate_id for _, _, candidate_id in top]

        with db() as conn:
            candidate_books = BookRepository().get_many(conn, candidate_ids)
            for score, book, candidate_id in top:
                candidate = candidate_books.get(candidate_id)
                if candidate is None:
                    continue  # или raise, если это ошибка

                result.append(
                    Similar.from_books(
                        score,
                        self._source,
                        candidate
                    )
                )

        return result

    def __run_index(self) -> List[Similar]:
        # 1. Загружаем индекс из файла (если файла нет, load_from_file выбросит FileNotFoundError)
        hnsw_service = HNSWService()
        index = hnsw_service.load_from_file()

        if index.ntotal == 0:
            return []

        query = self._embedding.vec.reshape(1, -1).astype(np.float32)
        k = min(self._limit * 20 + 200, index.ntotal)
        scores, indices = index.search(query, k)

        candidates = []

        with db() as conn:
            # Загружаем бусты по фидбеку для исходной книги
            feedbacks = FeedbackRepository().get(conn, self._source.id)

            # Восстанавливаем мэппинг "индекс HNSW -> book_id".
            # HNSWService.build использует EmbeddingsRepository.get_all, упорядоченный по book_id ASC,
            # поэтому здесь используем тот же порядок.
            rows = list[Tuple[int, str, str, str, bytes]](BookRepository().get_all_with_embeddings(conn))
            books_by_idx = [(book_id, archive, book, title) for book_id, archive, book, title, _ in rows]

            for score_raw, idx in zip[tuple](scores[0], indices[0]):
                if idx == -1:
                    continue
                if idx < 0 or idx >= len(books_by_idx):
                    continue

                candidate_idx = books_by_idx[idx]
                candidate = Book(id=candidate_idx[0], archive_name=candidate_idx[1], file_name=candidate_idx[2], title=candidate_idx[3])
            
                if self._should_skip(self._source, candidate):
                    continue

                # Для inner-product индекса score_raw уже является скалярным произведением,
                # поэтому это прямой аналог similarity из брутфорса.
                similarity = float(score_raw)
                boost = feedbacks.get_boost(self._source.id, candidate.id, FEEDBACK_BOOST_FACTOR)
                total_score = similarity + boost

                candidates.append((total_score, candidate))

            candidates.sort(key=lambda x: x[0], reverse=True)
            top = candidates[: self._limit]

            if not top:
                return []

            result: List[Similar] = []
            for score, candidate in top:
                result.append(Similar.from_books(score, self._source, candidate))

        return result