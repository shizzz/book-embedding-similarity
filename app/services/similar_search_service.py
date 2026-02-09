import time
from typing import List, Tuple
from app.models import Book, Similar, Embedding
from app.db import db, BookRepository, FeedbackRepository, EmbeddingsRepository
from app.services.hnswService import HNSWService
from app.settings.config import FEEDBACK_BOOST_FACTOR
from app.searchEngines import IndexSearchEngine, BruteforceSearchEngine

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
            # Use index-based engine
            hnsw_service = HNSWService()
            index = hnsw_service.load_from_file()

            # build books list (same ordering as when index was built)
            with db() as conn:
                rows = list[Tuple[int, str, str, str, bytes]](BookRepository().get_all_with_embeddings(conn))
                books_by_idx = [Book(id=book_id, archive_name=archive, file_name=book, title=title) for book_id, archive, book, title, _ in rows]
                feedbacks = FeedbackRepository().get(conn, self._source.id)

            engine = IndexSearchEngine(index=index, books=books_by_idx, feedbacks=feedbacks, limit=self._limit, exclude_same_authors=self._exclude_same_authors)
            result = engine.search(self._source, self._embedding)
        else:
            engine = BruteforceSearchEngine(limit=self._limit, exclude_same_authors=self._exclude_same_authors, step_percent=self._step_percent)
            result = engine.search(self._source, self._embedding, progress_callback)

        self.last_run_seconds = time.perf_counter() - started_at

        if progress_callback:
            progress_callback(100)

        return result

    def _should_skip(self, source: Book, candidate: Book) -> bool:
        if source.title != None and candidate.title != None and source.title == candidate.title:
            return True

        return False