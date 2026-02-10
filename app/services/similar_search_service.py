import time
from typing import List, Tuple
from app.models import Book, Embedding
from app.db import db, BookRepository, FeedbackRepository
from app.searchEngines import SimilarSearchEngine

class SimilarSearchService:
    def __init__(
        self,
        engine: SimilarSearchEngine,
        source: Book,
        embedding: Embedding
    ):
        self._engine = engine
        self._source = source
        self._embedding = embedding
        self.last_run_seconds = None
        
        with db() as conn:
            self._total = BookRepository.count_embeddings(conn)
            self._feedbacks = FeedbackRepository().get(conn, source.id)

    def run(self, progress_callback=None) -> List[Tuple[float, int, int]]:
        if self._embedding is None:
            return []

        started_at = time.perf_counter()

        result = self._engine.search(
            source=self._source,
            embedding=self._embedding,
            feedbacks=self._feedbacks,
            progress_callback=progress_callback
        )
        self.last_run_seconds = time.perf_counter() - started_at

        if progress_callback:
            progress_callback(100)

        return result