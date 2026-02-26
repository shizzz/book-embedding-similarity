import time
from typing import List, Tuple
from app.models import Book, BookRegistry
from app.db import db, BookRepository
from app.searchEngines.similarSearch import SimilarSearchEngine

class SimilarSearchService:
    def __init__(
        self,
        engine: SimilarSearchEngine  
    ):
        self._engine = engine
        self.last_run_seconds = None
        
        with db() as conn:
            self._total = BookRepository.count_embeddings(conn)

    def run(self, source: Book, progress_callback=None) -> List[Tuple[float, int, int]]:
        if source.embedding is None:
            return []

        started_at = time.perf_counter()
        repository = BookRegistry([source])

        result = self._engine.search(
            sources=repository,
            progress_callback=progress_callback
        )
        self.last_run_seconds = time.perf_counter() - started_at

        if progress_callback:
            progress_callback(100)

        return result