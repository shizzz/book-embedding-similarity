import time
from typing import List, Tuple
from app.db import DBRouter
from app.db.repositories import EmbeddingsRepository, ModelRepository
from app.searchEngines.similarSearch import SimilarSearchEngine
from app.settings.config import MODEL_NAME

class SimilarSearchService:
    def __init__(
        self,
        engine: SimilarSearchEngine  
    ):
        self._engine = engine
        self.last_run_seconds = None
        
        router = DBRouter()
        model_uid = ModelRepository(router).get_latest_uid(MODEL_NAME)
        self._total = EmbeddingsRepository(router, model_uid).count()

    def run(self, source: int, progress_callback=None) -> List[Tuple[float, int, int]]:
        started_at = time.perf_counter()
        repository = [source]

        result = self._engine.search(
            sources=repository,
            progress_callback=progress_callback
        )
        self.last_run_seconds = time.perf_counter() - started_at

        if progress_callback:
            progress_callback(100)

        return result