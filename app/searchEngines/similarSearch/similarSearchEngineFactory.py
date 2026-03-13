from faiss import IndexIDMap
from app.hnsw import IndexManager
from app.hnsw.rerankers import LightGBMReranker
from app.infrastructure.db import DBRouter
from app.infrastructure.models import SimilarSearchEngineType
from .similarSearchEngine import SimilarSearchEngine
from .indexSimilarSearchEngine import IndexSimilarSearchEngine
from .bruteforceSimilarSearchEngine import BruteforceSimilarSearchEngine
from app.settings import IndexLevel, SearchIndexLevel, IndexConfig

class SimilarSearchEngineFactory:
    @classmethod
    def create(
        cls,
        mode: SimilarSearchEngineType,
        router: DBRouter,
        limit: int,
        exclude_same_authors: bool,
        step_percent: int = 5,
        logger=None,
    ) -> SimilarSearchEngine:
        if mode == SimilarSearchEngineType.INDEX:
            hnsw = IndexManager(logger=logger)

            chunk_index = None
            document_index = None

            if IndexConfig.SEARCH_INDEX_LEVEL == SearchIndexLevel.CHUNK:
                chunk_index: IndexIDMap = hnsw.load_from_file(IndexLevel.CHUNK)

            if IndexConfig.SEARCH_INDEX_LEVEL == SearchIndexLevel.DOCUMENT:
                document_index: IndexIDMap = hnsw.load_from_file(IndexLevel.DOCUMENT)

            return IndexSimilarSearchEngine(
                reranker=LightGBMReranker(),
                router=router,
                document_index=document_index,
                chunk_index=chunk_index,
                limit=limit,
                level=IndexConfig.SEARCH_INDEX_LEVEL,
                exclude_same_authors=exclude_same_authors,
                step_percent=step_percent,
                logger=logger
            )

        elif mode == SimilarSearchEngineType.BRUTEFORCE:
            return BruteforceSimilarSearchEngine(
                reranker=LightGBMReranker(),
                router=router,
                limit=limit,
                exclude_same_authors=exclude_same_authors,
                step_percent=step_percent,
                logger=logger
            )

        raise ValueError(f"Unknown mode: {mode}")