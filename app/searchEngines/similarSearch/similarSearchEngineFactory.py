from typing import Literal
from faiss import IndexIDMap
from app.hnsw import IndexManager
from app.hnsw.rerankers import LightGBMReranker
from app.db import DBRouter
from .similarSearchEngine import SimilarSearchEngine
from .indexSimilarSearchEngine import IndexSimilarSearchEngine
from .bruteforceSimilarSearchEngine import BruteforceSimilarSearchEngine
from app.settings.config import IndexLevel, BUILD_INDEX_LEVEL


class SimilarSearchEngineFactory:
    INDEX = "index" 
    BRUTEFORCE = "bruteforce"
    EngineType = Literal["index", "bruteforce"]

    @classmethod
    def create(
        cls,
        mode: EngineType,
        router: DBRouter,
        limit: int,
        exclude_same_authors: bool,
        step_percent: int = 5,
        logger=None,
    ) -> SimilarSearchEngine:
        if mode == SimilarSearchEngineFactory.INDEX:
            hnsw = IndexManager(logger=logger)

            chunk_index, document_index = None

            if BUILD_INDEX_LEVEL in (IndexLevel.CHUNK, IndexLevel.BOTH):
                chunk_index: IndexIDMap = hnsw.load_from_file(IndexLevel.CHUNK)

            if BUILD_INDEX_LEVEL in (IndexLevel.DOCUMENT, IndexLevel.BOTH):
                document_index: IndexIDMap = hnsw.load_from_file(IndexLevel.DOCUMENT)

            return IndexSimilarSearchEngine(
                reranker=LightGBMReranker(),
                router=router,
                document_index=document_index,
                chunk_index=chunk_index,
                limit=limit,
                level=BUILD_INDEX_LEVEL,
                exclude_same_authors=exclude_same_authors,
                step_percent=step_percent,
            )

        elif mode == SimilarSearchEngineFactory.BRUTEFORCE:
            return BruteforceSimilarSearchEngine(
                reranker=LightGBMReranker(),
                limit=limit,
                exclude_same_authors=exclude_same_authors,
                step_percent=step_percent,
            )

        raise ValueError(f"Unknown mode: {mode}")