from typing import Literal
from faiss import IndexIDMap
from app.hnsw import IndexManager
from app.hnsw.rerankers import LightGBMReranker
from app.db import DBRouter
from app.db.repositories import BookRepository
from app.models import Book, BookRegistry
from .similarSearchEngine import SimilarSearchEngine
from .indexSimilarSearchEngine import IndexSimilarSearchEngine
from .bruteforceSimilarSearchEngine import BruteforceSimilarSearchEngine


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
            index: IndexIDMap = hnsw.load_from_file()

            return IndexSimilarSearchEngine(
                reranker=LightGBMReranker(),
                router=router,
                index=index,
                limit=limit,
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