from typing import Literal
from faiss import IndexIDMap
from app.hnsw import IndexManager
from app.hnsw.rerankers import LightGBMReranker
from app.db import DB, BookRepository
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
        limit: int,
        exclude_same_authors: bool,
        step_percent: int = 5,
        logger=None,
    ) -> SimilarSearchEngine:
        if mode == SimilarSearchEngineFactory.INDEX:
            hnsw = IndexManager(logger=logger)
            index: IndexIDMap = hnsw.load_from_file()

            with DB() as conn:
                books: list[Book] = [
                    Book.map_row(row)
                    for row in BookRepository.get_all_with_embeddings(conn)
                ]

            return IndexSimilarSearchEngine(
                reranker=LightGBMReranker(),
                index=index,
                books=BookRegistry(books),
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