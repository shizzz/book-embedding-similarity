from typing import Literal
from app.hnsw import HNSW
from app.hnsw.rerankers import LightGBMReranker
from app.db import db, BookRepository
from app.models import Book
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
    ) -> SimilarSearchEngine:
        if mode == SimilarSearchEngineFactory.INDEX:
            hnsw = HNSW()
            index = hnsw.load_from_file()

            with db() as conn:
                books: list[Book] = [
                    Book.map_row(row)
                    for row in BookRepository().get_all_with_embeddings(conn)
                ]

            return IndexSimilarSearchEngine(
                reranker=LightGBMReranker(),
                index=index,
                books=books,
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