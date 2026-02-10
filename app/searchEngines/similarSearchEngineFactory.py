from typing import Literal
from app.utils import HNSW
from app.db import db, BookRepository
from app.models import Book
from .similarSearchEngine import SimilarSearchEngine
from .indexSimilarSearchEngine import IndexSimilarSearchEngine
from .bruteforceSimilarSearchEngine import BruteforceSimilarSearchEngine


class SimilarSearchEngineFactory:
    INDEX = "index" 
    BRUTEFORCE = "bruteforce"
    
    EngineType = Literal[INDEX, BRUTEFORCE]

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
                rows = BookRepository().get_all_with_embeddings(conn)
                books = [Book(id=r[0], archive_name=r[1], file_name=r[2], title=r[3]) for r in rows]

            return IndexSimilarSearchEngine(
                index=index,
                books=books,
                limit=limit,
                exclude_same_authors=exclude_same_authors,
                step_percent=step_percent,
            )

        elif mode == SimilarSearchEngineFactory.BRUTEFORCE:
            return BruteforceSimilarSearchEngine(
                limit=limit,
                exclude_same_authors=exclude_same_authors,
                step_percent=step_percent,
            )

        raise ValueError(f"Unknown mode: {mode}")