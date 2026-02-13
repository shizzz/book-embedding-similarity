import numpy as np
from typing import List, Tuple
from app.hnsw.rerankers import Reranker
from app.models import Book, Embedding

class SimilarSearchEngine:
    def __init__(self, exclude_same_authors: bool, reranker: Reranker = None):
        self._exclude_same_authors = exclude_same_authors
        self._reranker = reranker

    def _should_skip(
            self,
            source: Book,
            candidate_name: str,
            candidate_title: str,
            seen: set[tuple[str, tuple[str, ...]]],
            candidate_authors: list[str] = None,
        ) -> bool:
        if source.file_name is not None and source.file_name == candidate_name:
            return True

        if source.title is not None and source.title == candidate_title:
            return True

        key = (
            candidate_title,
            tuple(sorted(candidate_authors)) if candidate_authors else ()
        )

        if key in seen:
            return True
        
        seen.add(key)

        return False

    def _rerank(
        self,
        source: Book,
        candidates: list[tuple[float, Book]],
    ):
        if not self._reranker or not self._reranker.model:
            reranked = candidates
        else:
            X = []
            valid = []

            for sim, book in candidates:
                X.append([
                    sim,
                    int(book.author == source.author),
                ])
                valid.append(book)

            scores = self._reranker.predict(np.array(X))
            reranked = list(zip(scores, valid))
            
        reranked.sort(key=lambda x: x[0], reverse=True)
        return reranked
    
    def search(
        self,
        source: Book,
        embedding: Embedding,
        progress_callback=None
    ) -> List[Tuple[float, int, int]]:
        raise NotImplementedError()

