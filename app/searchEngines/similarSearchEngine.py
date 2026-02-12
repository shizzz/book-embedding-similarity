import numpy as np
from typing import List, Tuple
from app.hnsw.rerankers import Reranker
from app.models import Book, Embedding

class SimilarSearchEngine:
    def __init__(self, exclude_same_authors: bool, reranker: Reranker = None):
        self._exclude_same_authors = exclude_same_authors
        self._reranker = reranker
        self._seen_titles = set()

    def _should_skip(self, source: Book, candidate_name: str, candidate_title: str) -> bool:
        if source.file_name is not None and source.file_name == candidate_name:
            return True

        if source.title is not None and source.title == candidate_title:
            return True

        if candidate_title in self._seen_titles:
            return True

        # if self.exclude_same_authors and source.authors and candidate.authors:
        #     if set(source.authors) & set(candidate.authors):
        #         return True
        
        self._seen_titles.add(candidate_title)

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

