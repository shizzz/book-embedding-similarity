from typing import List, Tuple
from app.models import Book, Embedding, Feedback

class SimilarSearchEngine:
    def __init__(self, exclude_same_authors: bool):
        self._exclude_same_authors = exclude_same_authors
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

    def search(
        self,
        source: Book,
        embedding: Embedding,
        feedbacks: List[Feedback],
        progress_callback=None
    ) -> List[Tuple[float, int, int]]:
        raise NotImplementedError()

