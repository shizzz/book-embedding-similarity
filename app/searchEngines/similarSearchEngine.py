from typing import List, Tuple, Literal
from app.models import Book, Embedding, Feedback

class SimilarSearchEngine:
    INDEX = "index" 
    BRUTEFORCE = "bruteforce"
    
    EngineType = Literal[INDEX, BRUTEFORCE]

    def __init__(self, exclude_same_authors: bool):
        self._exclude_same_authors = exclude_same_authors

    def _should_skip(self, source: Book, candidate_name: str, candidate_title: str) -> bool:
        if source.file_name is not None and source.file_name == candidate_name:
            return True

        if source.title is not None and source.title == candidate_title:
            return True

        # if self.exclude_same_authors and source.authors and candidate.authors:
        #     if set(source.authors) & set(candidate.authors):
        #         return True

        return False

    def search(
        self,
        source: Book,
        embedding: Embedding,
        feedbacks: List[Feedback],
        progress_callback=None
    ) -> List[Tuple[float, int, int]]:
        raise NotImplementedError()

