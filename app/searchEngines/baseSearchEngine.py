from typing import List
from app.models import Book, Similar

class BaseSearchEngine:
    def __init__(self, limit: int, exclude_same_authors: bool = False):
        self.limit = limit
        self.exclude_same_authors = exclude_same_authors

    def _should_skip(self, source: Book, candidate: Book) -> bool:
        if source.title is not None and candidate.title is not None and source.title == candidate.title:
            return True

        if self.exclude_same_authors and source.authors and candidate.authors:
            if set(source.authors) & set(candidate.authors):
                return True

        return False

    def search(self, *args, **kwargs) -> List[Similar]:
        raise NotImplementedError()

