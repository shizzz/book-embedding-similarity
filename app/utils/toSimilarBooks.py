from typing import Tuple, List
from app.db import DBRouter
from app.db.repositories import BookRepository
from app.models import Book, Similar

def to_similar_list(
    rows: List[Tuple[float, int, int]]
) -> List["Similar"]:
    if not rows:
        return []

    book_ids: set[int] = set[int]()
    for _, source_id, candidate_id in rows:
        book_ids.add(source_id)
        book_ids.add(candidate_id)

    router = DBRouter()
    books_by_id = BookRepository(router).get_many(list[int](book_ids))

    result: List[Similar] = []
    for score, source_id, candidate_id in rows:
        result.append(
            Similar(
                score=score,
                book_id=source_id,
                similar_book_id=candidate_id,
                source=books_by_id.get(source_id),
                candidate=books_by_id.get(candidate_id),
            )
        )

    return result