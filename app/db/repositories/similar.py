from typing import List, Tuple
from ..router import DBRouter

class SimilarRepository:
    GET_QUERY: str = """
            SELECT
                score, 
                book_id as source_id,
                similar_book_id as similar_book_id
            FROM similar
    """
    DELETE_QUERY: str = "DELETE FROM similar"

    def __init__(self, router: DBRouter):
        self.router = router

    def save(self, similars: List[Tuple[float, int, int]]):
        with self.router.meta() as conn:
            cur = conn.cursor()
            cur.executemany(
                "INSERT INTO similar (book_id, similar_book_id, score) VALUES (?, ?, ?)",
                [
                    (similar[1], similar[2], float(similar[0]))
                    for similar in similars
                ]
            )

    def replace(self, similars: List[Tuple[float, int, int]]):
        if len(similars) == 0:
            return
        
        self.delete_many(similars)
        self.save(similars)

    def get(self, book_id: int, limit: int) -> List[Tuple[float, int, int]]:
        with self.router.meta() as conn:
            cursor = conn.execute(f"{SimilarRepository.GET_QUERY} WHERE book_id = ? ORDER BY score DESC LIMIT ?",(book_id, limit))

            return [
                (row["score"], row["source_id"], row["similar_book_id"])
                for row in cursor
            ]
    
    def get_score(self, book_id: int, candidate_id: int) -> float:
        with self.router.meta() as conn:
            row = conn.execute(
                f"{SimilarRepository.GET_QUERY} WHERE book_id = ? AND similar_book_id = ?",
                (book_id, candidate_id)
            ).fetchone()

            if row is None:
                return 0.0

            return float(row["score"])
    
    def clear(self):
        with self.router.meta() as conn:
            conn.execute(f"{SimilarRepository.DELETE_QUERY}")
    
    def delete(self, book_id: int, similar_book_id: int):
        with self.router.meta() as conn:
            conn.execute(f"{SimilarRepository.DELETE_QUERY} WHERE book_id = ? AND similar_book_id = ?", (book_id, similar_book_id, ))

    def delete_many(self, similars: List[Tuple[float, int, int]]):
        with self.router.meta() as conn:
            books = list[int]({s[1] for s in similars})
            placeholders = ",".join("?" * len(books))

            conn.execute(f"{SimilarRepository.DELETE_QUERY} WHERE book_id IN ({placeholders})", books)
      
    def has_similar(self, book_id: int) -> bool:
        with self.router.meta() as conn:
            return conn.execute(
                "SELECT 1 FROM similar WHERE book_id = ? LIMIT 1",
                (book_id,)
            ).fetchone() is not None
