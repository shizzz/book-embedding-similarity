from sqlite3 import Row
from ..router import DBRouter

class FeedbackRepository:
    GET_QUERY: str = """
                SELECT
                    id,
                    source_book_id,
                    candidate_book_id,
                    label,
                    created_at
                FROM feedback
                """
    
    INSERT_QUERY = """
        INSERT INTO feedback (
            source_book_id,
            candidate_book_id,
            label,
            created_at
        )
        VALUES (?, ?, ?, ?)
    """
    DEL_QUERY: str = "DELETE FROM feedback"

    def __init__(self, router: DBRouter):
        self.router = router
    
    def get(self, book_id: int) -> list[Row]:
        with self.router.meta() as conn:
            return conn.execute(
                FeedbackRepository.GET_QUERY + " WHERE source_book_id = ?",
                (book_id,)
            ).fetchall()

    def get_all(self) -> list[Row]:
        with self.router.meta() as conn:
            return conn.execute(FeedbackRepository.GET_QUERY).fetchall()

    def submit(self, source_book_id: int, candidate_book_id: int, label: int):
        with self.router.meta() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO feedback
                (source_book_id, candidate_book_id, label)
                VALUES (?, ?, ?)
                """,
                (source_book_id, candidate_book_id, label)
            )

    def insert_many(self, rows: list[tuple]):
        with self.router.meta() as conn:
            conn.executemany(
                FeedbackRepository.INSERT_QUERY,
                rows
            )

    def delete_all(self):
        with self.router.meta() as conn:
            conn.execute(FeedbackRepository.DEL_QUERY)

    def delete(self, book_id: int, similar_book_id: int):
        with self.router.meta() as conn:
            conn.execute(f"{FeedbackRepository.DEL_QUERY} WHERE source_book_id = ? AND candidate_book_id = ?", (book_id, similar_book_id, ))
