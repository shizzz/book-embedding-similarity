from sqlite3 import Row

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
    
    @staticmethod
    def get(conn, book_id: int) -> list[Row]:
        return conn.execute(
            FeedbackRepository.GET_QUERY + " WHERE source_book_id = ?",
            (book_id,)
        ).fetchall()

    @staticmethod
    def get_all(conn) -> list[Row]:
        return conn.execute(FeedbackRepository.GET_QUERY).fetchall()

    @staticmethod
    def submit(conn, source_book_id: int, candidate_book_id: int, label: int):
        conn.execute(
            """
            INSERT OR REPLACE INTO feedback
            (source_book_id, candidate_book_id, label)
            VALUES (?, ?, ?)
            """,
            (source_book_id, candidate_book_id, label)
        )

    @staticmethod
    def insert_many(conn, rows: list[tuple]):
        conn.executemany(
            FeedbackRepository.INSERT_QUERY,
            rows
        )

    @staticmethod
    def delete_all(conn):
        conn.execute(FeedbackRepository.DEL_QUERY)

    @staticmethod
    def delete(conn, book_id: int, similar_book_id: int):
        conn.execute(f"{FeedbackRepository.DEL_QUERY} WHERE source_book_id = ? AND candidate_book_id = ?", (book_id, similar_book_id, ))
