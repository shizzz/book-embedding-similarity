from sqlite3 import Row

class FeedbackRepository:
    GET_QUERY: str = """
                SELECT 
                    source_book_id,
                    candidate_book_id,
                    label,
                    created_at
                FROM feedback
                """
    DEL_QUERY: str = "DELETE FROM feedback"
    
    def submit(conn, source_book_id: int, candidate_book_id: int, label: int):
        conn.execute(
            """
            INSERT OR REPLACE INTO feedback
            (source_book_id, candidate_book_id, label)
            VALUES (?, ?, ?)
            """,
            (source_book_id, candidate_book_id, label)
        )

    def get(conn, book_id: int) -> list[Row]:
        return conn.execute(
            FeedbackRepository.GET_QUERY + " WHERE source_book_id = ?",
            (book_id,)
        ).fetchall()

    def get_all(conn) -> list[Row]:
        return conn.execute(FeedbackRepository.GET_QUERY).fetchall()

    def delete(conn, book_id: int, similar_book_id: int):
        conn.execute(f"{FeedbackRepository.DEL_QUERY} WHERE source_book_id = ? AND candidate_book_id = ?", (book_id, similar_book_id, ))
