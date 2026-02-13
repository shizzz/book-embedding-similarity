from app.models import Feedback, Feedbacks, FeedbackReq

class FeedbackRepository:
    GET_QUERY: str = """
                SELECT 
                    source_book_id,
                    candidate_book_id,
                    label,
                    created_at
                FROM feedback
                """
    
    def _map(self, row) -> Feedback:
        return Feedback(
            source_id=row["source_book_id"],
            candidate_id=row["candidate_book_id"],
            label=row["label"]
        )
    
    def submit(self, conn, source_book_id: int, candidate_book_id: int, label: int):
        conn.execute(
            """
            INSERT OR REPLACE INTO feedback
            (source_book_id, candidate_book_id, label)
            VALUES (?, ?, ?)
            """,
            (source_book_id, candidate_book_id, label)
        )

    def get(self, conn, book_id: int) -> Feedbacks:
        rows = conn.execute(
            self.GET_QUERY + " WHERE source_book_id = ?",
            (book_id,)
        ).fetchall()

        return Feedbacks([self._map(r) for r in rows if r])

    def delete(self, conn, book_id: int, similar_book_id: int):
        conn.execute(f"DELETE FROM feedback WHERE source_book_id = ? AND candidate_book_id = ?", (book_id, similar_book_id, ))

    def get_all(self, conn) -> Feedbacks:
        rows = conn.execute(self.GET_QUERY).fetchall()

        return Feedbacks([self._map(r) for r in rows if r])
