from typing import List
from app.models import Similar, Book

class SimilarRepository:
    DELETE_QUERY: str = "DELETE FROM similar"

    def save(self, conn, similars: List[Similar]):
        cur = conn.cursor()
        cur.executemany(
            "INSERT INTO similar (book_id, similar_book_id, score) VALUES (?, ?, ?)",
            [
                (similar.book_id, similar.similar_book_id, float(similar.score))
                for similar in similars
            ]
        )

    def replace(self, conn, similars: List[Similar]):
        if len(similars) == 0:
            return
        
        self.delete_many(conn, similars)
        self.save(conn, similars)

    def get(self, conn, book_id: int, limit: int) -> List[Similar]:
        rows = conn.execute(
            """
            SELECT
                s.score,
                bs.archive, bs.book, bs.title,
                bt.archive, bt.book, bt.title
            FROM similar s
            JOIN books bs ON bs.id = s.book_id
            JOIN books bt ON bt.id = s.similar_book_id
            WHERE s.book_id = ?
            ORDER BY s.score DESC
            LIMIT ?
            """,
            (book_id, limit)
        ).fetchall()

        return [
            Similar.from_books(
                score,
                Book(archive_name=sa, file_name=sb, title=st),
                Book(archive_name=ta, file_name=tb, title=tt),
            )
            for score, sa, sb, st, ta, tb, tt in rows
        ]
    
    def clear(self, conn):
        conn.execute(f"{self.DELETE_QUERY}")
    
    def delete(self, conn, book_id: int):
        conn.execute(f"{self.DELETE_QUERY} WHERE book_id = ?", book_id)

    def delete_many(self, conn, similars: List[Similar]):
        books = list({s.book_id for s in similars})
        placeholders = ",".join("?" * len(books))

        conn.execute(f"{self.DELETE_QUERY} WHERE book_id IN ({placeholders})", books)
        

    def has_similar(self, conn, book_id: int) -> bool:
        return conn.execute(
            "SELECT 1 FROM similar WHERE book_id = ? LIMIT 1",
            (book_id,)
        ).fetchone() is not None
