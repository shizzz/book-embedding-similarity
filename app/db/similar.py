from typing import Any, List, Tuple

class SimilarRepository:
    DELETE_QUERY: str = "DELETE FROM similar"

    def save(self, conn, similars: List[Tuple[float, int, int]]):
        cur = conn.cursor()
        cur.executemany(
            "INSERT INTO similar (book_id, similar_book_id, score) VALUES (?, ?, ?)",
            [
                (similar[1], similar[2], float(similar[0]))
                for similar in similars
            ]
        )

    def replace(self, conn, similars: List[Tuple[float, int, int]]):
        if len(similars) == 0:
            return
        
        self.delete_many(conn, similars)
        self.save(conn, similars)

    def get(self, conn, book_id: int, limit: int) -> List[Tuple[float, int, int]]:
        cursor = conn.execute(
            """
            SELECT
                score, 
                book_id as source_id,
                similar_book_id as similar_book_id
            FROM similar
            WHERE book_id = ?
            ORDER BY score DESC
            LIMIT ?
            """,
            (book_id, limit)
        )

        return [
            (row["score"], row["source_id"], row["similar_book_id"])
            for row in cursor
        ]
    
    def clear(self, conn):
        conn.execute(f"{self.DELETE_QUERY}")
    
    def delete(self, conn, book_id: int, similar_book_id: int):
        conn.execute(f"{self.DELETE_QUERY} WHERE book_id = ? AND similar_book_id = ?", (book_id, similar_book_id, ))

    def delete_many(self, conn, similars: List[Tuple[float, int, int]]):
        books = list[int]({s[1] for s in similars})
        placeholders = ",".join("?" * len(books))

        conn.execute(f"{self.DELETE_QUERY} WHERE book_id IN ({placeholders})", books)
        
    def has_similar(self, conn, book_id: int) -> bool:
        return conn.execute(
            "SELECT 1 FROM similar WHERE book_id = ? LIMIT 1",
            (book_id,)
        ).fetchone() is not None
