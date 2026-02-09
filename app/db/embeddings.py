from typing import Iterator, Tuple

class EmbeddingsRepository:
    GET_QUERY: str = "SELECT book_id, embedding FROM embeddings"

    def get(self, conn, book_id: int) -> bytes | None:
        row = conn.execute(f"{self.GET_QUERY} WHERE book_id = ?", (book_id,)).fetchone()
        return row[1] if row else None

    def get_all(self, conn) -> Iterator[Tuple[int, bytes]]:
        cursor = conn.execute(f"{self.GET_QUERY} ORDER BY book_id ASC")
        for row in cursor:
            yield (row["book_id"], row["embedding"])

    def save(conn, book_id: int, embedding: bytes):
        conn.execute(
            "INSERT OR REPLACE INTO embeddings(book_id, embedding) VALUES (?, ?)",
            (book_id, embedding)
        )

    def count(self, conn) -> int:
        return conn.execute("SELECT COUNT(*) FROM embeddings").fetchone()[0]