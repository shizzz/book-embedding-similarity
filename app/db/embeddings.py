from typing import Iterator, Tuple

class EmbeddingsRepository:
    GET_QUERY: str = "SELECT book_id, embedding FROM embeddings"

    @staticmethod
    def get(conn, book_id: int) -> bytes | None:
        row = conn.execute(f"{EmbeddingsRepository.GET_QUERY} WHERE book_id = ?", (book_id,)).fetchone()
        return row[1] if row else None

    @staticmethod
    def get_all(conn) -> Iterator[Tuple[int, bytes]]:
        cursor = conn.execute(f"{EmbeddingsRepository.GET_QUERY} ORDER BY book_id ASC")
        for row in cursor:
            yield (row["book_id"], row["embedding"])

    @staticmethod
    def save(conn, book_id: int, embedding: bytes):
        conn.execute(
            "INSERT OR REPLACE INTO embeddings(book_id, embedding) VALUES (?, ?)",
            (book_id, embedding)
        )

    @staticmethod
    def save_bulk(conn, books: list, embeddings_db: list[bytes]):

        conn.executemany(
            """
            INSERT OR REPLACE INTO embeddings
            (book_id, embedding)
            VALUES (?, ?)
            """,
            [
                (book.id, emb)
                for book, emb in zip(books, embeddings_db)
            ]
        )

    @staticmethod
    def count(conn) -> int:
        return conn.execute("SELECT COUNT(*) FROM embeddings").fetchone()[0]