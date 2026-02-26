import numpy as np
from typing import Iterator, Tuple
from .connection import DB

class EmbeddingsRepository:
    GET_QUERY: str = "SELECT book_id, embedding FROM embeddings"

    @staticmethod
    def get(conn, book_id: int) -> np.ndarray | None:
        row = conn.execute(f"{EmbeddingsRepository.GET_QUERY} WHERE book_id = ?", (book_id,)).fetchone()
        return row["embedding"] if row else None

    @staticmethod
    def get_all(conn)-> Iterator[Tuple[int, np.ndarray]]:
        cursor = conn.execute(f"{EmbeddingsRepository.GET_QUERY} ORDER BY book_id ASC")
        for row in cursor:
            yield (row["book_id"], row["embedding"])

    def save(conn, book_id: int, embedding: np.ndarray, model: str,
             source_length: int = None, token_length: int = None):
        conn.execute(
            """
            INSERT OR REPLACE INTO embeddings
            (book_id, embedding, model, source_length, token_length)
            VALUES (?, ?, ?, ?, ?)
            """,
            (book_id, embedding, model, source_length, token_length)
        )

    @staticmethod
    def save_bulk(conn, books: list):
        conn.executemany(
            """
            INSERT OR REPLACE INTO embeddings
            (book_id, embedding, source_text, model, source_length, token_length)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                (book.id, book.embedding, DB.adapt_text(book.text), book.model_id, book.source_length, book.token_length)
                for book in books
            ]
        )

    @staticmethod
    def update(conn, book_id: int, embedding: np.ndarray):
        conn.execute(
            "UPDATE embeddings SET embedding = ? WHERE book_id = ?",
            (embedding, book_id)
        )

    @staticmethod
    def count(conn) -> int:
        return conn.execute("SELECT COUNT(*) FROM embeddings").fetchone()[0]
    
    @staticmethod
    def delete(conn, to_delete: list[int]) -> None:
        query = f"DELETE FROM embeddings WHERE book_id IN ({','.join(['?']*len(to_delete))})"
        conn.execute(query, to_delete)
