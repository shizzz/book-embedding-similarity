import numpy as np
from typing import Iterator, Tuple
from ...models.embedding import Embedding
from ..router import DBRouter

class EmbeddingsRepository:
    GET_QUERY: str = "SELECT book_id, embedding FROM embeddings"

    def __init__(self, router: DBRouter, model_uid: str):
        self.router = router
        self.model_uid = model_uid

    def get(self, book_id: int) -> list[Embedding]:
        with self.router.embeddings(self.model_uid) as conn:
            rows = conn.execute("SELECT * FROM embeddings WHERE book_id = ?", (book_id,)).fetchall()
            return [Embedding.from_row(r) for r in rows]

    def get_all(self)-> Iterator[Tuple[int, np.ndarray]]:
        with self.router.embeddings(self.model_uid) as conn:
            cursor = conn.execute(f"{EmbeddingsRepository.GET_QUERY} ORDER BY book_id ASC")
            for row in cursor:
                yield (row["book_id"], row["embedding"])

    def save(
            self,
            book_id: int,
            embedding: np.ndarray,
            model: str,
            source_length: int = None,
            source_chunk_length: int = None,
            shape: int = None
    ):
        with self.router.embeddings(self.model_uid) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO embeddings
                (book_id, embedding, model, source_length, source_chunk_length, shape)
                VALUES (?, ?, ?, ?, ?)
                """,
                (book_id, embedding, model, source_length, source_chunk_length, shape)
            )

    def save_bulk(self, embeddings: list[Embedding]):
        with self.router.embeddings(self.model_uid) as conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO embeddings
                (id, book_id, chunk_id, data, shape)
                VALUES (?,?,?,?,?)
                """,
                [e.to_tuple() for e in embeddings]
            )

    def update(self, book_id: int, embedding: np.ndarray):
        with self.router.embeddings(self.model_uid) as conn:
            conn.execute(
                "UPDATE embeddings SET embedding = ? WHERE book_id = ?",
                (embedding, book_id)
            )

    def count(self) -> int:
        with self.router.embeddings(self.model_uid) as conn:
            return conn.execute("SELECT COUNT(*) FROM embeddings").fetchone()[0]
    
    def delete(self, to_delete: list[int]) -> None:
        with self.router.embeddings(self.model_uid) as conn:
            query = f"DELETE FROM embeddings WHERE book_id IN ({','.join(['?']*len(to_delete))})"
            conn.execute(query, to_delete)

    def get_max_id(self) -> int:
        with self.router.embeddings(self.model_uid) as conn:
            cursor = conn.execute("SELECT seq FROM sqlite_sequence WHERE name = 'embeddings'")
            row = cursor.fetchone()
            if row is None:
                start_id = 1
            else:
                start_id = row["seq"] + 1
            return start_id