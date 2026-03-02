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

    def get_all_batch(self, batch_size: int = 1):
        with self.router.embeddings(self.model_uid) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM embeddings")
            total = cursor.fetchone()[0]

            for offset in range(0, total, batch_size):
                cursor.execute(
                    """
                    SELECT id, book_id, chunk_id, data, shape
                    FROM embeddings
                    LIMIT ? OFFSET ?
                    """,
                    (batch_size, offset)
                )
                rows = cursor.fetchall()
                if not rows:
                    break
                yield [Embedding.from_row(r) for r in rows]

    def _save_bulk(self, conn, embeddings: list[Embedding]):
        conn.executemany(
            """
            INSERT OR REPLACE INTO embeddings
            (id, book_id, chunk_id, data, shape)
            VALUES (?,?,?,?,?)
            """,
            [e.to_tuple() for e in embeddings]
        )
        
    def save_bulk(self, embeddings: list[Embedding], conn=None):
        if conn is None:
            with self.router.embeddings(self.model_uid) as conn:
                self._save_bulk(conn, embeddings)
        else:
            self._save_bulk(conn, embeddings)


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

    def meta_only(self, book_id: int = None) -> tuple[int, int, int]:
        with self.router.embeddings(self.model_uid) as conn:
            query = "SELECT id, book_id, chunk_id, NULL AS data, shape FROM embeddings"
            params = ()

            if book_id is not None:
                query += " WHERE book_id = ?"
                params = (int(book_id),)

            cursor = conn.execute(query, params).fetchall()
            return [Embedding.from_row(r) for r in cursor]
        
    def get_max_id(self) -> int:
        with self.router.embeddings(self.model_uid) as conn:
            cursor = conn.execute("SELECT seq FROM sqlite_sequence WHERE name = 'embeddings'")
            row = cursor.fetchone()
            if row is None:
                start_id = 1
            else:
                start_id = row["seq"] + 1
            return start_id