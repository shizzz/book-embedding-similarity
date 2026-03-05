import numpy as np
from ...models.embedding import Embedding
from ..router import DBRouter
from .model import ModelRepository
from app.settings import ProcessConfig

GET_QUERY: str = "SELECT id, book_id, data, shape, type FROM embeddings"

class EmbeddingsRepository:
    def __init__(self, router: DBRouter, model_uid: str = None):
        self.router = router
        self.model_uid = model_uid or ModelRepository(router).get_latest_uid(ProcessConfig.MODEL_NAME)

    def get(self, book_id: int) -> list[Embedding]:
        with self.router.embeddings(self.model_uid) as conn:
            rows = conn.execute("SELECT * FROM embeddings WHERE book_id = ?", (book_id,)).fetchall()
            return [Embedding.from_row(r) for r in rows]

    def get_ids(self) -> set[int]:
        with self.router.embeddings(self.model_uid) as conn:
            rows = conn.execute("SELECT DISTINCT book_id FROM embeddings").fetchall()
            return [r[0] for r in rows]

    def get_all_batch(self, batch_size: int = 1):
        with self.router.embeddings(self.model_uid) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM embeddings")
            total = cursor.fetchone()[0]

            for offset in range(0, total, batch_size):
                cursor.execute(
                    f"{GET_QUERY} LIMIT ? OFFSET ?",
                    (batch_size, offset)
                )
                rows = cursor.fetchall()
                if not rows:
                    break
                yield [Embedding.from_row(r) for r in rows]

    def get_by_ids(
        self,
        embedding_ids: list[int] = None
    ) -> dict[int, tuple[np.ndarray, int, int]]:
        if embedding_ids == []:
            return {}

        with self.router.embeddings(self.model_uid) as conn:
            if embedding_ids is None:
                query = GET_QUERY
                params = ()
            else:
                placeholders = ",".join("?" for _ in embedding_ids)
                query = f"{GET_QUERY} WHERE id IN ({placeholders}) "
                params = embedding_ids

            rows = conn.execute(query, params).fetchall()

        return {
            embedding_id: (
                data.reshape((shape,)) if shape else data,
                book_id,
                type
            )
            for embedding_id, book_id, data, shape, type in rows
        }

    def get_by_book_ids(
        self,
        book_ids: list[int]
    ) -> dict[int, tuple[np.ndarray, int, int]]:
        """
        Возвращает:
            embedding_id -> (vector, book_id)
        """
        if not book_ids:
            return {}

        placeholders = ",".join("?" for _ in book_ids)

        with self.router.embeddings(self.model_uid) as conn:
            cursor = conn.execute(
                f"{GET_QUERY} WHERE book_id IN ({placeholders})",
                book_ids
            )
            rows = cursor.fetchall()

        result: dict[int, tuple[np.ndarray, int]] = {}

        for embedding_id, book_id, data, shape, type in rows:
            vec = data
            if shape:
                vec = vec.reshape((shape,))

            result[embedding_id] = (vec, book_id, type)

        return result

    def _save_bulk(self, conn, embeddings: list[Embedding]):
        conn.executemany(
            """
            INSERT OR REPLACE INTO embeddings
            (id, book_id, chunk_id, seq, data, shape, type)
            VALUES (?,?,?,?,?,?,?)
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

    def meta_only(self, book_id: int = None) -> Embedding:
        with self.router.embeddings(self.model_uid) as conn:
            query = "SELECT id, book_id, chunk_id, seq, NULL AS data, shape, type FROM embeddings"
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