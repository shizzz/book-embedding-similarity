import numpy as np
from typing import Generator
from ...models.embedding import Embedding
from ...models.chunk import ChunkType
from ..router import DBRouter
from .model import ModelRepository
from app.settings import ProcessConfig

GET_QUERY: str = "SELECT id, book_id, chunk_id, seq, data, shape, type FROM embeddings"
GET_QUERY_META: str = "SELECT id, book_id, chunk_id, seq, shape, type FROM embeddings"

class EmbeddingsRepository:
    def __init__(self, router: DBRouter, model_uid: str = None):
        self.router = router
        self.model_uid = model_uid or ModelRepository(router).get_latest_uid(ProcessConfig.MODEL_NAME)

    def get(self, book_id: int) -> list[Embedding]:
        with self.router.embeddings(self.model_uid) as conn:
            rows = conn.execute("SELECT * FROM embeddings WHERE book_id = ?", (book_id,)).fetchall()
            return [Embedding.from_row(r) for r in rows]
        
    def get_shape(self) -> int:
        with self.router.embeddings(self.model_uid) as conn:
            row = conn.execute("SELECT shape FROM embeddings LIMIT 1").fetchone()
            if row is None:
                raise RuntimeError("Нет данных для создания индекса")
            return row["shape"]

    def get_ids(self) -> set[int]:
        with self.router.embeddings(self.model_uid) as conn:
            rows = conn.execute("SELECT DISTINCT chunk_id FROM embeddings").fetchall()
            return {r[0] for r in rows}
        
    def get_all(self, embedding_type: int = None, batch_size: int = 100) -> Generator[list[Embedding], None, None]:
        query = GET_QUERY
        params = ()
        if embedding_type is not None:
            query += " WHERE [type] = ?"
            params = (embedding_type,)

        with self.router.embeddings(self.model_uid) as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)

            while True:
                rows = cursor.fetchmany(batch_size)  # берём пачку
                if not rows:
                    break
                yield [Embedding.from_row(row) for row in rows]  # отдаём пачкой

    def get_all_batch(self, batch_size: int = 1, order_by: list[str] = None):
        order_by = order_by or []
        order_clause = ""
        if order_by:
            # Sanitize column names if necessary to prevent SQL injection
            order_clause = " ORDER BY " + ", ".join(order_by)

        with self.router.embeddings(self.model_uid) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM embeddings")
            total = cursor.fetchone()[0]

            for offset in range(0, total, batch_size):
                cursor.execute(
                    f"{GET_QUERY} WHERE [type] == 2 {order_clause} LIMIT ? OFFSET ?",
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
        """
        Возвращает:
            embedding_id -> (vector, book_id, type)
        """
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
            for embedding_id, book_id, chunk_id, seq, data, shape, type in rows
        }

    def get_by_ids_meta(
        self,
        embedding_ids: list[int] = None
    ) -> dict[int, tuple[None, int, int]]:
        """
        Возвращает:
            embedding_id -> (vector, book_id, type)
        """
        if embedding_ids == []:
            return {}

        with self.router.embeddings(self.model_uid) as conn:
            if embedding_ids is None:
                query = GET_QUERY_META
                params = ()
            else:
                placeholders = ",".join("?" for _ in embedding_ids)
                query = f"{GET_QUERY_META} WHERE id IN ({placeholders}) "
                params = embedding_ids

            rows = conn.execute(query, params).fetchall()

        return {
            embedding_id: (None, book_id, type)
            for embedding_id, book_id, chunk_id, seq, shape, type in rows
        }

    def get_by_book_ids(
        self,
        book_ids: list[int],
        type: ChunkType = None
    ) -> dict[int, tuple[np.ndarray, int, int]]:
        """
        Возвращает:
            embedding_id -> (vector, book_id, type)
        """
        if not book_ids:
            return {}

        params = []

        if type is None:
            where = ""
        else:
            where = f" [type] == ? AND "
            params.append(type)

        if book_ids is None:
            query = f"{GET_QUERY} WHERE {where}"
        else:
            placeholders = ",".join("?" for _ in book_ids)
            query = f"{GET_QUERY} WHERE {where} book_id IN ({placeholders}) "
            params.extend(book_ids)

        with self.router.embeddings(self.model_uid) as conn:
            rows = conn.execute(query, params).fetchall()

        result: dict[int, tuple[np.ndarray, int]] = {}

        for embedding_id, book_id, chunk_id, seq, data, shape, type in rows:
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
            return conn.execute("SELECT COUNT(*) FROM embeddings WHERE [type] == 2").fetchone()[0]
        
    def get_max_id(self) -> int:
        with self.router.embeddings(self.model_uid) as conn:
            cursor = conn.execute("SELECT seq FROM sqlite_sequence WHERE name = 'embeddings'")
            row = cursor.fetchone()
            if row is None:
                start_id = 1
            else:
                start_id = row["seq"] + 1
            return start_id