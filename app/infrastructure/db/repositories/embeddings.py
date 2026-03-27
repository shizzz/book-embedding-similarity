import numpy as np
from typing import Generator
from collections.abc import Iterable
from ...models.embedding import Embedding
from ...models.chunk import ChunkType
from ..router import DBRouter
from .model import ModelRepository
from app.settings import ProcessConfig

GET_QUERY: str = "SELECT id, source_id, chunk_id, seq, data, shape, type FROM embeddings"
GET_QUERY_META: str = "SELECT id, source_id, chunk_id, seq, shape, type FROM embeddings"

class EmbeddingsRepository:
    def __init__(self, router: DBRouter, model_uid: str = None):
        self.router = router
        self.model_uid = model_uid or ModelRepository(router).get_latest_uid(ProcessConfig.MODEL_NAME)[0]

    def get(self, source_id: int) -> list[Embedding]:
        with self.router.embeddings(self.model_uid) as conn:
            rows = conn.execute("SELECT * FROM embeddings WHERE source_id = ?", (source_id,)).fetchall()
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
        
    def get_all(
        self,
        embedding_type: ChunkType | Iterable[ChunkType] | None = None,
        batch_size: int = 100,
        order_by: list[str] | None = None,
    ) -> Generator[list[Embedding], None, None]:
        order_by = order_by or []

        query = GET_QUERY
        params: list = []

        # WHERE
        if embedding_type is not None:
            if isinstance(embedding_type, Iterable) and not isinstance(embedding_type, (str, bytes)):
                types = list(embedding_type)
                if types:  # не пустой список
                    placeholders = ", ".join(["?"] * len(types))
                    query += f" WHERE [type] IN ({placeholders})"
                    params.extend(types)
                else:
                    # пустой список → ничего не вернётся
                    query += " WHERE 1 = 0"
            else:
                query += " WHERE [type] = ?"
                params.append(embedding_type)

        # ORDER BY
        if order_by:
            query += " ORDER BY " + ", ".join(order_by)

        with self.router.embeddings(self.model_uid) as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)

            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                yield [Embedding.from_row(row) for row in rows]

    def get_by_ids(
        self,
        embedding_ids: list[int] = None
    ) -> dict[int, tuple[np.ndarray, int, int]]:
        """
        Возвращает:
            embedding_id -> (vector, source_id, type)
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
                source_id,
                type
            )
            for embedding_id, source_id, chunk_id, seq, data, shape, type in rows
        }

    def get_by_ids_meta(
        self,
        embedding_ids: list[int] = None
    ) -> dict[int, tuple[None, int, int]]:
        """
        Возвращает:
            embedding_id -> (vector, source_id, type)
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
            embedding_id: (None, source_id, type)
            for embedding_id, source_id, chunk_id, seq, shape, type in rows
        }

    def get_by_source_ids(
        self,
        source_ids: list[int],
        type: ChunkType = None
    ) -> dict[int, tuple[np.ndarray, int, int]]:
        """
        Возвращает:
            embedding_id -> (vector, source_id, type)
        """
        if not source_ids:
            return {}

        params = []

        if type is None:
            where = ""
        else:
            where = f" [type] == ? AND "
            params.append(type)

        if source_ids is None:
            query = f"{GET_QUERY} WHERE {where}"
        else:
            placeholders = ",".join("?" for _ in source_ids)
            query = f"{GET_QUERY} WHERE {where} source_id IN ({placeholders}) "
            params.extend(source_ids)

        with self.router.embeddings(self.model_uid) as conn:
            rows = conn.execute(query, params).fetchall()

        result: dict[int, tuple[np.ndarray, int]] = {}

        for embedding_id, source_id, chunk_id, seq, data, shape, type in rows:
            vec = data
            if shape:
                vec = vec.reshape((shape,))

            result[embedding_id] = (vec, source_id, type)

        return result

    def _save_bulk(self, conn, embeddings: list[Embedding]):
        conn.executemany(
            """
            INSERT OR REPLACE INTO embeddings
            (id, source_id, chunk_id, seq, data, shape, type, name)
            VALUES (?,?,?,?,?,?,?,?)
            """,
            [e.to_tuple() for e in embeddings]
        )
        
    def save_bulk(self, embeddings: list[Embedding], conn=None):
        if conn is None:
            with self.router.embeddings(self.model_uid) as conn:
                self._save_bulk(conn, embeddings)
        else:
            self._save_bulk(conn, embeddings)

    def update(self, source_id: int, embedding: np.ndarray):
        with self.router.embeddings(self.model_uid) as conn:
            conn.execute(
                "UPDATE embeddings SET embedding = ? WHERE source_id = ?",
                (embedding, source_id)
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
        
    # ------------------------
    # DELETE
    # ------------------------
    def delete_by_type(self, type: ChunkType) -> None:
        with self.router.embeddings(self.model_uid) as conn:
            conn.execute(f"DELETE FROM embeddings WHERE type = ?", (type,))