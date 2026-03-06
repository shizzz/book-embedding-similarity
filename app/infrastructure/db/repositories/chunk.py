from typing import List
from app.infrastructure.db import DBRouter
from ...models.chunk import Chunk

class ChunkRepository:
    def __init__(self, router: DBRouter):
        self.router = router

    def get_ids(self) -> set[int]:
        with self.router.meta() as conn:
            rows = conn.execute("SELECT DISTINCT book_id FROM chunks").fetchall()
            return [r[0] for r in rows]
        
    def _create_many_meta(self, conn, chunks: list[Chunk]):
        conn.executemany(
            """
            INSERT INTO chunks(
                id,
                book_id,
                type
            )
            VALUES(?,?,?)
            """,
            [e.to_tuple_meta() for e in chunks]
        )

    def _create_many_chunks(self, conn, chunks: list[Chunk]):
            conn.executemany(
                """
                INSERT INTO chunks(
                    id,
                    book_id,
                    data,
                    length,
                    type
                )
                VALUES(?,?,?,?,?)
                """,
                [e.to_tuple_chunks() for e in chunks]
            )

    def create_many(
        self,
        chunks: list[Chunk],
        conn_meta=None,
        conn_chunks=None
    ) -> None:
        if conn_meta is None:
            with self.router.meta() as conn:
                self._create_many_meta(conn, chunks)
        else:
            self._create_many_meta(conn_meta, chunks)

        if conn_chunks is None:
            with self.router.chunks() as conn:
                self._create_many_chunks(conn, chunks)
        else:
            self._create_many_chunks(conn_chunks, chunks)

    def get_by_book(self, book_id: int) -> List[Chunk]:
        with self.router.chunks() as conn:
            rows = conn.execute(
                """
                SELECT id, book_id, data, length, type
                FROM chunks
                WHERE book_id = ?
                """,
                (book_id,)
            ).fetchall()

            return [Chunk.from_chunks_row(r) for r in rows]

    def get_text(self, chunk_id: int) -> str | None:
        with self.router.chunks() as conn:
            row = conn.execute(
                """
                SELECT data
                FROM chunks
                WHERE id = ?
                """,
                (chunk_id,)
            ).fetchone()

            return row["data"] if row else None

    def meta_only(self) -> tuple[int, int]:
        with self.router.chunks() as conn:
            rows = conn.execute("SELECT id, book_id, type FROM chunks").fetchall()
            return [(row["id"], row["book_id"], row["type"]) for row in rows]

    def get_max_id(self) -> int:
        with self.router.chunks() as conn:
            cursor = conn.execute("SELECT seq FROM sqlite_sequence WHERE name = 'chunks'")
            row = cursor.fetchone()
            if row is None:
                start_id = 1
            else:
                start_id = row["seq"] + 1
            return start_id