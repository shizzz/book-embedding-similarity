from app.db import DBRouter, SQLiteAdapters
from ...models.chunk import Chunk

class ChunkRepository:
    def __init__(self, router: DBRouter):
        self.router = router

    def create(self, book_id: int, text: str) -> int:
        with self.router.meta() as conn:
            cursor = conn.execute(
                """
                INSERT INTO chunks(book_id)
                VALUES(?)
                """,
                (book_id,)
            )

            chunk_id = cursor.lastrowid

        with self.router.chunks() as conn:
            conn.execute(
                """
                INSERT INTO chunks(
                    id,
                    data,
                    length
                )
                VALUES(?,?,?)
                """,
                (
                    chunk_id,
                    text,
                    len(text)
                )
            )

        return chunk_id

    def create_many(
        self,
        chunks: list[Chunk]
    ) -> None:
        """
        chunks: [(chunk_id, text), ...]
        """

        with self.router.meta() as conn:
            conn.executemany(
                """
                INSERT INTO chunks(
                    id,
                    book_id
                )
                VALUES(?,?)
                """,
                [e.to_tuple_meta() for e in chunks]
            )

        with self.router.chunks() as conn:
            conn.executemany(
                """
                INSERT INTO chunks(
                    id,
                    book_id,
                    data,
                    length
                )
                VALUES(?,?,?,?)
                """,
                [e.to_tuple_chunks() for e in chunks]
            )

    def get_by_book(self, book_id: int) -> None:
        with self.router.chunks() as conn:
            rows = conn.execute(
                """
                SELECT id, book_id, data, length
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

    def get_max_id(self) -> int:
        with self.router.meta() as conn:
            cursor = conn.execute("SELECT seq FROM sqlite_sequence WHERE name = 'chunks'")
            row = cursor.fetchone()
            if row is None:
                start_id = 1
            else:
                start_id = row["seq"] + 1
            return start_id