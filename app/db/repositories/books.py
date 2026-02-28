from datetime import datetime
from typing import Any, Generator, Tuple
import numpy as np
from ..router import DBRouter
from ...models.book import BookRegistry, Book

class BookRepository:
    GET_QUERY = """
    SELECT
        b.id,
        b.book,
        b.title,
        b.author,
        b.source_type,
        b.source_link
    FROM books b
    """

    GET_FULL_QUERY = """
    SELECT
        b.id,
        b.book,
        b.title,
        b.author,
        b.serie,
        b.generes,
        b.year,
        b.source_type,
        b.source_link,
        b.source_length
    FROM books b
    """
    
    def __init__(self, router: DBRouter):
        self.router = router

    def get_all(self) -> Any:
        with self.router.meta() as conn:
            cursor = conn.execute(f"{BookRepository.GET_QUERY}")
            for row in cursor:
                yield (tuple[Any, ...](row))

    def get_all_with_embeddings(self) -> Generator[Tuple[int, str, str, str, str, str, np.ndarray]]:
        with self.router.meta() as conn:
            cursor = conn.execute("""
            SELECT
                b.id,
                b.book,
                b.title,
                b.author,
                b.source_type,
                b.source_link
            FROM books b
            ORDER BY b.id ASC
            """)
            for row in cursor:
                yield (tuple[Any, ...](row))

    def get_by_file(self, book: str) -> Any:
        with self.router.meta() as conn:
            row = conn.execute(f"{BookRepository.GET_QUERY} WHERE b.book = ?",(book,)).fetchone()
            return row if row else None

    def get_full_by_file(self, book: str) -> Any:
        with self.router.meta() as conn:
            row = conn.execute(f"{BookRepository.GET_FULL_QUERY} WHERE b.book = ?",(book,)).fetchone()
            return row if row else None
    
    def get_by_id(self, book_id: int) -> Any:
        with self.router.meta() as conn:
            row = conn.execute(f"{BookRepository.GET_QUERY} WHERE b.id = ?",(book_id,)).fetchone()
            return row if row else None
    
    def get_many(self, book_ids: list[int]) -> dict[int, Any]:
        with self.router.meta() as conn:
            if not book_ids:
                return {}

            placeholders = ",".join("?" for _ in book_ids)
            rows = conn.execute(f"{BookRepository.GET_QUERY} WHERE id IN ({placeholders})",book_ids).fetchall()

            return {
                row["id"]: row
                for row in rows
            }

    def get_names(self) -> list[str]:
        with self.router.meta() as conn:
            rows = conn.execute("SELECT book FROM books").fetchall()
            return [row[0] for row in rows]
    
    def embeddings_cursor(self):
        with self.router.meta() as conn:
            embeddings_cursor = conn.cursor()
            embeddings_cursor.execute(BookRepository.GET_QUERY + " GROUP BY b.book")
            return embeddings_cursor

    def save(
            self,
            book: str,
            uid: str,
            title: str,
            author: str,
            source_type: str,
            source_link: str
        ) -> int | None:
        with self.router.meta() as conn:
            cursor = conn.execute(
                """
                INSERT INTO books (book, uid, title, author, added_at, source_type, source_link)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    book,
                    uid,
                    title,
                    author,
                    datetime.now().isoformat(),
                    source_type,
                    source_link,
                )
            )

            return cursor.lastrowid

    def save_bulk(self, books: BookRegistry):
        if not books:
            return []

        with self.router.meta() as conn:
            now = datetime.now().isoformat()
            cursor = conn.cursor()

            cursor.executemany(
                """
                INSERT OR REPLACE INTO books (id, book, uid, title, author, serie, generes, year, added_at, source_type, source_link, source_length)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        book.id,
                        book.file_name,
                        book.uid,
                        book.title,
                        book.author,
                        book.serie,
                        "||".join(book.generes),
                        book.year,
                        now,
                        book.source_type,
                        book.source_link,
                        book.source_length,
                    )
                    for book in books
                ]
            )
        
    def get_max_id(self) -> int:
        with self.router.meta() as conn:
            cursor = conn.execute("SELECT seq FROM sqlite_sequence WHERE name = 'books'")
            row = cursor.fetchone()
            if row is None:
                start_id = 1
            else:
                start_id = row["seq"] + 1
            return start_id

    def count_embeddings(self) -> int:
        with self.router.meta() as conn:
            return conn.execute("SELECT COUNT(*) FROM embeddings").fetchone()[0]
