from datetime import datetime
from typing import Any
from ..router import DBRouter
from ...models.book import BookRegistry, Book

GET_QUERY = """
    SELECT
        b.id,
        b.book,
        b.title,
        b.author,
        b.source_type,
        b.source_link,
        b.empty
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
        b.source_length,
        b.text_length,
        b.empty
    FROM books b
    """

class BookRepository:
    def __init__(self, router: DBRouter):
        self.router = router

    def get_all(self, empty: bool = None) -> Any:
        with self.router.transaction():
            conn = self.router.meta()
            query = GET_QUERY
            params = ()

            if empty is not None:
                query += " WHERE b.empty = ?"
                params = (int(empty),)

            cursor = conn.execute(query, params)
            for row in cursor:
                yield tuple(row)

    async def load_books_after(self, last_id: int, limit: int) -> dict[str, int]:
        with self.router.meta() as conn:
            query = """
                SELECT id, title, path
                FROM books
                WHERE id > %s
                ORDER BY id
                LIMIT %s
            """

            rows = conn.execute("SELECT id, book FROM books").fetchall()
            return {row[1]: row[0] for row in rows}

    def get_by_ids(self, ids: list[int]) -> dict[int, dict]:
        if not ids:
            return {}

        placeholders = ",".join("?" for _ in ids)
        query = f"{GET_FULL_QUERY} WHERE id IN ({placeholders})"
        result = {}
        with self.router.meta() as conn:
            cursor = conn.execute(query, ids)
            for row in cursor:
                (
                    id_, book, title, author, serie, generes, year, source_type, source_link, source_length, text_length, empty
                ) = row
                result[id_] = {
                    "id": id_,
                    "book": book,
                    "title": title,
                    "author": author,
                    "serie": serie,
                    "generes": generes,
                    "year": year,
                    "source_type": source_type,
                    "source_link": source_link,
                    "source_length": source_length,
                    "text_length": text_length,
                    "empty": empty,
                }
        return result

    def get_by_file(self, book: str) -> Book:
        with self.router.meta() as conn:
            row = conn.execute(f"{GET_QUERY} WHERE b.book = ?",(book,)).fetchone()
            return Book.from_row(row) if row else None

    def get_full_by_file(self, book: str) -> Book:
        with self.router.meta() as conn:
            row = conn.execute(f"{GET_FULL_QUERY} WHERE b.book = ?",(book,)).fetchone()
            return Book.from_row(row) if row else None
    
    def get_by_id(self, book_id: int) -> Book:
        with self.router.meta() as conn:
            row = conn.execute(f"{GET_QUERY} WHERE b.id = ?",(book_id,)).fetchone()
            return Book.from_row(row) if row else None
    
    def get_many(self, book_ids: list[int]) -> dict[int, Book]:
        with self.router.meta() as conn:
            if not book_ids:
                return {}

            placeholders = ",".join("?" for _ in book_ids)
            rows = conn.execute(f"{GET_QUERY} WHERE id IN ({placeholders})",book_ids).fetchall()

            return {
                row["id"]: Book.from_row(row)
                for row in rows
            }

    def get_file_to_id(self) -> dict[str, int]:
        with self.router.meta() as conn:
            rows = conn.execute("SELECT id, book FROM books").fetchall()
            return {row[1]: row[0] for row in rows}
        
    def save_bulk(self, books: BookRegistry, conn=None):
        if conn is None:
            with self.router.meta() as conn:
                self._save_bulk(conn, books)
        else:
            self._save_bulk(conn, books)

    def _save_bulk(self, conn, books: BookRegistry):
        if not books:
            return []

        now = datetime.now().isoformat()
        cursor = conn.cursor()

        cursor.executemany(
            """
            INSERT OR REPLACE INTO books (id, book, uid, title, author, serie, generes, year, added_at, source_type, source_link, source_length, text_length, empty)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    book.text_length,
                    book.empty,
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
