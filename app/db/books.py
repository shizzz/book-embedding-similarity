from datetime import datetime
from typing import Any, Generator, Tuple
import numpy as np

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
        b.source_type,
        b.source_link,
        e.embedding,
        e.source_text,
        e.model
    FROM books b
    JOIN embeddings e ON e.book_id = b.id
    """
    
    @staticmethod
    def get_all(conn) -> Any:
        cursor = conn.execute(f"{BookRepository.GET_QUERY}")
        for row in cursor:
            yield (tuple[Any, ...](row))

    @staticmethod
    def get_all_with_embeddings(conn) -> Generator[Tuple[int, str, str, str, str, str, np.ndarray]]:
        cursor = conn.execute("""
        SELECT
            b.id,
            b.book,
            b.title,
            b.author,
            b.source_type,
            b.source_link,
            e.embedding
        FROM books b
        JOIN embeddings e ON e.book_id = b.id
        ORDER BY b.id ASC
        """)
        for row in cursor:
            yield (tuple[Any, ...](row))

    @staticmethod
    def get_by_file(conn, book: str) -> Any:
        row = conn.execute(f"{BookRepository.GET_QUERY} WHERE b.book = ?",(book,)).fetchone()
        return row if row else None

    @staticmethod
    def get_full_by_file(conn, book: str) -> Any:
        row = conn.execute(f"{BookRepository.GET_FULL_QUERY} WHERE b.book = ?",(book,)).fetchone()
        return row if row else None
    
    @staticmethod
    def get_by_id(conn, book_id: int) -> Any:
        row = conn.execute(f"{BookRepository.GET_QUERY} WHERE b.id = ?",(book_id,)).fetchone()
        return row if row else None
    
    @staticmethod
    def get_many(conn, book_ids: list[int]) -> dict[int, Any]:
        if not book_ids:
            return {}

        placeholders = ",".join("?" for _ in book_ids)
        rows = conn.execute(f"{BookRepository.GET_QUERY} WHERE id IN ({placeholders})",book_ids).fetchall()

        return {
            row["id"]: row
            for row in rows
        }

    def get_names(conn) -> list[str]:
        rows = conn.execute("SELECT book FROM books").fetchall()
        return [row[0] for row in rows]
    
    def embeddings_cursor(self, conn):
        embeddings_cursor = conn.cursor()
        embeddings_cursor.execute(BookRepository.GET_QUERY + " GROUP BY b.book")
        return embeddings_cursor

    def save(
            conn,
            book: str,
            uid: str,
            title: str,
            author: str,
            source_type: str,
            source_link: str
        ) -> int | None:
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

    @staticmethod
    def save_bulk(conn, books: list):
        if not books:
            return []

        now = datetime.now().isoformat()

        cursor = conn.cursor()

        cursor.executemany(
            """
            INSERT OR REPLACE INTO books (id, book, uid, title, author, added_at, source_type, source_link)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    book.id,
                    book.file_name,
                    book.uid,
                    book.title,
                    book.author,
                    now,
                    book.source_type,
                    book.source_link,
                )
                for book in books
            ]
        )
    
    @staticmethod
    def get_max_id(conn) -> int:
        cursor = conn.execute("SELECT seq FROM sqlite_sequence WHERE name = 'books'")
        row = cursor.fetchone()
        if row is None:
            start_id = 1
        else:
            start_id = row["seq"] + 1
        return start_id

    @staticmethod
    def count_embeddings(conn) -> int:
        return conn.execute("SELECT COUNT(*) FROM embeddings").fetchone()[0]
