from datetime import datetime
from typing import Any, Generator, Tuple

class BookRepository:
    GET_QUERY = """
    SELECT
        b.id,
        b.archive,
        b.book,
        b.title,
        b.author
    FROM books b
    """
    
    def get_all(self, conn) -> Any:
        cursor = conn.execute(f"{self.GET_QUERY}")
        for row in cursor:
            yield (tuple[Any, ...](row))

    def get_all_with_embeddings(self, conn) -> Generator[Tuple[int, str, str, str, bytes]]:
        cursor = conn.execute("""
        SELECT
            b.id,
            b.archive,
            b.book,
            b.title,
            e.embedding
        FROM books b
        JOIN embeddings e ON e.book_id = b.id
        ORDER BY b.id ASC
        """)
        for row in cursor:
            yield (tuple[Any, ...](row))

    def get_by_file(self, conn, book: str) -> Any:
        row = conn.execute(f"{self.GET_QUERY} WHERE b.book = ?",(book,)).fetchone()
        return row if row else None
    
    def get_by_id(self, conn, book_id: int) -> Any:
        row = conn.execute(f"{self.GET_QUERY} WHERE b.id = ?",(book_id,)).fetchone()
        return row if row else None
    
    def get_many(self, conn, book_ids: list[int]) -> dict[int, Any]:
        if not book_ids:
            return {}

        placeholders = ",".join("?" for _ in book_ids)
        rows = conn.execute(f"{self.GET_QUERY} WHERE id IN ({placeholders})",book_ids).fetchall()

        return {
            row["id"]: row
            for row in rows
        }

    def get_names(conn) -> list[str]:
        rows = conn.execute("SELECT book FROM books").fetchall()
        return [row[0] for row in rows]
    
    def embeddings_cursor(self, conn):
        embeddings_cursor = conn.cursor()
        embeddings_cursor.execute(self.GET_QUERY + " GROUP BY b.book")
        return embeddings_cursor

    def save(conn, book, archive, uid, title, author) -> int | None:
        conn.execute(
            """
            INSERT OR REPLACE INTO books
            (book, archive, uid, title, author, added_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (book, archive, uid, title, author, datetime.now().isoformat())
        )

        row = conn.execute("SELECT id FROM books WHERE book = ? AND archive = ?", (book,archive,)).fetchone()
        book_id = row[0]

        return book_id

    def count_embeddings(conn) -> int:
        return conn.execute("SELECT COUNT(*) FROM embeddings").fetchone()[0]
