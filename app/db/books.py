from datetime import datetime
from typing import Any, List, Tuple

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
    
    def get_all_with_embeddings(self, conn) -> List[Tuple[int, str, str, str, bytes]]:
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

    def save_authors(conn, book: str, authors: list[str]):
        if not authors:
            return
        
        cur = conn.cursor()
        cur.execute("DELETE FROM book_authors WHERE book = ?", (book,))

        author_ids = []
        for name in authors:
            cur.execute("SELECT id FROM authors WHERE name = ?", (name,))
            row = cur.fetchone()
            if row:
                author_ids.append(row["id"])
            else:
                cur.execute("INSERT INTO authors (name) VALUES (?)", (name,))
                author_ids.append(cur.lastrowid)

        cur.executemany(
            "INSERT INTO book_authors (book, author_id) VALUES (?, ?)",
            [(book, aid) for aid in author_ids]
        )

    def count_embeddings(conn) -> int:
        return conn.execute("SELECT COUNT(*) FROM embeddings").fetchone()[0]
