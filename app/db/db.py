import sqlite3
import pickle
import time
import asyncio
from datetime import datetime
from contextlib import contextmanager
from typing import List, Tuple
from app.models import BookTask, QueueRecord
from app.settings.config import DB_FILE

class DBManager:
    def __init__(self):
        self.db_file = DB_FILE

    @contextmanager
    def connection(self):
        conn = sqlite3.connect(self.db_file)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def init_db(self):
        with self.connection() as conn:
            conn.executescript("""
            CREATE TABLE IF NOT EXISTS books (
                book TEXT PRIMARY KEY,
                archive TEXT,
                id TEXT,
                title TEXT,
                author TEXT,
                added_at TEXT
            );

            CREATE TABLE IF NOT EXISTS embeddings (
                book TEXT PRIMARY KEY,
                embedding BLOB,
                FOREIGN KEY(book) REFERENCES books(id)
            );

            CREATE TABLE IF NOT EXISTS processing (
                book TEXT PRIMARY KEY
            );

            CREATE TABLE IF NOT EXISTS authors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS book_authors (
                book TEXT NOT NULL,
                author_id INTEGER NOT NULL,
                FOREIGN KEY (book) REFERENCES books(book),
                FOREIGN KEY (author_id) REFERENCES authors(id)
            );

            CREATE TABLE IF NOT EXISTS similar (
                book TEXT NOT NULL,
                similar_book TEXT NOT NULL,
                score FLOAT,
                FOREIGN KEY (book) REFERENCES books(book),
                FOREIGN KEY (similar_book) REFERENCES books(book)
            );

            CREATE TABLE IF NOT EXISTS process_queue (
                book TEXT PRIMARY KEY,
                progress INTEGER NOT NULL DEFAULT 0,
                started_at REAL NOT NULL,
                book_count INTEGER NOT NULL,
                exclude_same_author BOOL NOT NULL,
                FOREIGN KEY (book) REFERENCES books(book)
            );

            CREATE INDEX IF NOT EXISTS idx_books_book ON books(book);
            CREATE INDEX IF NOT EXISTS idx_embeddings_book ON embeddings(book);
            CREATE INDEX IF NOT EXISTS idx_similar_book ON similar(book);
            CREATE INDEX IF NOT EXISTS idx_authors_id ON authors(id);
            CREATE INDEX IF NOT EXISTS idx_book_authors_book ON book_authors(book);
            CREATE INDEX IF NOT EXISTS idx_book_authors_author_id ON book_authors(author_id);
            """)

    def save_book(self, book, archive, id, title, author):
        with self.connection() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO books
                (book, archive, id, title, author, added_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                book,
                archive,
                id,
                title,
                author,
                datetime.now().isoformat()
            ))

    def save_embeddings(self, book, emb_vector):
        with self.connection() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO embeddings(book, embedding) VALUES (?, ?)",
                (book, pickle.dumps(emb_vector))
            )

    def save_book_with_emb(self, book, archive, id, title, author, emb_vector):
        with self.connection() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO books
                (book, archive, id, title, author, added_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                book,
                archive,
                id,
                title,
                author,
                datetime.now().isoformat()
            ))
            conn.execute(
                "INSERT OR REPLACE INTO embeddings(book, embedding) VALUES (?, ?)",
                (book, pickle.dumps(emb_vector))
            )

    def save_similar(
        self,
        source: str,
        similars: List[Tuple[BookTask, float]],
    ):
        with self.connection() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM similar WHERE book = ?", (source,))
            cur.executemany(
                "INSERT INTO similar (book, similar_book, score) VALUES (?, ?, ?)",
                [
                    (source, s.file_name, score)
                    for s, score in similars
                ]
            )

    def update_book_authors(self, book: str, authors: list[str]):
        with self.connection() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM book_authors WHERE book = ?", (book,))

            if not authors:
                return

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

    async def load_books_only(self) -> list[str]:
        def _load():
            with self.connection() as conn:
                rows = conn.execute("""
                    SELECT book
                    FROM books;
                """).fetchall()
                return [row[0] for row in rows]

        return await asyncio.to_thread(_load)

    async def load_books_with_embeddings(self) -> list[BookTask]:
        with self.connection() as conn:
            rows = conn.execute("""
                SELECT
                    b.archive,
                    b.book,
                    b.title,
                    e.embedding,
                    GROUP_CONCAT(a.name, ',') AS authors_csv,
                    EXISTS (
                        SELECT 1 FROM similar s WHERE s.book = b.book
                    ) AS processed
                FROM books b
                JOIN embeddings e ON e.book = b.book
                LEFT JOIN book_authors ba ON ba.book = b.book
                LEFT JOIN authors a ON a.id = ba.author_id
                GROUP BY b.archive, b.book, e.embedding;
            """).fetchall()

            return [
                BookTask(
                    archive_name=archive,
                    file_name=book,
                    title=title,
                    embedding=embedding,
                    authors=authors_csv.split(",") if authors_csv else None,
                    completed=processed == 1,
                    in_progress=False
                )
                for archive, book, title, embedding, authors_csv, processed in rows
            ]

    async def load_books_with_authors(self) -> list[BookTask]:
        query = """
            SELECT
                b.archive,
                b.book,
                b.title,
                EXISTS (
                    SELECT 1 FROM book_authors s WHERE s.book = b.book
                ) AS processed
            FROM books b
        """

        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()

            return [
                BookTask(
                    archive_name=archive,
                    file_name=book,
                    title=title,
                    completed=processed == 1,
                    in_progress=False
                )
                for archive, book, title, processed in rows
            ]
                
    def get_queue(self) -> list[QueueRecord]:
        with self.connection() as conn:
            rows = conn.execute("""
                SELECT
                    q.book,
                    q.progress,
                    q.book_count,
                    q.exclude_same_author
                FROM process_queue q
            """).fetchall()

            return [
                QueueRecord(
                    book = book,
                    progress = progress,
                    book_count = book_count,
                    exclude_same_author = exclude_same_author
                )
                for book, progress, book_count, exclude_same_author in rows
            ]
        
    def finished_books_count(self) -> int:
        with self.connection() as conn:
            return conn.execute(
                "SELECT COUNT(*) FROM embeddings"
            ).fetchone()[0]

    def is_book_in_db(self, archive: str, book: str) -> bool:
        with self.connection() as conn:
            return conn.execute(
                "SELECT 1 FROM books WHERE archive = ? AND book = ? LIMIT 1",
                (archive, book)
            ).fetchone() is not None

    def has_similar(self, book: str) -> bool:
        with self.connection() as conn:
            return conn.execute(
                "SELECT 1 FROM similar WHERE book = ? LIMIT 1",
                (book,)
            ).fetchone() is not None

    def has_queue(self) -> bool:
        with self.connection() as conn:
            return conn.execute("SELECT 1 FROM process_queue LIMIT 1").fetchone() is not None
        
    def in_process_queue(self, book: str) -> bool:
        with self.connection() as conn:
            cur = conn.execute(
                "SELECT progress FROM process_queue WHERE book = ?",
                (book,)
            )
            row = cur.fetchone()
            return row is not None, (row[0] if row else 0)

    def enqueue_process(self, book: str, book_count: int, exclude_same_author: bool):
        with self.connection() as conn:
            conn.execute("""
                INSERT OR IGNORE INTO process_queue
                (book, progress, started_at, book_count, exclude_same_author)
                VALUES (?, 0, ?, ?, ?)
            """, (book, time.time(), book_count, exclude_same_author))

    def dequeue_process(self, book: str):
        with self.connection() as conn:
            conn.execute(
                "DELETE FROM process_queue WHERE book = ?",
                (book,)
            )

    def update_process_percent(self, book: str, percent: int):
        with self.connection() as conn:
            conn.execute(
                "UPDATE process_queue SET progress = ? WHERE book = ?",
                (percent, book)
            )
            conn.commit()

    def get_similar_rows(self, book: str, limit: int):
        with self.connection() as conn:
            return conn.execute("""
                SELECT s.similar_book, s.score, b.title
                FROM similar s
                JOIN books b ON b.book = s.similar_book
                WHERE s.book = ?
                ORDER BY s.score DESC
                LIMIT ?
            """, (book, limit)).fetchall()
