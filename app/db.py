import sqlite3
import pickle
import time
from datetime import datetime
from contextlib import contextmanager
from typing import List, Tuple
from app.models import BookTask
from app.settings import DB_FILE

class DBManager:
    def __init__(self):
        self.db_file = DB_FILE

    # ---------- connection ----------
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

    # ---------- init ----------
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

    # ---------- save ----------
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

    def save_similar(
        self,
        source: BookTask,
        similars: List[Tuple[BookTask, float]],
    ):
        with self.connection() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM similar WHERE book = ?", (source.file_name,))
            cur.executemany(
                "INSERT INTO similar (book, similar_book, score) VALUES (?, ?, ?)",
                [
                    (source.file_name, s.file_name, score)
                    for s, score in similars
                ]
            )

    def load_books_only(self) -> set[tuple[str, str]]:
        with self.connection() as conn:
            rows = conn.execute("""
                SELECT archive, book
                FROM books
                GROUP BY archive, book
            """).fetchall()
        return {(r["archive"], r["book"]) for r in rows}

    def load_books_with_embeddings(self):
        with self.connection() as conn:
            return conn.execute("""
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

    def load_books_with_authors(self) -> list[tuple]:
        query = """
            SELECT
                b.archive,
                b.book,
                b.title,
                NULL AS embedding,
                NULL AS authors_csv,
                EXISTS (
                    SELECT 1 FROM book_authors s WHERE s.book = b.book
                ) AS processed
            FROM books b
        """

        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            return cursor.fetchall()
        
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

    def get_similar_rows(self, book: str, limit: int):
        with self.connection() as conn:
            return conn.execute("""
                SELECT similar_book, score
                FROM similar
                WHERE book = ?
                ORDER BY score DESC
                LIMIT ?
            """, (book, limit)).fetchall()
