import sqlite3
import pickle
import asyncio
from datetime import datetime
from contextlib import contextmanager
from typing import List, Tuple
from app.models import Book, FeedbackReq, Feedback, Feedbacks
from app.settings.config import DB_FILE

class DBManager:
    def __init__(self):
        self.db_file = DB_FILE

    embeddings_query: str = """
                SELECT
                    b.archive,
                    b.book,
                    b.title,
                    e.embedding,
                    GROUP_CONCAT(a.name, ',') AS authors_csv
                FROM books b
                JOIN embeddings e ON e.book = b.book
                LEFT JOIN book_authors ba ON ba.book = b.book
                LEFT JOIN authors a ON a.id = ba.author_id
                GROUP BY b.archive, b.book, b.title, e.embedding;
            """
        
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
                               
            CREATE TABLE IF NOT EXISTS feedback (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_file_name TEXT NOT NULL,
                candidate_file_name TEXT NOT NULL,
                label INTEGER NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(source_file_name, candidate_file_name)
            );
                               
            CREATE INDEX IF NOT EXISTS idx_books_book ON books(book);
            CREATE INDEX IF NOT EXISTS idx_embeddings_book ON embeddings(book);
            CREATE INDEX IF NOT EXISTS idx_similar_book ON similar(book);
            CREATE INDEX IF NOT EXISTS idx_authors_id ON authors(id);
            CREATE INDEX IF NOT EXISTS idx_book_authors_book ON book_authors(book);
            CREATE INDEX IF NOT EXISTS idx_book_authors_author_id ON book_authors(author_id);
            """)

    def save_book(self, book, archive, id, title, author, emb_vector):
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
        similars: List[Tuple[float, Book]],
    ):
        with self.connection() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM similar WHERE book = ?", (source,))
            cur.executemany(
                "INSERT INTO similar (book, similar_book, score) VALUES (?, ?, ?)",
                [
                    (source, book.file_name, float(score))
                    for score, book in similars
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

    def load_books_with_embeddings(self) -> List[Book]:
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
                Book(
                    archive_name=archive,
                    file_name=book,
                    title=title,
                    embedding=embedding,
                    authors=authors_csv.split(",") if authors_csv else None,
                    completed=bool(processed)
                )
                for archive, book, title, embedding, authors_csv, processed in rows
            ]
        
    async def load_books_with_authors(self) -> list[Book]:
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
                Book(
                    archive_name=archive,
                    file_name=book,
                    title=title,
                    completed=bool(processed)
                )
                for archive, book, title, processed in rows
            ]
                  
    def get_book(self, book: str) -> Book | None:
        with self.connection() as conn:
            row = conn.execute("""
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
                WHERE b.book = ?
                GROUP BY b.archive, b.book, b.title, e.embedding
            """, (book,)).fetchone()

            if row is None:
                return None

            archive, book_from_db, title, embedding, authors_csv, processed = row

            return Book(
                archive_name=archive,
                file_name=book_from_db,
                title=title,
                embedding=embedding,
                authors=authors_csv.split(",") if authors_csv else None,
                completed=bool(processed)
            )

    def finished_books_count(self) -> int:
        with self.connection() as conn:
            return conn.execute(
                "SELECT COUNT(*) FROM embeddings"
            ).fetchone()[0]

    def has_similar(self, book: str) -> bool:
        with self.connection() as conn:
            return conn.execute(
                "SELECT 1 FROM similar WHERE book = ? LIMIT 1",
                (book,)
            ).fetchone() is not None

    def get_similar_rows(self, book: str, limit: int) -> List[Tuple[str, float, str]]:
        with self.connection() as conn:
            rows = conn.execute("""
                SELECT s.similar_book, s.score, b.title
                FROM similar s
                JOIN books b ON b.book = s.similar_book
                WHERE s.book = ?
                ORDER BY s.score DESC
                LIMIT ?
            """, (book, limit)).fetchall()

            return [(similar_book, score, title) for similar_book, score, title in rows]

    async def submit_feedback(self, fb: FeedbackReq):   
        with self.connection() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO feedback 
                (source_file_name, candidate_file_name, label)
                VALUES (?, ?, ?)
            """, (fb.source_file_name, fb.candidate_file_name, fb.label))
            conn.commit()
    
    def fetch_feedbacks(self, book: str) -> Feedbacks:
        feedbacks: List[Feedback] = []
        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT
                    source_file_name,
                    candidate_file_name,
                    label,
                    created_at
                FROM feedback
                WHERE source_file_name = ?
                GROUP BY candidate_file_name
            """, (book,))

            for row in cursor.fetchall():
                fb = Feedback.from_row(row)
                if fb:
                    feedbacks.append(fb)
        
        return Feedbacks(feedbacks=feedbacks)
    
    def fetch_feedbacks_all(self) -> Feedbacks:
        feedbacks: List[Feedback] = []
        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    source_file_name,
                    candidate_file_name,
                    label,
                    created_at
                FROM feedback
                ORDER BY created_at DESC
            """)

            for row in cursor.fetchall():
                fb = Feedback.from_row(row)
                if fb:
                    feedbacks.append(fb)
        
        return Feedbacks(feedbacks=feedbacks)