import sqlite3
import pickle
from book import BookTask
from typing import List, Tuple
from datetime import datetime
from contextlib import contextmanager
from settings import DB_FILE

# =====================
# СОЕДИНЕНИЕ
# =====================
@contextmanager
def get_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# =====================
# ИНИЦИАЛИЗАЦИЯ
# =====================
async def init_db():
    with get_connection() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS books (
            book TEXT PRIMARY KEY,
            archive TEXT,
            id TEXT,
            title TEXT,
            author TEXT,
            added_at TEXT
        );
        """)
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS embeddings (
            book TEXT PRIMARY KEY,
            embedding BLOB,
            FOREIGN KEY(book) REFERENCES books(id)
        );
        """)
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS processing (
            book TEXT PRIMARY KEY
        );
        """)
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS authors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL
        );
        """)
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS book_authors (
            book TEXT NOT NULL,
            author_id INTEGER NOT NULL,
            FOREIGN KEY (book) REFERENCES books(book),
            FOREIGN KEY (author_id) REFERENCES authors(id)
        );
        """)
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS similar (
            book TEXT NOT NULL,
            similar_book TEXT NOT NULL,
            score FLOAT,
            FOREIGN KEY (book) REFERENCES books(book),
            FOREIGN KEY (similar_book) REFERENCES books(book)
        );
        """)
        conn.executescript("""
        CREATE INDEX IF NOT EXISTS idx_books_book ON books(book);
        CREATE INDEX IF NOT EXISTS idx_embeddings_book ON embeddings(book);
        CREATE INDEX IF NOT EXISTS idx_similar_book ON similar(book);
        CREATE INDEX IF NOT EXISTS idx_authors_id ON authors(id);
        CREATE INDEX IF NOT EXISTS idx_book_authors_book ON book_authors(book);
        CREATE INDEX IF NOT EXISTS idx_book_authors_author_id ON book_authors(author_id);
        """)
        conn.commit()


# =====================
# CRUD МЕТОДЫ
# =====================
async def save_embeddings(book, emb_vector):
    with get_connection() as conn:
        conn.execute("INSERT OR REPLACE INTO embeddings(book, embedding) VALUES (?, ?)", (book, pickle.dumps(emb_vector)))
        conn.commit()

async def save_book(book, archive, id, title, author):
    with get_connection() as conn:
        conn.execute("""
                        INSERT OR REPLACE INTO books
                        (book, archive, id, title, author, added_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                     """,
                    (
                        book,
                        archive,
                        id,
                        title,
                        author,
                        datetime.now().isoformat()
                    ))
        conn.commit()

def save_book_with_emb(book, archive, id, title, author, emb_vector):
    with get_connection() as conn:
        conn.execute("""
                        INSERT OR REPLACE INTO books
                        (book, archive, id, title, author, added_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                     """,
                    (
                        book,
                        archive,
                        id,
                        title,
                        author,
                        datetime.now().isoformat()
                    ))
        conn.execute("INSERT OR REPLACE INTO embeddings(book, embedding) VALUES (?, ?)", (book, pickle.dumps(emb_vector)))
        conn.commit()

def save_similar(source: BookTask, similars: List[Tuple[BookTask, float]]):
    with get_connection() as conn:
        cur = conn.cursor()
        
        data_to_insert = [
            (source.file_name, similar_task.file_name, score)
            for similar_task, score in similars
        ]

        cur.execute("DELETE FROM similar WHERE book = ?", (source.file_name,))
        
        cur.executemany(
            "INSERT INTO similar (book, similar_book, score) VALUES (?, ?, ?)",
            data_to_insert
        )
        
        conn.commit()

def update_book_authors(book: str, authors: list[str]):
    with get_connection() as conn:
        cur = conn.cursor()

        cur.execute(
            "DELETE FROM book_authors WHERE book = ?",
            (book,)
        )

        if not authors:
            conn.commit()
            return

        author_ids = []
        for author_name in authors:
            cur.execute("SELECT id FROM authors WHERE name = ?", (author_name,))
            row = cur.fetchone()
            if row:
                author_id = row[0]
            else:
                cur.execute("INSERT INTO authors (name) VALUES (?)", (author_name,))
                author_id = cur.lastrowid
            author_ids.append(author_id)

        cur.executemany(
            "INSERT INTO book_authors (book, author_id) VALUES (?, ?)",
            [(book, aid) for aid in author_ids]
        )

        conn.commit()

def load_books_only() -> set[tuple[str, str]]:
    query = """
        SELECT b.archive, b.book
        FROM books b
        GROUP BY b.archive, b.book
    """

    with get_connection() as conn:
        rows = conn.execute(query).fetchall()

    return {(row["archive"], row["book"]) for row in rows}

def load_books_with_embeddings() -> list[tuple]:
    query = """
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
        LEFT JOIN similar s ON s.book = b.book
        LEFT JOIN book_authors ba ON ba.book = b.book
        LEFT JOIN authors a ON a.id = ba.author_id
        GROUP BY b.archive, b.book, e.embedding;
    """

    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()

def load_books_with_authors() -> list[tuple]:
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

    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()

def finshed_books_count():
    with get_connection() as conn:
        cursor = conn.execute("SELECT COUNT(*) FROM embeddings")
        return cursor.fetchone()[0]
    
def claim_book():
    with get_connection() as conn:
        conn.execute("""
            SELECT b.id, b.title, b.author
            FROM books b
            LEFT JOIN embeddings e ON b.id = e.book
            LEFT JOIN processing p ON b.id = p.book
            WHERE e.book IS NULL AND p.book IS NULL
            LIMIT 1
        """)
        row = conn.fetchone()
        if row:
            id, title, author = row
            conn.execute("INSERT OR IGNORE INTO processing(book) VALUES (?)", (id,))
            conn.commit()
            return {"book": id, "title": title, "author": author}
    return None

def is_book_in_db(archive_name: str, file_name: str) -> bool:
    with get_connection() as conn:
        cursor = conn.execute("""
            SELECT 1 FROM books
            WHERE archive = ? AND book = ?
            LIMIT 1
        """, (archive_name, file_name))
        return cursor.fetchone() is not None