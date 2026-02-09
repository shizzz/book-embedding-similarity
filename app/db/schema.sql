CREATE TABLE IF NOT EXISTS books (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    book TEXT,
    archive TEXT,
    uid TEXT,
    title TEXT,
    author TEXT,
    added_at TEXT
);

CREATE TABLE IF NOT EXISTS embeddings (
    book_id INTEGER PRIMARY KEY,
    embedding BLOB,
    FOREIGN KEY(book_id) REFERENCES books(id)
);

CREATE TABLE IF NOT EXISTS authors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS book_authors (
    book_id INTEGER NOT NULL,
    author_id INTEGER NOT NULL,
    FOREIGN KEY (book_id) REFERENCES books(id),
    FOREIGN KEY (author_id) REFERENCES authors(id)
);

CREATE TABLE IF NOT EXISTS similar (
    book_id INTEGER NOT NULL,
    similar_book_id INTEGER NOT NULL,
    score FLOAT,
    FOREIGN KEY (book_id) REFERENCES books(id),
    FOREIGN KEY (similar_book_id) REFERENCES books(id)
);
                    
CREATE TABLE IF NOT EXISTS feedback (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_book_id INTEGER NOT NULL,
    candidate_book_id INTEGER NOT NULL,
    label INTEGER NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_book_id, candidate_book_id),
    FOREIGN KEY (source_book_id) REFERENCES books(id),
    FOREIGN KEY (candidate_book_id) REFERENCES books(id)
);
                    
CREATE INDEX IF NOT EXISTS idx_books_book_archive ON books(book, archive);
CREATE INDEX IF NOT EXISTS idx_embeddings_book_id ON embeddings(book_id);
CREATE INDEX IF NOT EXISTS idx_similar_book_id ON similar(book_id);
CREATE INDEX IF NOT EXISTS idx_book_authors_book_id ON book_authors(book_id);
CREATE INDEX IF NOT EXISTS idx_book_authors_author_id ON book_authors(author_id);
CREATE INDEX IF NOT EXISTS idx_similar_book_id ON similar(book_id);
CREATE INDEX IF NOT EXISTS idx_similar_similar_book_id ON similar(similar_book_id);
CREATE INDEX IF NOT EXISTS idx_feedback_source_book_id ON feedback(source_book_id);
CREATE INDEX IF NOT EXISTS idx_feedback_candidate_book_id ON feedback(candidate_book_id);