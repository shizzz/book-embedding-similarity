CREATE TABLE IF NOT EXISTS embeddings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    book_id INTEGER NOT NULL,
    chunk_id INTEGER NOT NULL,
    data NUMPY NOT NULL,
    shape INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_embeddings_book_id
ON embeddings(book_id);

CREATE INDEX IF NOT EXISTS idx_embeddings_chunk_id
ON embeddings(chunk_id);