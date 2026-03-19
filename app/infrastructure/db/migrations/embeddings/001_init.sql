CREATE TABLE IF NOT EXISTS embeddings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id INTEGER NOT NULL,
    chunk_id INTEGER NULL,
    seq INTEGER NULL,
    data NUMPY NOT NULL,
    shape INTEGER NOT NULL,
    type INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_embeddings_source_id
ON embeddings(source_id);

CREATE INDEX IF NOT EXISTS idx_embeddings_chunk_id
ON embeddings(chunk_id);

CREATE INDEX IF NOT EXISTS idx_embeddings_type
ON embeddings(type);

CREATE INDEX IF NOT EXISTS idx_embeddings_type_book_chunk
ON embeddings(type, source_id, chunk_id);