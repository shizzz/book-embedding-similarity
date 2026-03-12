CREATE INDEX IF NOT EXISTS idx_embeddings_type_book_chunk
ON embeddings(type, book_id, chunk_id);