from app.infrastructure.models.report import Report
from app.settings import ChunkingConfig

class DatabaseReporter:
    def __init__(self, router, model_uid: str):
        self.router = router
        self.model_uid = model_uid

    def generate(
            self, 
            new_books: set | None = None,
        ) -> Report:

        report = Report()
        min_chars = ChunkingConfig.ST_MIN_CHARS

        # --- BOOKS ---
        with self.router.meta() as conn:
            books_total = conn.execute("SELECT COUNT(*) FROM books").fetchone()[0]
            books_empty = conn.execute("SELECT COUNT(*) FROM books WHERE empty = 1").fetchone()[0]
            books_without_chunks = conn.execute("""
                SELECT COUNT(*)
                FROM books b
                LEFT JOIN chunks c ON c.book_id = b.id
                WHERE c.id IS NULL
            """).fetchone()[0]

            # книги с одним чанком
            books_with_one_chunk = conn.execute("""
                SELECT COUNT(*)
                FROM (
                    SELECT c.book_id
                    FROM chunks c
                    GROUP BY c.book_id
                    HAVING COUNT(*) = 1
                )
            """).fetchone()[0]

            if new_books:
                books_in_db = {row[0] for row in conn.execute("SELECT book FROM books").fetchall()}
                first_10_new_books = list(new_books - books_in_db)[:10]
            else:
                first_10_new_books = []

        books_block = report.create_block("Books")
        books_block.add("Parsed books", len(new_books) if new_books else 0)
        books_block.add("Total books", books_total)
        books_block.add("Empty books", books_empty)
        books_block.add("Books without chunks", books_without_chunks)
        books_block.add("Books with one chunk", books_with_one_chunk)

        # --- CHUNKS ---
        with self.router.chunks() as conn:
            chunks_data = conn.execute("SELECT id, book_id, length FROM chunks").fetchall()
            chunks_total = len(chunks_data)

            chunk_ids = {c[0] for c in chunks_data}

            # чанки меньше минимального размера
            small_chunks_count = sum(1 for c in chunks_data if c[2] < min_chars)

        chunks_block = report.create_block("Chunks")
        chunks_block.add("Total chunks", chunks_total)
        chunks_block.add(f"Chunks length < {min_chars}", small_chunks_count)

        # --- EMBEDDINGS ---
        with self.router.embeddings(self.model_uid) as conn:
            embeddings_data = conn.execute("SELECT id, chunk_id FROM embeddings").fetchall()
            embeddings_total = len(embeddings_data)
            embedding_chunk_ids = {e[1] for e in embeddings_data}

        chunks_without_embeddings = sum(1 for cid in chunk_ids if cid not in embedding_chunk_ids)
        embeddings_without_chunks = sum(1 for e in embeddings_data if e[1] not in chunk_ids)

        chunks_block.add("Chunks without embeddings", chunks_without_embeddings)

        embeddings_block = report.create_block("Embeddings")
        embeddings_block.add("Total embeddings", embeddings_total)
        embeddings_block.add("Embeddings without chunks", embeddings_without_chunks)

        # --- средние ---
        avg_chunks_per_book = sum(1 for c in chunks_data) / max(1, len({c[1] for c in chunks_data}))
        avg_embeddings_per_chunk = sum(1 for e in embeddings_data) / max(1, len(chunk_ids))

        chunks_block.add("Average chunks per book", float(avg_chunks_per_book))
        embeddings_block.add("Average embeddings per chunk", float(avg_embeddings_per_chunk))

        # --- книги без эмбеддингов ---
        books_with_embeddings = set()
        for c in chunks_data:
            if c[0] in embedding_chunk_ids:
                books_with_embeddings.add(c[1])
        books_without_embeddings = len(set(c[1] for c in chunks_data)) - len(books_with_embeddings)
        books_block.add("Books without embeddings", books_without_embeddings)

        # --- новые книги ---
        if first_10_new_books:
            new_books_block = report.create_block("New Books")
            new_books_block.add("First 10 new books", None, extra=first_10_new_books)

        return report