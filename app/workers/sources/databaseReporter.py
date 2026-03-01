from dataclasses import dataclass


@dataclass
class DatabaseReport:
    parsed_books: int

    books_total: int
    books_empty: int
    books_without_chunks: int

    chunks_total: int
    chunks_without_embeddings: int

    embeddings_total: int
    embeddings_without_chunks: int

    avg_chunks_per_book: float
    avg_embeddings_per_chunk: float


class DatabaseReporter:

    def __init__(self, router, model_uid: str):
        self.router = router
        self.model_uid = model_uid

    def generate(self, parsed_books_count: int) -> DatabaseReport:

        # --- BOOKS ---
        with self.router.meta() as conn:

            books_total = conn.execute(
                "SELECT COUNT(*) FROM books"
            ).fetchone()[0]

            books_empty = conn.execute(
                "SELECT COUNT(*) FROM books WHERE empty = 1"
            ).fetchone()[0]

            books_without_chunks = conn.execute(
                """
                SELECT COUNT(*)
                FROM books b
                LEFT JOIN chunks c ON c.book_id = b.id
                WHERE c.id IS NULL
                """
            ).fetchone()[0]

            avg_chunks_per_book = conn.execute(
                """
                SELECT AVG(cnt)
                FROM (
                    SELECT COUNT(c.id) AS cnt
                    FROM books b
                    LEFT JOIN chunks c ON c.book_id = b.id
                    GROUP BY b.id
                )
                """
            ).fetchone()[0] or 0.0

        # --- CHUNKS ---
        with self.router.chunks() as conn:

            chunks_total = conn.execute(
                "SELECT COUNT(*) FROM chunks"
            ).fetchone()[0]

            chunks_without_embeddings = conn.execute(
                """
                SELECT COUNT(*)
                FROM chunks c
                LEFT JOIN embeddings e ON e.chunk_id = c.id
                WHERE e.id IS NULL
                """
            ).fetchone()[0]

            avg_embeddings_per_chunk = conn.execute(
                """
                SELECT AVG(cnt)
                FROM (
                    SELECT COUNT(e.id) AS cnt
                    FROM chunks c
                    LEFT JOIN embeddings e ON e.chunk_id = c.id
                    GROUP BY c.id
                )
                """
            ).fetchone()[0] or 0.0

        # --- EMBEDDINGS ---
        with self.router.embeddings(self.model_uid) as conn:

            embeddings_total = conn.execute(
                "SELECT COUNT(*) FROM embeddings"
            ).fetchone()[0]

            embeddings_without_chunks = conn.execute(
                """
                SELECT COUNT(*)
                FROM embeddings e
                LEFT JOIN chunks c ON c.id = e.chunk_id
                WHERE c.id IS NULL
                """
            ).fetchone()[0]

        return DatabaseReport(
            parsed_books=parsed_books_count,

            books_total=books_total,
            books_empty=books_empty,
            books_without_chunks=books_without_chunks,

            chunks_total=chunks_total,
            chunks_without_embeddings=chunks_without_embeddings,

            embeddings_total=embeddings_total,
            embeddings_without_chunks=embeddings_without_chunks,

            avg_chunks_per_book=float(avg_chunks_per_book),
            avg_embeddings_per_chunk=float(avg_embeddings_per_chunk),
        )

    @staticmethod
    def print(report: DatabaseReport):

        print("========== DATABASE REPORT ==========\n")

        print("BOOKS")
        print(f"Parsed books:                {report.parsed_books}")
        print(f"Books in database:          {report.books_total}")
        print(f"Empty books:                {report.books_empty}")
        print(f"Books without chunks:       {report.books_without_chunks}")

        print("\nCHUNKS")
        print(f"Total chunks:               {report.chunks_total}")
        print(f"Chunks without embeddings:  {report.chunks_without_embeddings}")

        print("\nEMBEDDINGS")
        print(f"Total embeddings:           {report.embeddings_total}")
        print(f"Embeddings without chunks:  {report.embeddings_without_chunks}")

        print("\nAVERAGES")
        print(f"Chunks per book:            {report.avg_chunks_per_book:.2f}")
        print(f"Embeddings per chunk:       {report.avg_embeddings_per_chunk:.2f}")

        print("\n====================================")