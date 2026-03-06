import asyncio
from app.model import Model
from app.infrastructure.db import DBRouter, Migrator
from app.infrastructure.db.repositories import BookRepository, ChunkRepository, EmbeddingsRepository
from app.infrastructure.models import Book, Chunk, Embedding
from app.searchEngines.bookSearch import BookSearchEngineFactory
from app.settings import ProcessConfig
from app.ui.live_ui import LiveUI
from app.workers.stats import PipelineStats
from app.workers.stages import BookProducer, Chunker, DbWorker, EmbeddingWorker

class GenerateEmbeddingsWorker:
    def __init__(self, batch: int):
        self.name = "Generate embeddings"
        self.model = Model()
        self.router = DBRouter()
        Migrator(self.router).migrate_all(self.model.info.uid)

        self.search_engine = BookSearchEngineFactory(BookSearchEngineFactory.INPIX)

        self.stats_books = PipelineStats("Books")
        self.stats_chunks = PipelineStats("Chunks")
        self.stats_embeddings = PipelineStats("Embeddings")
        self.stats_db_books = PipelineStats("DB Books")
        self.stats_db_chunks = PipelineStats("DB Chunks")
        self.stats_db_embeddings = PipelineStats("DB Embeddings")

        all_stats: dict[str, PipelineStats] = {
            "books": self.stats_books,
            "chunks": self.stats_chunks,
            "embeddings": self.stats_embeddings,
            "db books": self.stats_db_books,
            "db chunks": self.stats_db_chunks,
            "db embeddings": self.stats_db_embeddings,
        }

        self.ui = LiveUI(
            max_workers=ProcessConfig.MAX_WORKERS,
            title=self.name,
            stats=all_stats
        )
        
        self.queue_books = asyncio.Queue(maxsize=50)
        self.queue_books_db = asyncio.Queue(maxsize=50)
        self.queue_chunks = asyncio.Queue(maxsize=200)
        self.queue_chunks_db = asyncio.Queue(maxsize=200)
        self.queue_embeddings = asyncio.Queue(maxsize=200)

        self.book_stage = BookProducer(
            output_queues=[self.queue_books, self.queue_books_db],
            stats=self.stats_books,
            router=self.router,
            search_engine=self.search_engine,
            name="Book",
            edge="Chunk,DB",
            batch_size=10,
        )

        self.db_stage_book = DbWorker(
            input_queue=[self.queue_books_db],
            stats=self.stats_db_books,
            save_func=GenerateEmbeddingsWorker.save_books,
            batch_size=32,
            name="DB Books",
        )

        self.chunk_stage = Chunker(
            input_queue=self.queue_books,
            output_queue=[self.queue_chunks, self.queue_chunks_db],
            stats=self.stats_chunks,
            router=self.router,
            search_engine=self.search_engine,
            name="Chunk",
            edge="Embedding,DB",
            batch_size=16,
            max_workers=4
        )

        self.db_stage_chunk = DbWorker(
            input_queue=[self.queue_chunks_db],
            stats=self.stats_db_books,
            save_func=GenerateEmbeddingsWorker.save_chunks,
            batch_size=32,
            name="DB Chunk",
        )

        self.embedding_stage = EmbeddingWorker(
            input_queue=self.queue_chunks,
            output_queue=[self.queue_embeddings],
            stats=self.stats_embeddings,
            router=self.router,
            model=self.model,
            name="Embedding",
            edge="DB",
            batch_size=32,
            max_workers=2
        )

        self.db_stage_emb = DbWorker(
            input_queue=[self.queue_embeddings],
            stats=self.stats_db_books,
            save_func=self._save_embeddings,
            batch_size=32,
            name="DB Embedding",
        )

    async def start(self):
        self.ui.init()
        
        tasks = [
            asyncio.create_task(self.book_stage.start(workers=1)),
            asyncio.create_task(self.chunk_stage.start(workers=4)),
            asyncio.create_task(self.embedding_stage.start(workers=2)),
            asyncio.create_task(self.db_stage_book.start(workers=1)),
            asyncio.create_task(self.db_stage_chunk.start(workers=1)),
            asyncio.create_task(self.db_stage_emb.start(workers=1)),
        ]

        await asyncio.gather(*tasks)

    @staticmethod
    def save_books(router: DBRouter, books: list[Book]):
        BookRepository(router).save_bulk(books)

    @staticmethod
    def save_chunks(router: DBRouter, chunks: list[Chunk]):
        ChunkRepository(router).save_bulk(chunks)

    def _save_embeddings(self, router: DBRouter, embeddings: list[Embedding]):
        EmbeddingsRepository(router, self.model.info.uid).save_bulk(embeddings)