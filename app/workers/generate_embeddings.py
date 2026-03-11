import asyncio
import logging
from typing import List
from app.model import Model
from app.common.types import TEntity
from app.infrastructure.db import DBRouter, Migrator, DBTransaction
from app.infrastructure.db.repositories import BookRepository, ChunkRepository, EmbeddingsRepository, ModelRepository, AuthorRepository
from app.infrastructure.models import Book, Chunk, Embedding, Channel, Dataset, BatchTask, Stages
from app.searchEngines.bookSearch import BookSearchEngineFactory
from app.workers.base import BaseQueueWorker, BaseWorker
from app.workers.stages import BookProducer, Parser, DbWorker, EmbeddingWorker
from app.workers.sources.databaseReporter import DatabaseReporter

BOOK_THREADS: int = 1
CHUNK_THREADS: int = 4
EMB_THREADS: int = 4
DB_THREADS: int = 1

class GenerateEmbeddingsWorker(BaseWorker):
    def __init__(self, batch: int):
        super().__init__(name="Generate embeddings", logger=logging.getLogger(__name__))
        self.model = Model(EMB_THREADS)
        Migrator(self.router).migrate_all([self.model.info.uid])
        ModelRepository(self.router).get_or_create(self.model.info.uid, self.model.info.model_name)

        self._savers = {
            Dataset.BOOK: self._save_books,
            Dataset.CHUNK: self._save_chunks,
            Dataset.EMBEDDING: self._save_embeddings,
        }
        
        self.search_engine = BookSearchEngineFactory().create(BookSearchEngineFactory.INPIX, self.stats)

    async def after_run(self) -> None:
        report = DatabaseReporter(self.router, self.model.info.uid).generate()
        self.ui.report(report)

    async def setup_stages(self):
        self.ui.model_info = self.model.info
        
        self.pool: List[BaseQueueWorker] = []
        channel_book = Channel(Stages.PARSER, asyncio.Queue(maxsize=50))
        channel_chunks = Channel(Stages.EMBEDDING, asyncio.Queue(100))
        channel_db = Channel(Stages.DB, asyncio.Queue(maxsize=400))

        self.book_stage = BookProducer(
            router=self.router,
            search_engine=self.search_engine,
            output_channels=[channel_book],
            stats=self.stats,
            batch_size=10,
            workers=BOOK_THREADS,
            logger = self.logger, 
        )
        self.pool.append(self.book_stage)

        self.chunk_stage = Parser(
            router=self.router,
            search_engine=self.search_engine,
            input_channel=channel_book,
            output_channels=[channel_chunks, channel_db],
            stats=self.stats,
            batch_size=64,
            workers=CHUNK_THREADS,
            logger = self.logger, 
        )
        self.pool.append(self.chunk_stage)

        self.embedding_stage = EmbeddingWorker(
            model=self.model,
            router=self.router,
            input_channel=channel_chunks,
            output_channels=[channel_db],
            stats=self.stats,
            workers=EMB_THREADS,
            logger = self.logger, 
        )
        self.pool.append(self.embedding_stage)

        self.db_stage = DbWorker(
            router=self.router,
            save_func=self._save,
            input_channel=channel_db,
            stats=self.stats,
            batch_size=500,
            workers=DB_THREADS,
            logger = self.logger, 
        )
        self.pool.append(self.db_stage)

    def _save_books(self, books: List[Book], tx: DBTransaction):
        BookRepository(self.router).save_bulk(books, conn=tx.meta())
        AuthorRepository(self.router).save_bulk(books, conn=tx.meta())

    def _save_chunks(self, chunks: List[Chunk], tx: DBTransaction):
        ChunkRepository(self.router).save_bulk(chunks, conn_meta=tx.meta(), conn_chunks=tx.chunks())

    def _save_embeddings(self, emb: List[Embedding], tx: DBTransaction):
        EmbeddingsRepository(self.router, self.model.info.uid).save_bulk(emb, conn=tx.embeddings(self.model.info.uid))

    def _save(self, threads: int, router: DBRouter, tasks: List[BatchTask[TEntity]]):
        with router.transaction() as tx:
            for task in tasks:
                saver = self._savers[task.dataset]
                saver(task.entity, tx)
    
    async def _save_async(self, threads: int, router: DBRouter, tasks: List[BatchTask[TEntity]]):
        if threads > 1:
            async with router.lock_all(self.model.info.uid):
                await asyncio.to_thread(self._save, threads, router, tasks)
        else:
            await asyncio.to_thread(self._save, threads, router, tasks)
