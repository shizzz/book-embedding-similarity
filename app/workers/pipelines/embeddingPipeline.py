import asyncio
from typing import List
from app.model import Model
from app.common.types import TEntity
from app.infrastructure.db import Migrator, DBRouter, DBTransaction
from app.infrastructure.db.repositories import ModelRepository, BookRepository, ChunkRepository, EmbeddingsRepository, AuthorRepository
from app.infrastructure.models import Channel, Stages, Dataset, Book, Chunk, Embedding, BatchTask
from app.searchEngines.bookSearch import BookSearchEngineFactory
from app.workers.stages import BookProducer, Parser, DbWorker, EmbeddingWorker
from .pipeline import Pipeline

BOOK_THREADS: int = 1
CHUNK_THREADS: int = 4
EMB_THREADS: int = 4
DB_THREADS: int = 1

class EmbeddingPipeline(Pipeline):
    def __init__(
        self,
        *args, 
        **kwargs
    ):
        super().__init__(name="embeddings", *args, **kwargs)

        self.model = Model(EMB_THREADS)
        self.search_engine = BookSearchEngineFactory().create(BookSearchEngineFactory.INPIX, self._stats)

        Migrator(self._router).migrate_all([self.model.info.uid])
        ModelRepository(self._router).get_or_create(self.model.info.uid, self.model.info.model_name)

        self._savers = {
            Dataset.BOOK: self._save_books,
            Dataset.CHUNK: self._save_chunks,
            Dataset.EMBEDDING: self._save_embeddings,
        }

    async def setup_stages(self) -> None:
        channel_book = Channel(Stages.PARSER, asyncio.Queue(maxsize=50))
        channel_chunks = Channel(Stages.EMBEDDING, asyncio.Queue(100))
        channel_db = Channel(Stages.DB, asyncio.Queue(maxsize=400))

        book_stage = BookProducer(
            router=self._router,
            search_engine=self.search_engine,
            input_channel=self._input_channel,
            output_channels=[channel_book],
            stats=self._stats,
            batch_size=10,
            workers=BOOK_THREADS,
            logger = self._logger, 
        )
        self.pool.append(book_stage)

        chunk_stage = Parser(
            router=self._router,
            search_engine=self.search_engine,
            input_channel=channel_book,
            output_channels=[channel_chunks, channel_db],
            stats=self._stats,
            batch_size=64,
            workers=CHUNK_THREADS,
            logger = self._logger, 
        )
        self.pool.append(chunk_stage)

        embedding_stage = EmbeddingWorker(
            model=self.model,
            router=self._router,
            input_channel=channel_chunks,
            output_channels=[channel_db, *(self._output_channels or [])],
            stats=self._stats,
            workers=EMB_THREADS,
            logger = self._logger, 
        )
        self.pool.append(embedding_stage)

        db_stage = DbWorker(
            router=self._router,
            save_func=self._save_async,
            input_channel=channel_db,
            stats=self._stats,
            batch_size=256,
            workers=DB_THREADS,
            logger = self._logger, 
        )
        self.pool.append(db_stage)

    def _save_books(self, books: List[Book], tx: DBTransaction):
        BookRepository(self._router).save_bulk(books, conn=tx.meta())
        AuthorRepository(self._router).save_bulk(books, conn=tx.meta())

    def _save_chunks(self, chunks: List[Chunk], tx: DBTransaction):
        ChunkRepository(self._router).save_bulk(chunks, conn_meta=tx.meta(), conn_chunks=tx.chunks())

    def _save_embeddings(self, emb: List[Embedding], tx: DBTransaction):
        EmbeddingsRepository(self._router, self.model.info.uid).save_bulk(emb, conn=tx.embeddings(self.model.info.uid))

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