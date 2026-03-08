import asyncio
from typing import List
from app.model import Model
from app.common.types import TEntity
from app.infrastructure.db import DBRouter, Migrator, DBTransaction
from app.infrastructure.db.repositories import BookRepository, ChunkRepository, EmbeddingsRepository, ModelRepository
from app.infrastructure.models import Book, Chunk, Embedding, Channel, Action, BatchTask, Stages
from app.searchEngines.bookSearch import BookSearchEngineFactory
from app.settings import ProcessConfig
from app.ui.live_ui import LiveUI
from app.workers.base import BaseQueueWorker
from app.workers.stats import PipelineStats
from app.workers.stages import BookProducer, Chunker, DbWorker, EmbeddingWorker

BOOK_THREADS: int = 1
CHUNK_THREADS: int = 4
EMB_THREADS: int = 2
DB_THREADS: int = 1

class GenerateEmbeddingsWorker:
    def __init__(self, batch: int):
        self.name = "Generate embeddings"
        self.model = Model(EMB_THREADS)
        self.router = DBRouter()
        Migrator(self.router).migrate_all([self.model.info.uid])

        self.stats = PipelineStats()
        self.search_engine = BookSearchEngineFactory().create(BookSearchEngineFactory.INPIX, self.stats)
        self.stages = []
        ModelRepository(self.router).get_or_create(self.model.info.uid, self.model.info.model_name)

        self._savers = {
            Action.BOOK: self._save_books,
            Action.CHUNK: self._save_chunks,
            Action.EMBEDDING: self._save_embeddings,
        }

    async def run(self):
        await self.setup_stages()
        self.ui.init()
        self.ui.model_info = self.model.info
        self._ui_task = asyncio.create_task(self.refresh_ui_loop())
        await asyncio.gather(*(worker.start() for worker in self.pool))
        await asyncio.gather(*(worker.wait() for worker in self.pool))

    async def setup_stages(self):
        self.ui = LiveUI(
            max_workers=ProcessConfig.MAX_WORKERS,
            title=self.name,
            stats=self.stats
        )
        
        self.pool: List[BaseQueueWorker] = []
        channel_book = Channel(Stages.BOOK_SEARCH, asyncio.Queue(maxsize=50))
        channel_chunks = Channel(Stages.EMBEDDING, asyncio.Queue(200))
        channel_db = Channel(Stages.DB, asyncio.Queue(maxsize=50))

        self.book_stage = BookProducer(
            router=self.router,
            search_engine=self.search_engine,
            output_channels=[channel_book],
            stats=self.stats,
            batch_size=10,
            workers=BOOK_THREADS,
        )
        self.pool.append(self.book_stage)

        self.chunk_stage = Chunker(
            router=self.router,
            search_engine=self.search_engine,
            input_channel=channel_book,
            output_channels=[channel_chunks, channel_db],
            stats=self.stats,
            batch_size=16,
            producer_done = self.book_stage.done,
            workers=CHUNK_THREADS,
        )
        self.pool.append(self.chunk_stage)

        self.embedding_stage = EmbeddingWorker(
            model=self.model,
            router=self.router,
            input_channel=channel_chunks,
            output_channels=[channel_db],
            stats=self.stats,
            batch_size=32,
            producer_done = self.chunk_stage.done,
            workers=EMB_THREADS,
        )
        self.pool.append(self.embedding_stage)

        self.db_stage = DbWorker(
            router=self.router,
            save_func=self._save,
            input_channel=channel_db,
            stats=self.stats,
            batch_size=128,
            producer_done = self.book_stage.done,
            workers=DB_THREADS,
        )
        self.pool.append(self.db_stage)

    async def refresh_ui_loop(self):
        while True:
            await self.ui.update()
            await asyncio.sleep(1)

    def _save_books(self, books: List[Book], tx: DBTransaction):
        BookRepository(self.router).save_bulk(books, conn=tx.meta())

    def _save_chunks(self, chunks: List[Chunk], tx: DBTransaction):
        ChunkRepository(self.router).save_bulk(chunks, conn_meta=tx.meta(), conn_chunks=tx.chunks())

    def _save_embeddings(self, emb: List[Embedding], tx: DBTransaction):
        EmbeddingsRepository(self.router, self.model.info.uid).save_bulk(emb, conn=tx.embeddings(self.model.info.uid))
    
    def _save(self, router: DBRouter, tasks: List[BatchTask[TEntity]]):
        with router.transaction() as tx:
            for task in tasks:
                saver = self._savers[task.action]
                saver(task.entity, tx)