import logging
from app.workers.pipelines import EmbeddingPipeline
from app.workers.base import BaseWorker
from app.workers.sources.databaseReporter import DatabaseReporter

class GenerateEmbeddingsWorker(BaseWorker):
    def __init__(self, batch: int):
        super().__init__(name="Generate embeddings", logger=logging.getLogger(__name__))

    async def after_run(self) -> None:
        report = DatabaseReporter(self.router, self.model_uid).generate()
        self.ui.report(report)

    async def setup_stages(self):
        self.ui.model_info = self.model.info
        
        self.pool: List[BaseQueueWorker] = []
        channel_book = Channel(Stages.CHUNK, asyncio.Queue(maxsize=50))
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

        self.chunk_stage = Chunker(
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
            batch_size=128,
            workers=EMB_THREADS,
            logger = self.logger, 
        )
        self.pool.append(self.embedding_stage)

        self.db_stage = DbWorker(
            router=self.router,
            save_func=self._save_async,
            input_channel=channel_db,
            stats=self.stats,
            batch_size=256,
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

    def _save(self, router: DBRouter, tasks: List[BatchTask[TEntity]]):
        with router.transaction(self.model.info.uid) as tx:
            for task in tasks:
                saver = self._savers[task.dataset]
                saver(saver, task.entity, tx)
    
    async def _save_async(self, router: DBRouter, tasks: List[BatchTask[TEntity]]):
        async with router.lock_all(self.model.info.uid) as tx:
            await asyncio.to_thread(self._save, router, tasks)
