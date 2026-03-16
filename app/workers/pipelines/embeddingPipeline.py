import asyncio
from typing import List
from app.model import Model
from app.infrastructure.db import Migrator, DBRouter
from app.infrastructure.db.repositories import ModelRepository, BookRepository, ChunkRepository, EmbeddingsRepository, AuthorRepository
from app.infrastructure.models import Channel, Stages, Dataset, Book, Chunk, Embedding, BookSearchEngineType
from app.searchEngines.bookSearch import BookSearchEngineFactory
from app.workers.stages import BookProducer, Parser, EmbeddingWorker, TokenizerStage
from .pipeline import Pipeline

BOOK_THREADS: int = 1
CHUNK_THREADS: int = 4
TOKENS_THREADS: int = 4
EMB_THREADS: int = 4

class EmbeddingPipeline(Pipeline):
    def __init__(
        self,
        *args, 
        **kwargs
    ):
        super().__init__(name="embeddings", *args, **kwargs)

        self.model = Model(EMB_THREADS)
        self.search_engine = BookSearchEngineFactory().create(BookSearchEngineType.INPIX, self._stats)

        Migrator(self._router).migrate_all([self.model.info.uid])
        ModelRepository(self._router).get_or_create(self.model.info.uid, self.model.info.model_name)

    async def setup_stages(self) -> None:
        channel_book = Channel(Stages.PARSER, asyncio.Queue(maxsize=50))
        channel_tokens = Channel(Stages.TOKENIZER, asyncio.Queue(100))
        channel_emb = Channel(Stages.EMBEDDING, asyncio.Queue(100))

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
            output_channels=[channel_tokens, *self._output_channels],
            stats=self._stats,
            batch_size=64,
            workers=CHUNK_THREADS,
            logger = self._logger, 
        )
        self.pool.append(chunk_stage)

        embedding_stage = TokenizerStage(
            model=self.model,
            router=self._router,
            input_channel=channel_tokens,
            output_channels=[channel_emb],
            stats=self._stats,
            batch_size=64,
            workers=TOKENS_THREADS,
            logger = self._logger, 
        )
        self.pool.append(embedding_stage)

        embedding_stage = EmbeddingWorker(
            model=self.model,
            router=self._router,
            input_channel=channel_emb,
            output_channels=[*(self._output_channels or [])],
            stats=self._stats,
            batch_size=1,
            workers=EMB_THREADS,
            logger = self._logger, 
        )
        self.pool.append(embedding_stage)

        self._registry.register(Dataset.BOOK, self._save_books_async)
        self._registry.register(Dataset.CHUNK, self._save_chunks_async)
        self._registry.register(Dataset.EMBEDDING, self._save_embeddings_async)

    async def _save_books_async(self, router: DBRouter, books: List[Book]):
        def save(router: DBRouter, books: List[Book]):
            with router.transaction() as tx:
                BookRepository(router).save_bulk(books, conn=tx.meta())
                AuthorRepository(router).save_bulk(books, conn=tx.meta())

        async with router.meta_lock():
            await asyncio.to_thread(save, router, books)

    async def _save_chunks_async(self, router: DBRouter, chunks: List[Chunk]):
        def save(router: DBRouter, chunks: List[Book]):
            with router.transaction() as tx:
                ChunkRepository(router).save_bulk(chunks, conn_meta=tx.meta(), conn_chunks=tx.chunks())

        async with router.chunks_lock():
            await asyncio.to_thread(save, router, chunks)

    async def _save_embeddings_async(self, router: DBRouter, emb: List[Embedding]):
        def save(router: DBRouter, emb: List[Book]):
            with router.transaction() as tx:
                EmbeddingsRepository(router, self.model.info.uid).save_bulk(emb, conn=tx.embeddings(self.model.info.uid))

        async with router.embeddings_lock(self.model.info.uid):
            await asyncio.to_thread(save, router, emb)