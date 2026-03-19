import asyncio
from typing import List
from app.parsers.book import ParserConfig
from app.infrastructure.db import Migrator, DBRouter
from app.infrastructure.db.repositories import BookRepository, ChunkRepository, AuthorRepository
from app.infrastructure.models import Channel, Stages, Dataset, Book, Chunk, BookSearchEngineType
from app.searchEngines.bookSearch import BookSearchEngineFactory
from app.workers.stages import TagProducer, Parser
from .pipeline import Pipeline

BOOK_THREADS: int = 1
CHUNK_THREADS: int = 4

class TagPipeline(Pipeline):
    def __init__(
        self,
        *args, 
        **kwargs
    ):
        super().__init__(name="tag scanner", *args, **kwargs)

        Migrator(self._router).migrate_all([])
        

    async def setup_stages(self) -> None:
        channel_tag = Channel(Stages.PARSER, asyncio.Queue(maxsize=50))

        tag_genres_stage = TagProducer(
            input_channel=self._input_channel,
            output_channels=[channel_tag],
            stats=self._stats,
            batch_size=10,
            workers=BOOK_THREADS,
            logger = self._logger, 
        )
        self.pool.append(tag_genres_stage)

        parser_stage = Parser(
            router=self._router,
            search_engine=self.search_engine,
            cnf=self._cnf,
            input_channel=channel_tag,
            output_channels=[*(self._output_channels or [])],
            stats=self._stats,
            batch_size=64,
            workers=CHUNK_THREADS,
            logger = self._logger, 
        )
        self.pool.append(parser_stage)

        self._registry.register(Dataset.BOOK, self._save_books_async)
        self._registry.register(Dataset.CHUNK, self._save_chunks_async)

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