import asyncio
from typing import List, Tuple
from app.infrastructure.db import DBRouter
from app.infrastructure.db.repositories import SimilarRepository
from app.infrastructure.models import Channel, Stages, BookSearchEngineType, SimilarSearchEngineType, BatchTask
from app.searchEngines.bookSearch import BookSearchEngineFactory
from app.searchEngines.similarSearch import SimilarSearchEngineFactory
from app.workers.stages import BookProducer, SimilarStage, DbWorker
from .pipeline import Pipeline

THREADS: int = 1
TOP_K: int = 100
EXCLUDE_SAME_AUTHOR: bool = False
SEARCH_BATCH_SIZE: int = 100
DB_BATCH_SIZE: int = 1000

class SimilarSearchPipeline(Pipeline):
    def __init__(
        self,
        *args, 
        **kwargs
    ):
        super().__init__(name="similar", *args, **kwargs)
        self.book_search_engine = BookSearchEngineFactory().create(BookSearchEngineType.DB, self._stats, self._router)
        self.similar_search_engine = SimilarSearchEngineFactory.create(
            mode=SimilarSearchEngineType.INDEX,
            router=self._router,
            limit=TOP_K,
            exclude_same_authors=EXCLUDE_SAME_AUTHOR,
            logger=self._logger,
        )
        self._repo = SimilarRepository(self._router)

    async def setup_stages(self) -> None:
        channel_book = Channel(Stages.SIMILAR, asyncio.Queue(maxsize=SEARCH_BATCH_SIZE))
        channel_db = Channel(Stages.DB, asyncio.Queue(maxsize=DB_BATCH_SIZE))

        book_stage = BookProducer(
            router=self._router,
            search_engine=self.book_search_engine,
            input_channel=self._input_channel,
            output_channels=[channel_book],
            stats=self._stats,
            batch_size=10,
            workers=THREADS,
            logger = self._logger, 
        )
        self.pool.append(book_stage)

        similar_stage = SimilarStage(
            engine=self.similar_search_engine,
            input_channel=channel_book,
            output_channels=[channel_db],
            stats=self._stats,
            batch_size=SEARCH_BATCH_SIZE,
            workers=THREADS,
            logger = self._logger, 
        )
        self.pool.append(similar_stage)

        db_stage = DbWorker(
            router=self._router,
            save_func=self._save_async,
            input_channel=channel_db,
            output_channels=self._output_channels,
            stats=self._stats,
            batch_size=DB_BATCH_SIZE,
            workers=THREADS,
            logger = self._logger, 
        )
        self.pool.append(db_stage)
    
    def _save(self, threads: int, router: DBRouter, tasks: List[BatchTask[Tuple[float, int, int]]]):
        for task in tasks:
            for e in task.entity:
                self._repo.save(e)

    async def _save_async(self, threads: int, router: DBRouter, tasks: List[BatchTask[Tuple[float, int, int]]]):
        if threads > 1:
            async with router.meta_lock():
                await asyncio.to_thread(self._save, threads, router, tasks)
        else:
            await asyncio.to_thread(self._save, threads, router, tasks)