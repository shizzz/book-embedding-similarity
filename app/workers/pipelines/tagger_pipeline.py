import asyncio
from typing import List
from app.infrastructure.db import DBRouter
from app.infrastructure.db.repositories import BookTagsRepository
from app.infrastructure.models import Channel, Stages, BookTag, Dataset, IndexLevel
from app.workers.stages import EmbeddingProducer, BookTagger
from .pipeline import Pipeline

THREADS: int = 4

class TaggerPipeline(Pipeline):
    def __init__(
        self,
        threshold: float,
        model_id: int,
        *args, 
        **kwargs
    ):
        super().__init__(name="indexer", *args, **kwargs)

        self._threshold = threshold
        self._model_id = model_id

    async def setup_stages(self) -> None:
        channel_tag = Channel(f"{Stages.TAG}_{IndexLevel.TAGS}", asyncio.Queue(maxsize=10000))
        channel_centroids = Channel(f"{Stages.TAG}_{IndexLevel.CENTROIDS}", asyncio.Queue(maxsize=10000))

        emb_stage = EmbeddingProducer(
            router=self._router,

            batch_size=10000,
            workers=THREADS,
            stats=self._stats,
            logger = self._logger,
            
            input_channel=self._input_channel,
            output_channels=[channel_tag, channel_centroids],
        )

        tagger_tag = BookTagger(
            model_id=self._model_id,
            type=IndexLevel.TAGS,
            threshold=self._threshold,
            
            batch_size=50,
            workers=THREADS,
            stats=self._stats,
            logger=self._logger,

            input_channel=channel_tag,
            output_channels=[*(self._output_channels or [])],
        )

        tagger_centroid = BookTagger(
            model_id=self._model_id,
            type=IndexLevel.CENTROIDS,
            threshold=self._threshold,
            
            batch_size=50,
            workers=THREADS,
            stats=self._stats,
            logger=self._logger,

            input_channel=channel_centroids,
            output_channels=[*(self._output_channels or [])],
        )

        self.pool.append(emb_stage)
        self.pool.append(tagger_tag)
        self.pool.append(tagger_centroid)

        self._registry.register(Dataset.TAG, self._save_tag_async)
        self._registry.register(Dataset.CENROID, self._save_centroid_async)

    def _save(self, router: DBRouter, tags: List[BookTag], table: str):
        BookTagsRepository(router, table).create_many(tags)

    async def _save_tag_async(self, router: DBRouter, tags: List[BookTag]):
        async with router.meta_lock():
            await asyncio.to_thread(self._save, router, tags, BookTagsRepository.GENRES_TABLE)

    async def _save_centroid_async(self, router: DBRouter, tags: List[BookTag]):
        async with router.meta_lock():
            await asyncio.to_thread(self._save, router, tags, BookTagsRepository.CENTOIDS_TABLE)