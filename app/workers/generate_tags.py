import asyncio
import logging
from app.infrastructure.db.repositories import BookTagsRepository, EmbeddingsRepository
from app.infrastructure.models import Channel, Stages, ChunkType
from app.workers.pipelines import TagIndexerPipeline, TaggerPipeline, DbPipeline
from app.workers.base import BaseWorker

class GenerateTags(BaseWorker):
    def __init__(
            self,
            centros: int,
            threshold: float,
            recreate: bool,
            *args,
            **kwargs
        ):
        super().__init__(name="Generate tags", logger=logging.getLogger(__name__), *args, **kwargs)

        self._channel_tag = Channel(Stages.TAG, asyncio.Queue(maxsize=0))
        self._channel_db = Channel(Stages.DB, asyncio.Queue(maxsize=400))
        self._centros = centros
        self._threshold = threshold
        self._recreate = recreate

        self._model_uid = None
        self._model_id = None

    async def after_run(self) -> None:
        pass

    async def before_run(self) -> None:
        BookTagsRepository(self.router, BookTagsRepository.GENRES_TABLE).delete_by_model(self._model_id)
        BookTagsRepository(self.router, BookTagsRepository.CENTOIDS_TABLE).delete_by_model(self._model_id)
        
        if self._recreate:
            EmbeddingsRepository(self.router, self._model_uid).delete_by_type(ChunkType.CENTROID)
            EmbeddingsRepository(self.router, self._model_uid).delete_by_type(ChunkType.TAG)

    async def setup_stages(self):
        tag_indexer_pipeline = TagIndexerPipeline(
            centros=self._centros,
            recreate=self._recreate,

            router=self.router,
            registry=self.registry,
            stats=self.stats,
            logger=self.logger,

            output_channels = [self._channel_tag, self._channel_db],
        )

        self._model_id = tag_indexer_pipeline.model_id
        self._model_uid = tag_indexer_pipeline.model.info.uid

        tagger_pipeline = TaggerPipeline(
            threshold=self._threshold,
            model_id=tag_indexer_pipeline.model_id,

            router=self.router,
            registry=self.registry,
            stats=self.stats,
            logger=self.logger,

            upstream_done=self._channel_tag.upstream_done,
            output_channels = [self._channel_db],
        )

        dbPipeline = DbPipeline(
            model_name=tag_indexer_pipeline.model.info.model_name,
            model_uid=tag_indexer_pipeline.model.info.uid,
            threads=1,
            batch_size=20000,

            router=self.router,
            registry=self.registry,
            stats=self.stats,
            logger=self.logger,

            input_channel=self._channel_db,
        )

        self.pipelines.append(tag_indexer_pipeline)
        self.pipelines.append(tagger_pipeline)
        self.pipelines.append(dbPipeline)
