import asyncio
from typing import List
from app.model import Model
from app.infrastructure.db import DBRouter
from app.infrastructure.db.repositories import GenresRepository, ModelRepository, EmbeddingsRepository
from app.infrastructure.models import Embedding, Channel, Stages, ChunkType, IndexLevel, Dataset
from app.workers.stages import TagProducer, TokenizerStage, EmbeddingWorker, Indexer, CentroidsProducer
from .pipeline import Pipeline
from app.settings import PathsConfig, ProcessConfig

PROD_THREADS: int = 2
INDEX_THREADS: int = 1
TOKENS_THREADS: int = 2
EMB_THREADS: int = 2

class TagIndexerPipeline(Pipeline):
    def __init__(
        self,
        centros: int,
        recreate: bool,
        *args, 
        **kwargs
    ):
        super().__init__(name="indexer", *args, **kwargs)

        self._centros = centros
        self._recreate = recreate
        
        self.model = Model(EMB_THREADS)
        self.model_id = ModelRepository(self._router).get_or_create(self.model.info.uid, self.model.info.model_name)
        self._shape = EmbeddingsRepository(self._router, self.model.info.uid).get_shape()

    def _check_index(self, level: IndexLevel) -> bool:  
        path = PathsConfig.DATA_DIR / f"{ProcessConfig.MODEL_NAME}.{level}.faiss"
        return not path.exists() or self._recreate

    async def setup_stages(self) -> None:
        if not self._check_index(IndexLevel.TAGS) and not self._check_index(IndexLevel.CENTROIDS):
            for ch in self._output_channels:
                ch.upstream_done.set()
            return
        channel_tokenizer = Channel(Stages.TOKENIZER, asyncio.Queue(10))
        channel_emb = Channel(Stages.EMBEDDING, asyncio.Queue(10))
        channel_tags = Channel(f"{Stages.INDEX}_{IndexLevel.TAGS}", asyncio.Queue(10))
        channel_centroids = Channel(f"{Stages.INDEX}_{IndexLevel.CENTROIDS}", asyncio.Queue(10))

        tag_out_channels = [channel_tags]
        centroids_out_channels = [channel_centroids]

        db_channel = next(
            (ch for ch in self._output_channels if ch.downstream == Stages.DB),
            None
        )

        if db_channel:
            tag_out_channels.append(db_channel)
            centroids_out_channels.append(db_channel)
        
        genres_repo = GenresRepository(self._router)
        
        # centroids route
        centroid_producer_stage = CentroidsProducer(
            router=self._router,
            centros=self._centros,

            stats=self._stats,
            logger=self._logger, 

            output_channels=centroids_out_channels,
        )
        self.pool.append(centroid_producer_stage)

        # human readable route
        tag_producer_stage = TagProducer(
            repo=genres_repo,
            type=ChunkType.TAG,

            batch_size=10,
            stats=self._stats,
            workers=PROD_THREADS,
            logger = self._logger, 

            output_channels=[channel_tokenizer],
        )
        self.pool.append(tag_producer_stage)

        embedding_stage = TokenizerStage(
            model=self.model,
            router=self._router,
            
            stats=self._stats,
            batch_size=1,
            workers=TOKENS_THREADS,
            logger = self._logger,

            input_channel=channel_tokenizer,
            output_channels=[channel_emb],
        )
        self.pool.append(embedding_stage)

        embedding_stage = EmbeddingWorker(
            model=self.model,
            router=self._router,
            
            stats=self._stats,
            batch_size=1,
            workers=EMB_THREADS,
            logger = self._logger,

            input_channel=channel_emb,
            output_channels=tag_out_channels,
        )
        self.pool.append(embedding_stage)

        # Indexes
        tag_indexer = Indexer(
            router=self._router,
            shape=self._shape,

            level=IndexLevel.TAGS,
            batch_size=20000,
            stats=self._stats,
            workers=INDEX_THREADS,
            logger = self._logger,

            input_channel=channel_tags,
            output_channels=[*(self._output_channels or [])],
        )
        self.pool.append(tag_indexer)

        centroid_indexer = Indexer(
            router=self._router,
            level=IndexLevel.CENTROIDS,
            shape=self._shape,

            batch_size=20000,
            stats=self._stats,
            workers=INDEX_THREADS,
            logger = self._logger,

            input_channel=channel_centroids,
            output_channels=[*(self._output_channels or [])],
        )
        self.pool.append(centroid_indexer)

        self._registry.register(Dataset.EMBEDDING, self._save_embeddings_async)

    async def _save_embeddings_async(self, router: DBRouter, emb: List[Embedding]):
        def save(router: DBRouter, emb: List[Embedding]):
            with router.transaction() as tx:
                EmbeddingsRepository(router, self.model.info.uid).save_bulk(emb, conn=tx.embeddings(self.model.info.uid))

        async with router.embeddings_lock(self.model.info.uid):
            await asyncio.to_thread(save, router, emb)