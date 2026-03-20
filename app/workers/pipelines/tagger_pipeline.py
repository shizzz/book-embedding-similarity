import asyncio
from typing import List
from app.model import Model
from app.infrastructure.db import Migrator, DBRouter
from app.infrastructure.db.repositories import BookTagsRepository, GenresRepository, ModelRepository, EmbeddingsRepository
from app.infrastructure.models import Channel, Stages, ChunkType, BookTag, Dataset, Embedding
from app.workers.stages import TagProducer, TokenizerStage, EmbeddingWorker, BookTagger
from .pipeline import Pipeline

PROD_THREADS: int = 2
INDEX_THREADS: int = 1
TOKENS_THREADS: int = 2
EMB_THREADS: int = 2
TOP_K: int = 1000

class TaggerPipeline(Pipeline):
    def __init__(
        self,
        *args, 
        **kwargs
    ):
        super().__init__(name="indexer", *args, **kwargs)

        self.model = Model(EMB_THREADS)
        Migrator(self._router).migrate_all([self.model.info.uid])
        self._model_id = ModelRepository(self._router).get_or_create(self.model.info.uid, self.model.info.model_name)

    async def setup_stages(self) -> None:
        channel_tokenizer = Channel(Stages.TOKENIZER, asyncio.Queue(10))
        channel_emb = Channel(Stages.EMBEDDING, asyncio.Queue(10))
        channel_tagger = self._input_channel or Channel(Stages.TAG, asyncio.Queue(10))

        genres_repo = GenresRepository(self._router)

        tag_producer_stage = TagProducer(
            repo=genres_repo,
            type=ChunkType.TAG,
            batch_size=10,
            output_channels=[channel_tokenizer, channel_tagger],
            stats=self._stats,
            workers=PROD_THREADS,
            logger = self._logger, 
        )
        self.pool.append(tag_producer_stage)

        # human readable route
        embedding_stage = TokenizerStage(
            model=self.model,
            router=self._router,
            input_channel=channel_tokenizer,
            output_channels=[channel_emb],
            stats=self._stats,
            batch_size=1,
            workers=TOKENS_THREADS,
            logger = self._logger, 
        )
        self.pool.append(embedding_stage)

        embedding_stage = EmbeddingWorker(
            model=self.model,
            router=self._router,
            input_channel=channel_emb,
            output_channels=[channel_tagger],
            stats=self._stats,
            batch_size=1,
            workers=EMB_THREADS,
            logger = self._logger, 
        )
        self.pool.append(embedding_stage)

        # search and apply tags
        embedding_stage = BookTagger(
            model_id=self._model_id,
            top_k=TOP_K,
            input_channel=channel_tagger,
            output_channels=[*(self._output_channels or [])],
            stats=self._stats,
            batch_size=1,
            workers=EMB_THREADS,
            logger=self._logger,
        )
        self.pool.append(embedding_stage)

        self._registry.register(Dataset.TAG, self._save_tag_async)
        self._registry.register(Dataset.CENROID, self._save_centroid_async)
        self._registry.register(Dataset.EMBEDDING, self._save_embeddings_async)


    def _save(self, router: DBRouter, tags: List[BookTag], table: str):
        BookTagsRepository(router, table).create_many(tags)

    async def _save_tag_async(self, router: DBRouter, tags: List[BookTag]):
        async with router.meta_lock():
            await asyncio.to_thread(self._save, router, tags, BookTagsRepository.GENRES_TABLE)

    async def _save_centroid_async(self, router: DBRouter, tags: List[BookTag]):
        async with router.meta_lock():
            await asyncio.to_thread(self._save, router, tags, BookTagsRepository.CENTOIDS_TABLE)

    async def _save_embeddings_async(self, router: DBRouter, emb: List[Embedding]):
        def save(router: DBRouter, emb: List[Embedding]):
            with router.transaction() as tx:
                EmbeddingsRepository(router, self.model.info.uid).save_bulk(emb, conn=tx.embeddings(self.model.info.uid))

        async with router.embeddings_lock(self.model.info.uid):
            await asyncio.to_thread(save, router, emb)