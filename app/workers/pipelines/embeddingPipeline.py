import asyncio
from typing import List
from app.model import Model
from app.infrastructure.db import Migrator, DBRouter
from app.infrastructure.db.repositories import ModelRepository, EmbeddingsRepository
from app.infrastructure.models import Channel, Stages, Dataset, Embedding, BookSearchEngineType
from app.searchEngines.bookSearch import BookSearchEngineFactory
from app.workers.stages import DbChunkProducer, EmbeddingWorker, TokenizerStage
from app.workers.skipStrategies import SkipIfInSetStrategy
from .pipeline import Pipeline

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
        channel_tokenizer: Channel = None
        channel_emb = Channel(Stages.EMBEDDING, asyncio.Queue(10))
        
        if self._input_channel is None:
            channel_tokenizer = Channel(Stages.TOKENIZER, asyncio.Queue(10))
            chunk_stage = DbChunkProducer(
                router=self._router,
                output_channels=[channel_tokenizer],
                stats=self._stats,
                batch_size=64,
                workers=TOKENS_THREADS,
                logger = self._logger, 
            )
            self.pool.append(chunk_stage)
        else:
            channel_tokenizer = self._input_channel

        emb_to_chunk_id = EmbeddingsRepository(self._router, self.model.info.uid).get_ids()
        tokenizer_skip_strategy = SkipIfInSetStrategy(
            key_fn=lambda c: c.chunk_id,
            values_set=emb_to_chunk_id
        )
        tokenizer_stage = TokenizerStage(
            model=self.model,

            stats=self._stats,
            batch_size=64,
            workers=TOKENS_THREADS,
            logger=self._logger,
            skip_strategy=tokenizer_skip_strategy,

            input_channel=channel_tokenizer,
            output_channels=[channel_emb],
        )

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

        self.pool.append(tokenizer_stage)
        self.pool.append(embedding_stage)

        self._registry.register(Dataset.EMBEDDING, self._save_embeddings_async)

    async def _save_embeddings_async(self, router: DBRouter, emb: List[Embedding]):
        def save(router: DBRouter, emb: List[Embedding]):
            with router.transaction() as tx:
                EmbeddingsRepository(router, self.model.info.uid).save_bulk(emb, conn=tx.embeddings(self.model.info.uid))

        async with router.embeddings_lock(self.model.info.uid):
            await asyncio.to_thread(save, router, emb)