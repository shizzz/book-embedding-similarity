import asyncio
from app.infrastructure.models import Channel, Stages
from app.workers.stages import EmbeddingProducer, EmbeddingMeger, Indexer
from app.settings import IndexLevel
from .pipeline import Pipeline

THREADS: int = 1

class IndexPipeline(Pipeline):
    def __init__(
        self,
        level: IndexLevel,
        *args, 
        **kwargs
    ):
        super().__init__(name="indexer", *args, **kwargs)
        self.level = level

    async def setup_stages(self) -> None:
        emb_channels: list[Channel] = []
        if self.level == IndexLevel.BOTH or self.level == IndexLevel.DOCUMENT:
            emb_channel_document = Channel(Stages.MERGER, asyncio.Queue(maxsize=10000))
            emb_channels.append(emb_channel_document)
        if self.level == IndexLevel.BOTH or self.level == IndexLevel.CHUNK:
            emb_channel_index = Channel(f"{Stages.INDEX}_{IndexLevel.CHUNK.value}", asyncio.Queue(maxsize=10000))
            emb_channels.append(emb_channel_index)

        emb_stage = EmbeddingProducer(
            router=self._router,
            batch_size=10,
            input_channel=self._input_channel,
            output_channels=emb_channels,
            stats=self._stats,
            workers=THREADS,
            logger = self._logger, 
        )
        self.pool.append(emb_stage)

        if self.level == IndexLevel.BOTH or self.level == IndexLevel.DOCUMENT:
            merged_channel = Channel(f"{Stages.INDEX}_{IndexLevel.DOCUMENT.value}", asyncio.Queue(10000))
            merge_stage = EmbeddingMeger(
                batch_size=500,
                input_channel=emb_channel_document,
                output_channels=[merged_channel],
                stats=self._stats,
                workers=THREADS,
                logger = self._logger, 
            )
            self.pool.append(merge_stage)

            doc_index_stage = Indexer(
                router=self._router,
                level=IndexLevel.DOCUMENT,
                batch_size=5000,
                input_channel=merged_channel,
                output_channels=self._output_channels,
                stats=self._stats,
                workers=THREADS,
                logger = self._logger, 
            )
            self.pool.append(doc_index_stage)

        if self.level == IndexLevel.BOTH or self.level == IndexLevel.CHUNK:
            chunk_index_stage = Indexer(
                router=self._router,
                level=IndexLevel.CHUNK,
                batch_size=5000,
                input_channel=emb_channel_index,
                output_channels=self._output_channels,
                stats=self._stats,
                workers=THREADS,
                logger = self._logger, 
            )
            self.pool.append(chunk_index_stage)