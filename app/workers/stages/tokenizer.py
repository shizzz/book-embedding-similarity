import asyncio
from typing import List, Dict
from collections import defaultdict
from app.model import Model
from app.parsers.chunk import ChunkStrategyFactory
from app.workers.base import BaseQueueWorker
from app.infrastructure.db import DBRouter
from app.infrastructure.db.repositories import EmbeddingsRepository
from app.infrastructure.models import Task, Action, Chunk, TokenChunk, Stages, ChunkType

class TokenizerStage(BaseQueueWorker[Chunk]):
    def __init__(
        self,
        model: Model,
        name: str = Stages.TOKENIZER,
        *args,
        **kwargs
    ):
        super().__init__(name=name, *args, **kwargs)

        self._tokenizer = model.tokenizer
        self._max_tokens = model.info.max_seq_length
        self._min_tokens = max(100, int(self._max_tokens * 0.15))
        self._overlap = model.info.st_overlap

        factory = ChunkStrategyFactory()
        self._token_strategies: Dict[ChunkType, any] = {
            chunk_type: factory.create(chunk_type, self._tokenizer)
            for chunk_type in ChunkType
            if chunk_type.supports_tokenization()
        }

    async def process(self, batch: List[Task[Chunk]], wid: int) -> List[Task[TokenChunk]]:
        result: List[Task[TokenChunk]] = []
        chunk_seq_counter = defaultdict(int)

        for task in batch:
            chunk = task.entity
            strategy = self._token_strategies[ChunkType(chunk.type)]

            encoded = await asyncio.to_thread(
                self._tokenizer,
                chunk.text,
                add_special_tokens=False,
                return_attention_mask=False,
                return_token_type_ids=False
            )

            parts = await asyncio.to_thread(
                strategy.prepare(encoded["input_ids"]).split,
                max_tokens=self._max_tokens,
                min_tokens=self._min_tokens,
                overlap=self._overlap,
                single_chunk_mode=False
            )

            for part in parts:
                seq = chunk_seq_counter[(chunk.source_id, chunk.chunk_id)]
                chunk_seq_counter[(chunk.source_id, chunk.chunk_id)] += 1

                chunk_task = Task(
                    id=chunk.chunk_id,
                    name=f"{chunk.source_id}:{chunk.chunk_id}",
                    entity=TokenChunk(
                        book_id=chunk.source_id,
                        chunk_id=chunk.chunk_id,
                        type=chunk.type,
                        tokens=part,
                        seq=seq,
                        length=len(part)
                    ),
                    routes=[Action.EMBEDDING, Action.TAG]
                )

                result.append(chunk_task)

        return result