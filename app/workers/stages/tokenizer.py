import asyncio
from typing import List
from collections import defaultdict
from app.model import Model
from app.parsers.chunk import ChunkStrategyFactory
from app.workers.base import BaseQueueWorker
from app.infrastructure.models import Task, Action, Chunk, TokenChunk, Stages

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

    async def process(self, batch: List[Task[Chunk]], wid: int) -> List[Task[TokenChunk]]:
        result: List[Task[TokenChunk]] = []
        chunk_seq_counter = defaultdict(int)

        for task in batch:
            chunk = task.entity
            strategy = ChunkStrategyFactory().create(chunk.type)

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
                seq = chunk_seq_counter[(chunk.book_id, chunk.chunk_id)]
                chunk_seq_counter[(chunk.book_id, chunk.chunk_id)] += 1

                chunk_task = Task(
                    id=chunk.chunk_id,
                    name=f"{chunk.book_id}:{chunk.chunk_id}",
                    entity=TokenChunk(
                        book_id=chunk.book_id,
                        chunk_id=chunk.chunk_id,
                        type=chunk.type,
                        tokens=part,
                        seq=seq,
                        length=len(part)
                    ),
                    routes=Action.EMBEDDING
                )

                result.append(chunk_task)

        return result