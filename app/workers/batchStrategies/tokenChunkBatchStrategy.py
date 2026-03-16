from .base_batch_strategy import BaseBatchStrategy
from app.infrastructure.models import Task, TokenChunk
from typing import List

class TokenChunkBatchStrategy(BaseBatchStrategy):
    def __init__(self, max_chars_func):
        self.max_chars_func = max_chars_func
        self.buffer: List[Task[TokenChunk]] = []
        self.current_length = 0
        self.pending_chunk: List[Task[TokenChunk]] = []
        self.pending_chunk_id = None

    def info(self) -> str:
        return str(self.max_chars_func())

    def collect(self, task: Task[TokenChunk]) -> List[Task[TokenChunk]] | None:
        chunk: TokenChunk = task.entity
        chunk_id = chunk.chunk_id

        # Если это новый chunk_id, проверяем pending_chunk
        if self.pending_chunk_id is not None and self.pending_chunk_id != chunk_id:
            # решаем, что делать с накопленным pending_chunk
            pending_length = sum(c.entity.length for c in self.pending_chunk)
            if self.current_length + pending_length > self.max_chars_func():
                if self.buffer:
                    batch = self.buffer
                    self.buffer = self.pending_chunk.copy()
                    self.current_length = pending_length
                else:
                    batch = self.pending_chunk.copy()
                    self.buffer = []
                    self.current_length = 0
                self.pending_chunk = []
                self.pending_chunk_id = None
                # теперь новый task попадет в pending_chunk ниже
                self.pending_chunk.append(task)
                self.pending_chunk_id = chunk_id
                return batch
            else:
                # добавляем pending_chunk в buffer
                self.buffer.extend(self.pending_chunk)
                self.current_length += pending_length
                self.pending_chunk = []

        # накапливаем текущий chunk в pending
        self.pending_chunk.append(task)
        self.pending_chunk_id = chunk_id
        return None

    def flush(self) -> List[Task[TokenChunk]] | None:
        # сначала добавим pending_chunk в buffer
        if self.pending_chunk:
            self.buffer.extend(self.pending_chunk)
            self.current_length += sum(c.entity.length for c in self.pending_chunk)
            self.pending_chunk = []
            self.pending_chunk_id = None

        if self.buffer:
            batch = self.buffer
            self.buffer = []
            self.current_length = 0
            return batch
        return None