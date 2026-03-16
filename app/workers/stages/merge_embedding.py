import numpy as np
from typing import List
from app.workers.batchStrategies import BookEmbeddingBatchStrategy
from app.workers.base import BaseQueueWorker
from app.infrastructure.models import Task, Action, Embedding, BookTask, Stages

class EmbeddingMeger(BaseQueueWorker[BookTask]):
    def __init__(
            self, 
            name: str = Stages.MERGER,
            batch_size: int = 100,
            *args, 
            **kwargs
        ):
        super().__init__(
            name=name,
            batch_strategy=lambda: BookEmbeddingBatchStrategy(batch_size),
            *args, 
            **kwargs
        )

    async def process(self, batch: List[Task[Embedding]], wid: int) -> List[Task]:
        result: List[Task[Embedding]] = []
        grouped: dict[int, list[np.ndarray]] = {}
        shape = None

        for r in batch:
            if shape is None:
                shape = r.entity.shape
            vec = r.entity.data
            grouped.setdefault(r.entity.book_id, []).append(vec)

        for book_id, vectors in grouped.items():
            stacked = np.vstack(vectors)
            merged = np.mean(stacked, axis=0)

            task = Task[Embedding](
                id=book_id,
                name=str(book_id),
                entity=Embedding(
                    book_id=book_id,
                    data=merged,
                    shape=shape,
                ),
                routes=Action.INDEX
            )

            result.append(task)

        return result