import asyncio
import numpy as np
from faiss import IndexIDMap
from typing import List
from app.hnsw import IndexManager
from app.workers.batchStrategies import BookEmbeddingBatchStrategy
from app.workers.base import BaseQueueWorker
from app.infrastructure.models import Task, Embedding, Stages, BookTag, Action, ChunkType, Dataset, IndexLevel

class BookTagger(BaseQueueWorker):
    def __init__(
            self, 
            model_id: int,
            type: IndexLevel,
            top_k: int = 1000,
            threshold: float = 0.1,
            name: str = Stages.TAG,
            batch_size: int = 100,
            *args, 
            **kwargs
        ):
        super().__init__(
            name=f"{name}_{type}",
            batch_strategy=lambda: BookEmbeddingBatchStrategy(batch_size),
            *args, 
            **kwargs
        )
        
        self.model_id = model_id
        self.top_k = top_k
        self.threshold = threshold
        self._type = type
        self._chunk_type = ChunkType.TAG if type == IndexLevel.TAGS else ChunkType.CENTROID
        self._index = None

    def create_index(self):
        hnsw = IndexManager(logger=self.logger)
        self._index: IndexIDMap = hnsw.load_from_file(self._type)

    async def before_start(self):
        if self._index is None:
            await asyncio.to_thread(self.create_index)

    async def process(self, batch: List[Task[Embedding]], wid: int) -> List[BookTag]:
        """
        Батчевый поиск тегов по книгам с Faiss.
        Сохраняем для каждой книги максимальный score на каждый тег.
        """
        embeddings = np.stack([e.entity.data for e in batch])
        emb_source_ids = [e.entity.source_id for e in batch]

        def _search_and_process() -> list[BookTag]:
            # book_id -> tag_id -> BookTag
            result_dict: dict[int, dict[int, BookTag]] = {}

            # Поиск в Faiss
            lims, D_flat, I_flat = self._index.range_search(embeddings, self.threshold)

            for emb_idx, emb_source_id in enumerate(emb_source_ids):
                start = lims[emb_idx]
                end = lims[emb_idx + 1]

                scores = D_flat[start:end]
                tag_ids = I_flat[start:end]

                if emb_source_id not in result_dict:
                    result_dict[emb_source_id] = {}

                for score, tag_id in zip(scores, tag_ids):
                    if tag_id == -1:
                        continue

                    existing = result_dict[emb_source_id].get(tag_id)
                    if existing is None or score > existing.distance:
                        result_dict[emb_source_id][tag_id] = BookTag(
                            book_id=int(emb_source_id),
                            genre_id=int(tag_id),
                            model_id=self.model_id,
                            distance=float(score),
                            type=self._chunk_type,
                        )

            # Flatten в список
            all_tags = []
            for book_tags in result_dict.values():
                all_tags.extend(book_tags.values())

            return all_tags

        # Запускаем в отдельном потоке
        tags = await asyncio.to_thread(_search_and_process)

        # Оборачиваем в Task для дальнейшей обработки
        return [
            Task(
                id=r.book_id,
                name=str(r.book_id),
                entity=r,
                cost=100,
                routes=[Action.DB],
                dataset=Dataset.TAG if r.type == ChunkType.TAG else Dataset.CENROID,
            )
            for r in tags
        ]