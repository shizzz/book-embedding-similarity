import asyncio
import numpy as np
from faiss import IndexIDMap
from typing import List
from app.hnsw import IndexManager, FaissId
from app.workers.base import BaseQueueWorker
from app.infrastructure.models import Task, Embedding, Stages, BookTag, Action, ChunkType, Dataset
from app.settings import IndexLevel

class BookTagger(BaseQueueWorker):
    def __init__(
            self, 
            model_id: int,
            top_k: int = 1000,
            threshold: float = 0.8,
            name: str = Stages.TAG,
            *args, 
            **kwargs
        ):
        super().__init__(
            name=name,
            *args, 
            **kwargs
        )

        hnsw = IndexManager(logger=self.logger)
        self._chunk_index: IndexIDMap = hnsw.load_from_file(IndexLevel.CHUNK)
        self._unpacker = FaissId.unpack_book
        self.model_id = model_id
        self.top_k = top_k
        self.threshold = threshold

    async def process(self, batch: List[Task[Embedding]], wid: int) -> List[BookTag]:
        """
        Батчевый поиск тегов по книгам с Faiss.
        Используется to_thread, чтобы не блокировать event loop.
        """
        tag_embeddings = np.stack([e.entity.data for e in batch])  # shape (B, d)
        tag_ids = [e.entity.source_id for e in batch]
        tag_types = [e.entity.type for e in batch]

        # Функция для синхронного поиска и постобработки
        def _search_and_process():
            result_dict: dict[int, BookTag] = {}

            # Поиск в Faiss
            D, I = self._chunk_index.search(tag_embeddings, k=self.top_k)  # D, I shape = (B, k)

            for tag_idx, tag_id in enumerate(tag_ids):
                for score, packed_id in zip(D[tag_idx], I[tag_idx]):
                    if packed_id == -1:
                        continue

                    if score < self.threshold:
                        continue

                    book_id = FaissId.unpack_book(packed_id)

                    # Оставляем только максимальный score для каждой книги
                    if book_id not in result_dict or score > result_dict[book_id].distance:
                        result_dict[book_id] = BookTag(
                                book_id=int(book_id), 
                                genre_id=int(tag_id),
                                model_id=self.model_id,
                                distance=float(score),
                                type=tag_types[tag_idx],
                            )

            return list(result_dict.values())

        # Запускаем синхронную работу в отдельном потоке
        result = await asyncio.to_thread(_search_and_process)
        return [
            Task(
                id=r.id,
                name=str(r.id),
                entity=r,
                cost=100,
                routes=[Action.DB],
                dataset=Dataset.TAG if r.type == ChunkType.TAG else Dataset.CENROID,
            )
            for r in result
        ]