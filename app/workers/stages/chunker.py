import asyncio
from typing import List
from app.searchEngines.bookSearch import BaseBookSearchEngine
from app.workers.base import BaseQueueWorker
from app.parsers.book import BookParserFactory
from app.infrastructure.db import DBRouter
from app.infrastructure.db.repositories import ChunkRepository
from app.infrastructure.models import Task, Book, BookTask, Chunk, Action, Channel, Stages

ROUTES = {
    Action.BOOK: {Stages.DB},
    Action.CHUNK: {Stages.EMBEDDING, Stages.DB},
}

class Chunker(BaseQueueWorker[BookTask]):
    def __init__(
            self, 
            router: DBRouter,
            search_engine: BaseBookSearchEngine,
            name: str = Stages.CHUNK,
            *args, 
            **kwargs
        ):
        super().__init__(name=name, *args, **kwargs)

        self._engine = search_engine
        self._repo = ChunkRepository(router)
        self._chunk_id = self._repo.get_max_id()
        self._chunk_id_lock = asyncio.Lock()
        self._batch: List[Task[List[Chunk]]] = []
        self._chunk_to_book_id = self._repo.get_ids()

    async def process(self, batch: List[Task[BookTask]], wid: int) -> List[Task]:
        result: List[Task] = []

        for task in batch:
            book = task.entity.book

            if task.entity.data:
                parser = BookParserFactory.create_parser(book.file_name)
                parsed = parser.parse(task.entity.data)
                Book.merge_from(book, parsed)

                task = Task(
                    id=book.id,
                    name=book.file_name,
                    entity=book,
                    action=Action.BOOK
                )

                if len(book.chunks or []) > 0:
                    for chunk in book.chunks:
                        chunk.book_id = book.id
                        chunk.chunk_id = await self._reserve_id()
            else:
                if book.id not in self._chunk_to_book_id:
                    book.chunks = await self._enrich_from_db(book)
            
            for chunk in book.chunks:
                task = Task(
                    id=chunk.chunk_id,
                    name=book.file_name,
                    entity=chunk,
                    action=Action.CHUNK
                )
                result.append(task)
        return result

    def route(self, task: Task, channels: list[Channel]) -> list[Channel]:
        allowed = ROUTES.get(task.action, set())
        return [ch for ch in channels if ch.downstream in allowed]

    async def _enrich_from_db(self, book: Book) -> List[Chunk]:
        def _sync(book_id: int) -> List[Chunk]:
            chunks = self._repo.get_by_book(book_id)
            return chunks

        return await asyncio.to_thread(_sync, book.id)

    async def _reserve_id(self) -> int:
        async with self._chunk_id_lock:
            id = self._chunk_id
            self._chunk_id += 1
            return id