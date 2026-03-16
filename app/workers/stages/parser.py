import asyncio
from typing import List
from app.searchEngines.bookSearch import BaseBookSearchEngine
from app.workers.base import BaseQueueWorker
from app.parsers.book import BookParserFactory
from app.infrastructure.db import DBRouter
from app.infrastructure.db.repositories import ChunkRepository
from app.infrastructure.models import Task, Book, BookTask, Chunk, Action, Channel, Stages, Dataset

ROUTES = {
    Action.EMBEDDING: {Stages.TOKENIZER},
    Action.DB: {Stages.DB},
    Action.NONE: set(),
}

class Parser(BaseQueueWorker[BookTask]):
    def __init__(
            self, 
            router: DBRouter,
            search_engine: BaseBookSearchEngine,
            name: str = Stages.PARSER,
            *args, 
            **kwargs
        ):
        super().__init__(name=name, *args, **kwargs)

        self._engine = search_engine
        self._repo = ChunkRepository(router)
        self._chunk_id = self._repo.get_max_id()
        self._chunk_id_lock = asyncio.Lock()
        self._chunk_to_book_id = self._repo.get_ids()

    async def process(self, batch: List[Task[BookTask]], wid: int) -> List[Task]:
        result: List[Task] = []

        for task in batch:
            chunk_actions = [Action.EMBEDDING]
            parsed = None
            book = task.entity.book
            chunks: List[Chunk] = []

            if task.entity.data:
                parser = BookParserFactory.create_parser(book.file_name)
                parsed = parser.parse(task.entity.data)
                chunks = parsed.chunks
                Book.merge_from(book, parsed.book)

                book.source_length = len(task.entity.data)
                if parsed.book.empty is not None:
                    book.empty = parsed.book.empty

                book_task = Task(
                    id=book.id,
                    name=book.file_name,
                    entity=book,
                    routes=Action.DB,
                    dataset=Dataset.BOOK,
                    cost=100,
                )
                result.append(book_task)

                if len(chunks or []) > 0:
                    chunk_actions.append(Action.DB)
                    for chunk in chunks:
                        chunk.book_id = book.id
                        chunk.chunk_id = await self._reserve_id()

            if task.entity.data is None:      
                if book.empty:
                    result.append(
                        Task(
                            id=0, 
                            name="Done",
                            entity=None, 
                            routes=Action.NONE,
                        )
                    )
                    continue

                if book.id in self._chunk_to_book_id:
                    chunks = await self._enrich_from_db(book)
            
            for chunk in chunks:
                chunk_task = Task(
                    id=chunk.chunk_id,
                    name=book.file_name,
                    entity=chunk,
                    routes=chunk_actions,
                    dataset=Dataset.CHUNK,
                    cost=len(chunk.text) * 2,
                )
                result.append(chunk_task)
        return result

    def route(self, task: Task, channels: list[Channel]) -> list[Channel]:
        allowed = set()
        for action in task.routes:
            allowed |= ROUTES.get(action, set())

        return [ch for ch in channels if ch.downstream in allowed]

    async def _enrich_from_db(self, book: Book) -> List[Chunk]:
        def _sync(book_id: int) -> List[Chunk]:
            return self._repo.get_by_book(book_id)

        return await asyncio.to_thread(_sync, book.id)

    async def _reserve_id(self) -> int:
        async with self._chunk_id_lock:
            id = self._chunk_id
            self._chunk_id += 1
            return id