import asyncio
from typing import List
from app.searchEngines.bookSearch import BaseBookSearchEngine
from app.workers.base import BaseQueueWorker
from app.parsers.book import BookParserFactory
from app.infrastructure.db import DBRouter
from app.infrastructure.db.repositories import ChunkRepository
from app.infrastructure.models import Task, TaskResult, Book, BookTask, Chunk

class Chunker(BaseQueueWorker[BookTask]):
    def __init__(
            self, 
            router: DBRouter,
            search_engine: BaseBookSearchEngine,
            *args, 
            **kwargs
        ):
        super().__init__(name="Chunker", *args, **kwargs)

        self._engine = search_engine
        self._repo = ChunkRepository(router)
        self._chunk_id = ChunkRepository(router).get_max_id()
        self._batch: List[TaskResult[List[Chunk]]] = []
        self._chunk_to_book_id = self._repo.get_ids()

    async def process(self, batch: List[Task[BookTask]]) -> List[TaskResult[Chunk]]:
        result: List[TaskResult] = []

        for task in batch:
            book = task.entity.book

            if task.entity.data:
                parser = BookParserFactory.create_parser(book.file_name)
                parsed = parser.parse(task.entity.data)
                Book.merge_from(book, parsed)

                if len(book.chunks or []) > 0:
                    for chunk in book.chunks:
                        chunk.book_id = book.id
                        chunk.chunk_id = self._reserve_id()
            else:
                if book.id not in self._chunk_to_book_id:
                    book.chunks = await self._enrich_from_db(book)
            
            for chunk in book.chunks:
                task = TaskResult(
                    id=chunk.chunk_id,
                    name=book.file_name,
                    entity=chunk
                )
                result.append(task)
        return result

    async def _enrich_from_db(self, book: Book) -> List[Chunk]:
        def _sync(book_id: int) -> List[Chunk]:
            chunks = self._repo.get_by_book(book_id)
            return chunks

        return await asyncio.to_thread(_sync, book.id)
    
    def _reserve_id(self) -> int:
        id = self._chunk_id
        self._chunk_id += 1
        return id