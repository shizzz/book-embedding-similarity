import asyncio
from asyncio import to_thread
from typing import List, Tuple
from app.workers import BaseWorker
from app.services import BulkSimilarSearchService
from app.models import Task, Book, Task
from app.db import db, BookRepository, SimilarRepository
from app.searchEngines.similarSearch import SimilarSearchEngineFactory
from app.settings.config import SIMILARS_PER_BOOK

class GenerateSimilarWorker(BaseWorker):
    _service: BulkSimilarSearchService
    _limit: int = SIMILARS_PER_BOOK

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self._task_total: int = 0

    async def process(self, task: Task) -> Tuple[int, List[Tuple[float, int, int]]]:
        return (1, await to_thread(self._service.run, task.entity[0], task.entity[1]))

    async def prepare(self) -> None:
        self.logger.info(f"Очистка таблицы similar")

        with db() as conn:
            SimilarRepository.clear(conn)

            self.logger.info(f"Получение всех книг из базы данных")
            books_with_embeddings = list(
                await asyncio.to_thread(BookRepository.get_all_with_embeddings, conn)
            )

            self.logger.info(f"Фильтрация книг и эмбеддингов по ID")
            valid_books: List[Book] = []
            valid_embeddings: List[bytes] = []

            for book_id, book_name, title, author, _, _, embedding in books_with_embeddings:
                valid_books.append(Book(id=book_id, file_name=book_name, title=title, author=author))
                valid_embeddings.append(embedding)
                self._task_total += 1

        engine = SimilarSearchEngineFactory.create(SimilarSearchEngineFactory.INDEX, SIMILARS_PER_BOOK, False, 1)

        self._service = BulkSimilarSearchService(
            engine,
            valid_books,
            valid_embeddings,
            logger=self.logger
        )

        self.logger.info(f"Добавление книг и эмбеддингов в очередь")
        
        for book_id, book_name, title, _, _, _, embedding in books_with_embeddings:
            self.queue.put_nowait(
                Task(
                    name=book_name,
                    entity=(
                        Book(
                            id=book_id,
                            file_name=book_name,
                            title=title
                        ),
                        embedding,
                    )
                )
            )

        del books_with_embeddings

    async def get_total(self) -> int:
        return self._task_total
    
    async def fin(self) -> None:
        return

    def save_to_db(self, buffer: List[Tuple[float, int, int]]) -> None:
        with db() as conn:
            SimilarRepository.save(conn, buffer)
        return len(buffer)
