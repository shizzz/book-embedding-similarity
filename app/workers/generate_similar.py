import asyncio
from asyncio import to_thread
from typing import List
from app.workers import BaseWorker
from app.services import BulkSimilarSearchService
from app.models import Task, Book, Task
from app.db import db, BookRepository, SimilarRepository
from app.searchEngines.similarSearch import SimilarSearchEngineFactory
from app.settings.config import SIMILARS_PER_BOOK, DATABASE_QUEUE_BATCH_SIZE

class GenerateSimilarWorker(BaseWorker):
    _service: BulkSimilarSearchService
    _limit: int = SIMILARS_PER_BOOK

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self._smilar_queue_size: int = 0
        self._smilar_queue: asyncio.Queue = asyncio.Queue()
        self._stop_event = asyncio.Event()
        self._queue_batch_size: int = DATABASE_QUEUE_BATCH_SIZE
        self._save_task: asyncio.Task | None = None
        self._task_total: int = 0

    def _save_similar(self, buffer):
        with db() as conn:
            SimilarRepository.save(conn, buffer)

    async def _queue_step(self, buffer, bar_idx: int=None):
        try:
            item = self._smilar_queue.get_nowait()

            buffer.extend(item)
            self._smilar_queue.task_done()

            if len(buffer) >= self._queue_batch_size:
                with db() as conn:
                    await asyncio.to_thread(self._save_similar, buffer)
                if bar_idx:
                    await self.ui.done(bar_idx, len(buffer))
                self._smilar_queue_size -= len(buffer)
                buffer.clear()

            return True
        except asyncio.QueueEmpty:
            if buffer:
                await asyncio.to_thread(self._save_similar, buffer)
                if bar_idx:
                    await self.ui.done(bar_idx, len(buffer))
                self._smilar_queue_size -= len(buffer)
                buffer.clear()
            await asyncio.sleep(1)
            return False
        except Exception as e:
            self.ui.console.log(f"Критическая ошибка при сбросе очереди: {e}")
            return False

    async def _save_loop(self):
        buffer = []

        while not self._stop_event.is_set():
            await self._queue_step(buffer)

        if self._smilar_queue_size > 0:
            self.ui.console.log("Остановка. Сбрасываем остаток очереди...")
            bar_idx = self.ui.add_progress("Сброс оставшихся записей", "Записано строк")
            await self.ui.update_total(self._smilar_queue_size, bar_idx)

            while await self._queue_step(buffer, bar_idx=bar_idx):
                pass

        self.ui.console.log("Save thread stopped")

    async def get_total(self) -> int:
        return self._task_total

    async def stat_books(self):
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
        
        self.queue = asyncio.Queue()
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
        
        # Фоновая задача по сохранению совпадений в базу
        self._save_task = asyncio.create_task(self._save_loop())
    
    async def process_book(self, task: Task):
        similar = await to_thread(self._service.run, task.entity[0], task.entity[1])
        await self._smilar_queue.put(similar)
        self._smilar_queue_size += len(similar)

    async def fin(self):
        self._stop_event.set()

        if self._smilar_queue_size > 0:
            self.ui.console.log(f"Still have {self._smilar_queue_size} records to save into database")

        if self._save_task:
            await self._save_task