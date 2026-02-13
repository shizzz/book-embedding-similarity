import queue
import threading
from tqdm import tqdm
from typing import Tuple, List
from app.workers import BaseWorker
from app.services import BulkSimilarSearchService
from app.models import Task, Book
from app.db import db, BookRepository, SimilarRepository
from app.searchEngines import SimilarSearchEngineFactory
from app.settings.config import SIMILARS_PER_BOOK, DATABASE_QUEUE_BATCH_SIZE

class GenerateSimilarWorker(BaseWorker):
    _service: BulkSimilarSearchService
    _limit: int = SIMILARS_PER_BOOK

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._queue = queue.Queue()
        self._stop_event = threading.Event()
        self._save_thread = threading.Thread(target=self._save_loop)
        self._save_thread.start()
        self._queue_batch_size: int = DATABASE_QUEUE_BATCH_SIZE

    def _queue_step(self, buffer, conn, pbar=None):
        try:
            item = self._queue.get(timeout=0.1)

            buffer.extend(item)
            self._queue.task_done()

            if len(buffer) >= self._queue_batch_size:
                SimilarRepository().save(conn, buffer)
                if pbar:
                    pbar.update(len(buffer))
                buffer.clear()

            return True
        except queue.Empty:
            if buffer:
                SimilarRepository().save(conn, buffer)
                if pbar:
                    pbar.update(len(buffer))
                buffer.clear()
            return False
        except Exception as e:
            self.logger.error(
                f"Критическая ошибка при сбросе очереди: {e}",
                exc_info=True
            )
            return False

    def _save_loop(self):
        buffer = []

        with db() as conn:
            while not self._stop_event.is_set():
                self._queue_step(buffer, conn)

        approx_total = self._queue.unfinished_tasks * self._limit

        if approx_total > 0:
            self.logger.info("Остановка. Сбрасываем остаток очереди...")

            with tqdm(
                total=approx_total,
                desc="Сброс оставшихся записей",
                unit=" rows",
                unit_scale=True
            ) as pbar, db() as conn:
                while self._queue_step(buffer, conn, pbar=pbar):
                    pass

        self.logger.info("Save thread stopped")

    async def stat_books(self):
        self.logger.info(f"Очистка таблицы similar")

        with db() as conn:
            SimilarRepository().clear(conn)

            self.logger.info(f"Получение всех книг из базы данных")
            books_with_embeddings = list[Tuple[int, str, str, str, bytes]](BookRepository().get_all_with_embeddings(conn))

            self.logger.info(f"Фильтрация книг и эмбеддингов по ID")
            valid_books: List[Book] = []
            valid_embeddings: List[bytes] = []

            for book_id, archive, book_name, title, embedding in books_with_embeddings:
                valid_books.append(Book(id=book_id, archive_name=archive, file_name=book_name, title=title))
                valid_embeddings.append(embedding)

        engine = SimilarSearchEngineFactory.create(SimilarSearchEngineFactory.INDEX, SIMILARS_PER_BOOK, False, 1)

        self._service = BulkSimilarSearchService(
            engine,
            valid_books,
            valid_embeddings,
            logger=self.logger
        )

        self.logger.info(f"Добавление книг и эмбеддингов в очередь")
        tasks: List[Task] = []
        for book_id, archive, book_name, title, embedding in books_with_embeddings:
            tasks.append(Task(
                name=book_name,
                book=Book(
                    id=book_id,
                    archive_name=archive,
                    file_name=book_name,
                    title=title),
                embedding=embedding))
        await self.registry.add(tasks)
        del books_with_embeddings
    
    def process_book(self, task: Task):
        similar = self._service.run(task.book, task.embedding)
        self._queue.put(similar)

    async def fin(self):
        total = self._queue.unfinished_tasks
        self._stop_event.set()

        if total > 0:
            self.logger.info(f"Still have {self._queue.unfinished_tasks} records to save into database")

        self._save_thread.join()