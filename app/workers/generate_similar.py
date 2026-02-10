import queue
import threading
from tqdm import tqdm
from typing import Tuple, List
from app.workers import BaseWorker
from app.services import BulkSimilarSearchService
from app.models import Task, Book
from app.db import db, BookRepository, SimilarRepository
from app.searchEngines import SimilarSearchEngine, SimilarSearchEngineFactory
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

    def _save_loop(self):
        buffer: list = []
        while not self._stop_event.is_set():
            try:
                similar_list = self._queue.get(timeout=0.1)
                buffer.extend(similar_list)
                self._queue.task_done()

                if len(buffer) >= self._queue_batch_size:
                    with db() as conn:
                        SimilarRepository().save(conn, buffer)
                    buffer = []
            except queue.Empty:
                if buffer:
                    with db() as conn:
                        SimilarRepository().save(conn, buffer)
                    buffer = []
                continue

        self.logger.info("Остановка. Сбрасываем остаток очереди...")
        buffer = []
        
        approx_total = self._queue.unfinished_tasks * self._limit

        with tqdm(total=approx_total, desc="Сброс оставшихся записей", unit=" rows", unit_scale=True) as pbar:
            with db() as conn:
                while True:
                    try:
                        similar_list = self._queue.get_nowait()
                        buffer.extend(similar_list)
                        self._queue.task_done()

                        if len(buffer) >= self._queue_batch_size:
                            SimilarRepository().save(conn, buffer)
                            pbar.update(len(buffer))
                            buffer = []
                    except queue.Empty:
                        if buffer:
                            SimilarRepository().save(conn, buffer)
                            pbar.update(len(buffer))
                            buffer = []
                        break
                    except Exception as e:
                        self.logger.error(f"Критическая ошибка при сбросе очереди: {e}", exc_info=True)
                        break

        while not self._queue.empty():
            try:
                self._queue.get_nowait()
                self._queue.task_done()
            except queue.Empty:
                break

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

        engine = SimilarSearchEngineFactory.create(SimilarSearchEngine.INDEX, SIMILARS_PER_BOOK, False, 1)

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