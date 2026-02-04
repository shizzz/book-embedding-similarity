import queue
import threading
from tqdm import tqdm
from app.workers import BaseWorker
from app.services import BulkSimilarSearchService
from app.models import Task
from app.settings.config import SIMILARS_PER_BOOK, DATABASE_QUEUE_BATCH_SIZE


class GenerateSimilarWorker(BaseWorker):
    __service: BulkSimilarSearchService
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
                    self.db.save_similar(buffer)
                    buffer = []
            except queue.Empty:
                if buffer:
                    self.db.save_similar(buffer)
                    buffer = []
                continue

        self.logger.info("Остановка. Сбрасываем остаток очереди...")
        buffer = []
        
        approx_total = self._queue.unfinished_tasks * self._limit

        with tqdm(total=approx_total, desc="Сброс оставшихся записей", unit=" rows", unit_scale=True) as pbar:
            while True:
                try:
                    similar_list = self._queue.get_nowait()
                    buffer.extend(similar_list)
                    self._queue.task_done()

                    if len(buffer) >= self._queue_batch_size:
                        self.db.save_similar(buffer)
                        pbar.update(len(buffer))
                        buffer = []
                except queue.Empty:
                    if buffer:
                        self.db.save_similar(buffer)
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
        self.db.delete_similar()
        self.__service = BulkSimilarSearchService(
            limit=self._limit,
            exclude_same_authors=False,
            logger=self.logger
        )
        await self.registry.fill_from_books(self.__service.valid_books, True)

    def process_book(self, task: Task):
        similar = self.__service.run(task.book)
        self._queue.put(similar)

    async def fin(self):
        total = self._queue.unfinished_tasks
        self._stop_event.set()

        if total > 0:
            self.logger.info(f"Still have {self._queue.unfinished_tasks} records to save into database")

        self._save_thread.join()