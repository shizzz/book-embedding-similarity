import queue
import threading
from app.workers import BaseWorker
from app.services import BulkSimilarSearchService
from app.models import Task


class GenerateSimilarWorker(BaseWorker):
    __service: BulkSimilarSearchService

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._queue = queue.Queue()
        self._stop_event = threading.Event()
        self._save_thread = threading.Thread(target=self._save_loop)
        self._save_thread.start()
        self._batch_size: int = 10000

    def _save_loop(self):
        buffer: list = []
        while not self._stop_event.is_set() or not self._queue.empty():
            try:
                similar_list = self._queue.get(timeout=0.1)
                buffer.extend(similar_list)
                if len(buffer) >= self._batch_size:
                    self.db.save_similar(buffer)
                    buffer = []
            except queue.Empty:
                if buffer:
                    self.db.save_similar(buffer)
                    buffer = []
                if self._stop_event.is_set():
                    break

    async def stat_books(self):
        self.__service = BulkSimilarSearchService(
            limit=100,
            exclude_same_authors=False
        )
        await self.registry.fill_from_books(self.__service.valid_books, True)

    def process_book(self, task: Task):
        similar = self.__service.run(task.book)
        self._queue.put(similar)

    def fin(self):
            self._stop_event.set()
            self._save_thread.join()