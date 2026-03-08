from asyncio import to_thread
from app.workers.base import BaseDbQueueWorker
from app.services import BulkSimilarSearchService
from app.infrastructure.models import Task, Task, Action
from app.infrastructure.db.repositories import BookRepository, SimilarRepository
from app.searchEngines.similarSearch import SimilarSearchEngineFactory
from app.settings import ProcessConfig

class GenerateSimilarWorker(BaseDbQueueWorker):
    _service: BulkSimilarSearchService
    _limit: int = ProcessConfig.SIMILARS_PER_BOOK

    def __init__(self, batch_size: int = 50, **kwargs):
        super().__init__(**kwargs)

        self._task_total: int = 0
        self.batch_size: int = batch_size
        self._services: dict[int, BulkSimilarSearchService] = {}

    async def thread_start(self, thread_id: int) -> None:
        self._services[thread_id] = BulkSimilarSearchService(self._engine, logger=self.logger)
        pass

    async def process(self, task: Task, thread_id: int) -> Task:
        service = self._services[thread_id]
        result = await to_thread(service.run, task.entity)
        
        return task.to_result(
            done=len(task.entity),
            db_queue_count=len(result),
            entity=result
        )

    async def prepare(self) -> None:
        self.logger.info(f"Очистка таблицы similar")
        SimilarRepository(self._router).clear()

        self._engine = SimilarSearchEngineFactory.create(
            mode=SimilarSearchEngineFactory.INDEX,
            router=self._router,
            limit=ProcessConfig.SIMILARS_PER_BOOK, 
            exclude_same_authors=True, 
            step_percent=1,
            logger=self.logger
        )

    async def pull_queue(self) -> None:
        self.logger.info("Добавление книг и эмбеддингов в очередь")
        buffer = []
        batch_name = None

        for book in BookRepository.get_all(self._router, False):
            self._task_total += 1
            buffer.append(book[0])
            batch_name = book[1]  # сохраняем имя книги для текущего батча

            if len(buffer) >= self.batch_size:
                self.queue.put_nowait(
                    Task(
                        name=batch_name,
                        entity=buffer.copy(),
                        action=Action.INSERT
                    )
                )
                buffer = []

        # остаток
        if buffer:
            self.queue.put_nowait(
                Task(
                    name=batch_name,
                    entity=buffer,
                    action=Action.INSERT
                )
            )
            buffer = []

        await self.enqueue_shutdown_signals_async()
        self._queue_pulled.set()

    async def get_total(self) -> int:
        await self._queue_pulled.wait()
        return self._task_total

    async def fin(self) -> None:
        pass

    def save_to_db(self, conn, task: Task) -> int:
        SimilarRepository(self._router).save(task.entity)
        return len(task.entity)
