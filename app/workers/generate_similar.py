from asyncio import to_thread
from app.workers.base import BaseDbQueueWorker
from app.services import BulkSimilarSearchService
from app.models import Task, Book, Task, TaskResult, Action, BookRegistry
from app.db import db, BookRepository, SimilarRepository
from app.searchEngines.similarSearch import SimilarSearchEngineFactory
from app.settings.config import SIMILARS_PER_BOOK

class GenerateSimilarWorker(BaseDbQueueWorker):
    _service: BulkSimilarSearchService
    _limit: int = SIMILARS_PER_BOOK

    def __init__(self, batch_size: int = 50, **kwargs):
        super().__init__(**kwargs)

        self._task_total: int = 0
        self.batch_size: int = batch_size
        self._services: dict[int, BulkSimilarSearchService] = {}

    async def thread_start(self, thread_id: int) -> None:
        self._services[thread_id] = BulkSimilarSearchService(self._engine, logger=self.logger)
        pass

    async def process(self, task: Task, thread_id: int) -> TaskResult:
        service = self._services[thread_id]
        result = await to_thread(service.run, task.entity)
        
        return task.to_result(
            done=len(task.entity),
            db_queue_count=len(result),
            entity=result
        )

    async def prepare(self) -> None:
        self.logger.info(f"Очистка таблицы similar")

        with db() as conn:
            SimilarRepository.clear(conn)

        self._engine = SimilarSearchEngineFactory.create(
            mode=SimilarSearchEngineFactory.INDEX, 
            limit=SIMILARS_PER_BOOK, 
            exclude_same_authors=True, 
            step_percent=1,
            logger=self.logger
        )

    async def pull_queue(self) -> None:
        self.logger.info(f"Добавление книг и эмбеддингов в очередь")  
        registry = BookRegistry()
        expected_dim = None
        with db() as conn:
            for book_id, book_name, title, author, _, _, embedding in BookRepository.get_all_with_embeddings(conn):
                if expected_dim is None:
                    expected_dim = embedding.shape[0]
                if embedding.shape[0] != expected_dim:
                    continue

                self._task_total += 1

                book = Book(
                    id=book_id,
                    file_name=book_name,
                    title=title,
                    author=author,
                    embedding=embedding
                )
                registry.append(book)

                if len(registry) >= self.batch_size:
                    self.queue.put_nowait(
                        Task(
                            name=book_name,
                            entity=registry,
                            action=Action.INSERT
                        )
                    )
                    registry = BookRegistry()


            if len(registry) > 0:
                self.queue.put_nowait(
                    Task(
                        name=book_name,
                        entity=registry,
                        action=Action.INSERT
                    )
                )
                registry = BookRegistry()

        await self.enqueue_shutdown_signals_async()
        self._queue_pulled.set()

    async def get_total(self) -> int:
        await self._queue_pulled.wait()
        return self._task_total

    def save_to_db(self, conn, task: Task) -> int:
        SimilarRepository.save(conn, task.entity)
        return len(task.entity)
