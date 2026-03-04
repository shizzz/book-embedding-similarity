import asyncio
import traceback
import gc
from abc import ABC, abstractmethod
from typing import Generic
from app.common.types import TEntity
from app.infrastructure.models import Task, TaskResult
from .base import BaseWorker

class BaseQueueWorker(BaseWorker, ABC, Generic[TEntity]):
    def __init__(self, queue_size: int, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.queue: asyncio.Queue[Task[TEntity]] = asyncio.Queue(maxsize=queue_size)
        self._queue_pulled = asyncio.Event()
        self._progress_idx = self.ui.add_progress("Book analys process", "books")

    @abstractmethod
    async def process(self, task: Task, thread_id: int) -> TaskResult:
        raise NotImplementedError("process_book must be implemented by subclass")

    @abstractmethod
    async def get_total(self) -> int:
        pass
    
    @abstractmethod
    async def pull_queue(self) -> None:
        pass
    
    async def post_process(self, result: TaskResult) -> None:
        pass

    async def thread_start(self, thread_id: int) -> None:
        pass

    def create_tasks(self) -> list[asyncio.Task]:
        return [
            asyncio.create_task(self._get_total()),
            asyncio.create_task(self.pull_queue())
        ]

    async def thread(self, thread_id: int) -> None:
        await self.thread_start(thread_id)
        while not self._queue_pulled.is_set() or not self.queue.empty():
            task = await self.queue.get()

            if task is None:
                self.queue.task_done()
                return

            try:
                await self.ui.set_thread(thread_id, task.name)

                result = await self.process(task, thread_id)
                await self.post_process(result)

                await self.ui.done_async(self._progress_idx, count=result.done or 1)
            except Exception as error:
                await self.ui.error(self._progress_idx)
                traceback.print_exc()
                self.logger.error(f"ERROR processing {task.name}: {error}")
            finally:
                if hasattr(task, "entity"):
                    del task.entity
                del task
                self.queue.task_done()
                gc.collect()
                
    async def enqueue_shutdown_signals_async(self):
        for _ in range(self.max_workers):
            await self.queue.put(None)

    async def enqueue_shutdown_signals(self):
        for _ in range(self.max_workers):
            await self.queue.put_nowait(None)

    async def _get_total(self):
        total = await self.get_total()
        await self.ui.update_total_async(total, self._progress_idx)