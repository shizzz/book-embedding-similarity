import asyncio
import traceback
import gc
from abc import ABC, abstractmethod
from typing import Generic
from app.common.types import TEntity
from app.models import Task, TaskResult
from .base import BaseWorker

class BaseQueueWorker(BaseWorker, ABC, Generic[TEntity]):
    def __init__(self, queue_size: int, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.queue: asyncio.Queue[Task[TEntity]] = asyncio.Queue(maxsize=queue_size)
        self._queue_pulled: bool = False

    @abstractmethod
    async def process(self, task: Task) -> TaskResult:
        raise NotImplementedError("process_book must be implemented by subclass")

    @abstractmethod
    async def get_total(self) -> int:
        pass
    
    @abstractmethod
    async def pull_queue(self) -> None:
        pass
    
    async def post_process(self, result: TaskResult) -> None:
        pass

    def create_tasks(self) -> list[asyncio.Task]:
        return [
            asyncio.create_task(self._get_total()),
            asyncio.create_task(self.pull_queue())
        ]

    async def worker(self, worker_id: int) -> None:
        while not self._queue_pulled or not self.queue.empty():
            task = await self.queue.get()

            if task is None:
                self.queue.task_done()
                return

            try:
                if self.show_ui:
                    await self.ui.set_thread(worker_id, task.name)

                result = await self.process(task)
                await self.post_process(result)

                if self.show_ui:
                    await self.ui.done_async(count=result.done or 1)
            except Exception as error:
                if self.show_ui:
                    await self.ui.error()
                traceback.print_exc()
                self.ui.console.log(f"ERROR processing {task.name}: {error}")
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
        
        if self.show_ui:
            await self.ui.update_total_async(total)