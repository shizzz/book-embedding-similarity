import asyncio
import traceback
import gc
from app.common.types import TEntity
from typing import Generic, List
from app.models import Task
from app.db import DBRouter
from app.ui import BaseUI

class DbQueue(Generic[TEntity]):
    def __init__(
            self,
            save_func,
            batch_size: int,
            db_queue_max_size: int,
            ui: BaseUI,
            router: DBRouter
        ):
        self.queue: asyncio.Queue[TEntity] = asyncio.Queue(maxsize=db_queue_max_size)
        self.task: asyncio.Task | None = None

        self._stop_event = asyncio.Event()
        self._total: int = 0
        self._progress_idx: int = None
        self._batch_size: int = batch_size

        self._ui = ui
        self._save_func = save_func
        self._router = router

    def start(self):
        self._progress_idx = self._ui.add_progress("Сохранение в БД", "пакетов")
        self.task = asyncio.create_task(self._loop())

    async def stop(self):
        self._stop_event.set()
        
        db_queue_size = self.queue.qsize()
        if db_queue_size > 0:
            self._ui.console.log(f"Still have {db_queue_size} records to save into database")

        self.queue.put_nowait(None)

        await self.task

    async def put(self, done: int, task: Task):
        self._total += done
        await self.queue.put(task)
        await self._ui.update_total_async(
            idx=self._progress_idx,
            total=self._total
        )

    async def _loop(self):
        buffer = []
        errors = 0

        while not self._stop_event.is_set():
            await self._step_async(buffer, errors)

        while self._step(self._router, buffer, errors):
            pass

        self._ui.console.log("Save thread stopped")

    def _save(self, tasks: List[Task]) -> int:
        total = 0
        for task in tasks:
            total += self._save_func(self._router, task)
        return total

    async def _step_async(self, buffer: List[Task], errors: int):
        try:
            task = await self.queue.get()
        
            if task:
                buffer.append(task)

            total = sum(len(task.entity) for task in buffer)

            if total >= self._batch_size or task is None:
                done = await asyncio.to_thread(self._save, buffer.copy())
                await self._ui.done_async(self._progress_idx, done)
                for t in buffer:
                    del t.entity
                buffer.clear()
                gc.collect()
            self.queue.task_done()

            return task is not None
        except Exception as e:
            errors += 1
            if errors > 5:
                self.queue.task_done()
                errors = 0
            traceback.print_exc()
            self._ui.console.log(f"Критическая ошибка при сохранение в базу данных: {e}")
            return False
    
    def _step(self, conn, buffer: list[Task], errors: int) -> bool:
        if buffer:
            for task in buffer:
                done = self._save_func(conn, task)
                self._ui.done(self._progress_idx, done)
                del task.entity
            buffer.clear()
            gc.collect()

        try:
            task = self.queue.get_nowait()
        except asyncio.QueueEmpty:
            return

        try:
            if task is not None:
                done = self._save_func(conn, task)
                self._ui.done(self._progress_idx, done)

            self.queue.task_done()
        except Exception as e:
            errors += 1
            if errors > 5:
                self.queue.task_done()
                errors = 0

        return task is not None