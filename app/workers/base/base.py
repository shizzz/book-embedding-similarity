import asyncio
import logging
from abc import ABC, abstractmethod
from asyncio import create_task, gather
from rich.live import Live
from app.workers.sources import StatsUI
from app.models import Task, TaskResult
from app.db import Migrator
from app.settings.config import MAX_WORKERS

class BaseWorker(ABC):
    def __init__(
        self,
        max_workers: int = MAX_WORKERS,
        show_ui: bool = True,
        title: str = None
    ):
        self.max_workers: int = max_workers
        self.show_ui: bool = show_ui
        if self.show_ui:
            self.ui = StatsUI(max_workers=self.max_workers, title=title)

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('[%(levelname)s] %(asctime)s %(message)s'))
        self.logger.addHandler(handler)
        
    @abstractmethod
    async def prepare(self) -> None:
        pass

    @abstractmethod
    async def fin(self) -> None:
        pass

    @abstractmethod
    async def worker(self, worker_id: int) -> None:
        pass
    
    async def before_run(self):    
        pass

    async def before_fin(self) -> None:
        pass

    def create_tasks(self) -> list[asyncio.Task]:
        return []

    async def run(self):
        self.logger.info("DB Init...")
        await asyncio.to_thread(Migrator().apply_schema)

        self.logger.info("Prepare...")
        await self.prepare()

        if self.show_ui:
            await self.ui.init()
            with Live(self.ui.layout(), refresh_per_second=1, console=self.ui.console) as live:
                self.ui.live = live
                await self._run_internal()
        else:
            await self._run_internal()
    
    async def _run_internal(self):     
        self.logger.info("Run...")
        await self.before_run()

        tasks = self.create_tasks()
        tasks.extend(
            create_task(self.worker(i))
            for i in range(1, self.max_workers + 1)
        )
        await gather(*tasks)

        self.ui.console.log("Finalise")
        await self.before_fin()
        await self.fin()
        self.ui.console.log("All books processed!")