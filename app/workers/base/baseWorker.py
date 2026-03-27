import asyncio
import logging
from abc import ABC, abstractmethod
from typing import List, Optional
from app.settings import ProcessConfig
from app.ui.live_ui import LiveUI, BaseUI
from app.workers.sources import ConsoleHandler
from app.workers.stats import PipelineStats
from app.workers.pipelines import Pipeline
from app.infrastructure.db import DBRouter
from .saveRegistry import SaveRegistry

class BaseWorker(ABC):
    """
    Orchestrates a pool of stage workers (typically `BaseQueueWorker` instances),
    manages a LiveUI refresh loop, and provides a consistent logger.
    """

    name: str = "Worker"

    def __init__(
        self,
        *,
        stats: PipelineStats = None,
        name: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
        ui: BaseUI = None
    ):
        if name is not None:
            self.name = name

        self.stats = stats or PipelineStats()
        self.ui = ui or LiveUI(
            max_workers=ProcessConfig.MAX_WORKERS,
            title=self.name,
            stats=self.stats
        )
        self.logger = logger or logging.getLogger(self.__class__.__name__)
        self._configure_logger()

        self.router = DBRouter()
        self.registry = SaveRegistry()
        self.pipelines: List[Pipeline] = []
        self._ui_task: Optional[asyncio.Task] = None

    def _configure_logger(self) -> None:
        if self.logger.handlers:
            return

        self.logger.setLevel(logging.INFO)
        handler = ConsoleHandler(console=getattr(self, "ui", None) and getattr(self.ui, "console", None))
        handler.setFormatter(logging.Formatter("[%(levelname)s] %(asctime)s %(message)s"))
        self.logger.addHandler(handler)
        self.logger.propagate = False

    async def run(self):
        await self.setup_stages()

        if self.ui is not None:
            self.ui.init()
            self._ui_task = asyncio.create_task(self.refresh_ui_loop())

        try:
            await self.before_run()

            if self.pipelines:
                await asyncio.gather(*(pipeline.setup_stages() for pipeline in self.pipelines))
                
                # Сначала стартовые задачи всех pipeline
                start_tasks_nested = await asyncio.gather(
                    *(p.get_start_tasks() for p in self.pipelines)
                )

                start_tasks = [t for sublist in start_tasks_nested for t in sublist]
                if start_tasks:
                    await asyncio.gather(*start_tasks)

                # Потом задачи ожидания/с зависимостью
                wait_tasks = [t for p in self.pipelines for t in p.get_wait_tasks()]
                if wait_tasks:
                    await asyncio.gather(*wait_tasks)

            if self.ui is not None:
                await self.ui.update()

            await self.after_run()

            if self.ui is not None:
                await self.ui.update()
        finally:
            if self._ui_task is not None:
                self._ui_task.cancel()
                await asyncio.gather(self._ui_task, return_exceptions=True)

    @abstractmethod
    async def setup_stages(self) -> None:
        raise NotImplementedError

    async def before_run(self) -> None:
        return

    async def after_run(self) -> None:
        return

    async def refresh_ui_loop(self):
        while True:
            if self.ui is not None:
                await self.ui.update()
            await asyncio.sleep(1)

