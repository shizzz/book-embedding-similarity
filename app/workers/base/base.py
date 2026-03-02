import asyncio
import traceback
import logging
from abc import ABC, abstractmethod
from rich.live import Live
from app.ui import LiveUI
from app.workers.sources import ConsoleHandler
from app.db import Migrator, DBRouter
from app.settings.config import MAX_WORKERS

class BaseWorker(ABC):
    def __init__(
        self,
        max_workers: int = MAX_WORKERS,
        title: str = None
    ):
        self.max_workers: int = max_workers
        self._router = DBRouter()
        
        self.ui = LiveUI(max_workers=self.max_workers, title=title)
        self.ui.init()

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        handler = ConsoleHandler(console=getattr(self, "ui", None) and getattr(self.ui, "console", None))
        handler.setFormatter(logging.Formatter('[%(levelname)s] %(asctime)s %(message)s'))
        self.logger.addHandler(handler)
        
    @abstractmethod
    async def prepare(self) -> None:
        pass

    @abstractmethod
    async def thread(self, worker_id: int) -> None:
        pass

    @abstractmethod
    async def fin(self) -> None:
        pass
    
    async def before_run(self):    
        pass

    async def before_fin(self) -> None:
        pass

    def create_tasks(self) -> list[asyncio.Task]:
        return []

    async def run(self):
        try:
            self.logger.info("DB Init...")
            Migrator(self._router).migrate_chunks()
            Migrator(self._router).migrate_meta()

            self.logger.info("Prepare...")
            await self.prepare()

            self.logger.info("Run...")
            await self.before_run()

            producer_tasks = self.create_tasks()

            await asyncio.sleep(0)

            worker_tasks = [
                asyncio.create_task(self.thread(i))
                for i in range(1, self.max_workers + 1)
            ]

            if producer_tasks:
                await asyncio.gather(*producer_tasks)
            await asyncio.gather(*worker_tasks)

            self.logger.info("Finalise")
            await self.before_fin()
            await self.fin()
            self.logger.info("All books processed!")
        except Exception as error:
            self.logger.error(error)
            traceback.print_exc()
        finally:
            self.ui.live.stop()