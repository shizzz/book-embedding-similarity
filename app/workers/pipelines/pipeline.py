import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Optional, List
from app.workers.base import BaseQueueWorker
from app.workers.stats import PipelineStats
from app.infrastructure.db import DBRouter
from app.infrastructure.models import Channel

class Pipeline(ABC):
    def __init__(
        self,
        name: str,
        router: DBRouter,
        stats: PipelineStats,
        logger: logging.Logger,
        input_channel: Optional[Channel] = None,
        output_channels: Optional[List[Channel]] = None,
        upstream_done: Optional[asyncio.Event()] = None,
    ):
        self.name = name
        self._router = router
        self._stats = stats
        self._logger = logger
        self._input_channel = input_channel
        self._output_channels = output_channels
        self._upstream_done = asyncio.Event()

        if self._upstream_done and self._input_channel:
            raise ValueError(
                f"В пайплайне {name} Нельзя указывать одновременно upstream_done и input_channel: это приведёт к зависанию."
            )

        self.pool: list[BaseQueueWorker] = []

    def get_start_tasks(self) -> list:
        if self._upstream_done is None:
            [self._exec_start()]
        else:
            return []

    def get_wait_tasks(self) -> list:
        if self._upstream_done is None:
            [self._exec_wait()]
        else:
            return [self._exec_with_upstream()]

    async def _exec_start(self):
        await asyncio.gather(*(worker.start() for worker in self.pool))

    async def _exec_wait(self):
        await asyncio.gather(*(worker.wait() for worker in self.pool))

    async def _exec_with_upstream(self):
        await self._upstream_done.wait()
        await self._exec_start()
        await self._exec_wait()

    @abstractmethod
    async def setup_stages(self) -> None:
        raise NotImplementedError