import asyncio
from dataclasses import dataclass
from typing import Optional, List, Tuple
from .book import Book

@dataclass
class Task:
    name: str
    book: Optional[Book] = None
    embedding: Optional[bytes] = None

class TaskRegistry:
    def __init__(self):
        self.queue = asyncio.Queue()
        self.total = 0
        self.completed = 0

    async def add(self, tasks: list[Task]):
        for task in tasks:
            await self.queue.put(task)
        self.total += len(tasks)

    async def add_one(self, task: Task):
        await self.queue.put(task)
        self.total += 1

    async def get(self) -> Task | None:
        try:
            return await self.queue.get()
        except asyncio.CancelledError:
            return None

    def mark_completed(self):
        self.completed += 1
