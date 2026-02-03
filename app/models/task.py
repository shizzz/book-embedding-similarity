import asyncio
from dataclasses import dataclass
from typing import Optional
from app.models.book import Book

@dataclass
class Task:
    name: str
    book: Optional[Book] = None

class TaskRegistry:
    def __init__(self):
        self.queue = asyncio.Queue()
        self.total = 0
        self.completed = 0

    async def add(self, tasks: list[Task]):
        for task in tasks:
            await self.queue.put(task)
        self.total += len(tasks)

    async def fill_from_books(self, books: list[Book], add_all: bool = False):
        for book in books:
            if book.completed and not add_all:
                self.completed += 1
            else:
                await self.queue.put(Task(
                    name=book.file_name,
                    book=book))
                
        self.total = len(books)

    async def get(self) -> Task | None:
        try:
            return await self.queue.get()
        except asyncio.CancelledError:
            return None

    def mark_completed(self):
        self.completed += 1
