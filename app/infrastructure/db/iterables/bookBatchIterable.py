import asyncio
from typing import Generator
from typing import AsyncGenerator
from app.infrastructure.models import Book
from app.infrastructure.db.repositories import BookRepository

class BookBatchIterable:
    def __init__(
        self, 
        repo: BookRepository, 
        batch_size: int = 1,
        empty: bool = None,
        order_by: list[str] = None,
    ):
        self.repo = repo
        self.batch_size = batch_size
        self.order_by = order_by
        self.empty = empty
        self._total = None

    async def __aiter__(self) -> AsyncGenerator[list[Book], None]:
        batches = await asyncio.to_thread(
            lambda: list(self.repo.get_all_batch(self.batch_size, self.empty, self.order_by))
        )
        for batch in batches:
            yield batch

    def __len__(self):
        if self._total is None:
            self._total = self.repo.count()
        return (self._total + self.batch_size - 1) // self.batch_size

    def __iter__(self) -> Generator[list[Book], None]:
        yield from self.repo.get_all_batch(self.batch_size, self.empty, self.order_by)

    def count(self):
        return self.repo.count()