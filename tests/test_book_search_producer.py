import asyncio
import unittest
from typing import List

import app.workers.stages.book_search_producer as bsp
from app.infrastructure.models import Book, Task, BookTask, BookAction


class DummyEngine:
    def __init__(self, books: List[Book] | None = None, data_by_file: dict[str, bytes] | None = None, total: int = 0):
        self._books = books or []
        self._data_by_file = data_by_file or {}
        self._total = total

    async def search_books(self):
        for book in self._books:
            yield book

    async def get_book_data(self, book: Book) -> bytes | None:
        return self._data_by_file.get(book.file_name)

    async def get_total(self) -> int:
        return self._total


class FakeBookRepository:
    max_id: int = 1
    file_to_id: dict[str, int] = {}
    full_by_file: dict[str, Book] = {}

    def __init__(self, router):
        self.router = router

    def get_max_id(self) -> int:
        return type(self).max_id

    def get_file_to_id(self) -> dict[str, int]:
        return dict(type(self).file_to_id)

    def get_full_by_file(self, file_name: str) -> Book | None:
        return type(self).full_by_file.get(file_name)


class FakeChunkRepository:
    ids: set[int] = set()

    def __init__(self, router):
        self.router = router

    def get_ids(self) -> set[int]:
        return set(type(self).ids)


# Patch repositories in the target module to avoid real DB usage in tests.
bsp.BookRepository = FakeBookRepository
bsp.ChunkRepository = FakeChunkRepository

BookProducer = bsp.BookProducer


class TestBookProducer(unittest.IsolatedAsyncioTestCase):
    async def test_produce_wraps_engine_books_into_tasks(self):
        books = [
            Book(file_name="file1.fb2", id=1),
            Book(file_name="file2.fb2", id=2),
        ]
        engine = DummyEngine(books=books, total=len(books))

        # Configure repository state for constructor.
        FakeBookRepository.max_id = 10
        FakeBookRepository.file_to_id = {b.file_name: b.id for b in books}
        FakeChunkRepository.ids = set()

        producer = BookProducer(router=None, search_engine=engine)

        produced: list[Task[Book]] = []
        async for task in producer.produce():
            produced.append(task)

        self.assertEqual([t.id for t in produced], [1, 2])
        self.assertEqual([t.name for t in produced], ["file1.fb2", "file2.fb2"])
        self.assertEqual([t.entity for t in produced], books)

        await producer.count_task

    async def test_process_existing_book_without_chunks_creates_chunk_task(self):
        book = Book(file_name="known.fb2", id=1, empty=False)

        FakeBookRepository.max_id = 10
        FakeBookRepository.file_to_id = {book.file_name: book.id}
        FakeBookRepository.full_by_file = {
            book.file_name: Book(file_name=book.file_name, id=book.id, empty=False)
        }
        FakeChunkRepository.ids = set()

        engine = DummyEngine(data_by_file={book.file_name: b"data"})
        producer = BookProducer(router=None, search_engine=engine)

        batch = [Task(id=book.id, name=book.file_name, entity=book)]
        tasks = await producer.process(batch, wid=0)

        self.assertEqual(len(tasks), 1)
        t = tasks[0]
        self.assertIsInstance(t.entity, BookTask)
        self.assertEqual(t.id, book.id)
        self.assertEqual(t.name, book.file_name)
        self.assertEqual(t.entity.action, BookAction.CHUNK)
        self.assertEqual(t.entity.book.file_name, book.file_name)
        self.assertEqual(t.entity.data, b"data")

        await producer.count_task

    async def test_process_new_book_with_data_creates_book_task_and_reserves_id(self):
        new_book = Book(file_name="new.fb2", id=None, empty=False)

        FakeBookRepository.max_id = 42
        FakeBookRepository.file_to_id = {}
        FakeBookRepository.full_by_file = {}
        FakeChunkRepository.ids = set()

        engine = DummyEngine(data_by_file={new_book.file_name: b"payload"})
        producer = BookProducer(router=None, search_engine=engine)

        batch = [Task(id=0, name=new_book.file_name, entity=new_book)]
        tasks = await producer.process(batch, wid=0)

        self.assertEqual(len(tasks), 1)
        t = tasks[0]

        self.assertEqual(t.id, 42)
        self.assertEqual(t.entity.book.id, 42)
        self.assertEqual(t.name, new_book.file_name)
        self.assertEqual(t.entity.action, BookAction.BOOK)
        self.assertEqual(t.entity.data, b"payload")
        self.assertEqual(producer._book_id, 43)

        await producer.count_task

    async def test_process_new_book_without_data_skips_task(self):
        new_book = Book(file_name="empty.fb2", id=None, empty=False)

        FakeBookRepository.max_id = 5
        FakeBookRepository.file_to_id = {}
        FakeBookRepository.full_by_file = {}
        FakeChunkRepository.ids = set()

        engine = DummyEngine(data_by_file={})
        producer = BookProducer(router=None, search_engine=engine)

        batch = [Task(id=0, name=new_book.file_name, entity=new_book)]
        tasks = await producer.process(batch, wid=0)

        self.assertEqual(tasks, [])
        self.assertEqual(producer._book_id, 6)

        await producer.count_task


if __name__ == "__main__":
    unittest.main()

