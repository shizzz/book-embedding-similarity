from .base_batch_strategy import BaseBatchStrategy
from app.infrastructure.models import Task, Embedding

class BookEmbeddingBatchStrategy(BaseBatchStrategy):
    """Собирает batch из N книг"""

    def __init__(self, books: int):
        self.books = books
        self.current_book_id = None
        self.current_book: list[Task] = []
        self.books_buffer: list[Task] = []

    def info(self):
        return str(self.books)

    def collect(self, task: Task[Embedding]) -> list[Task] | None:
        book_id = task.entity.source_id

        if not self.current_book:
            self.current_book_id = book_id
            self.current_book.append(task)
            return None

        # книга продолжается
        if book_id == self.current_book_id:
            self.current_book.append(task)
            return None

        # книга закончилась
        self.books_buffer.extend(self.current_book)

        self.current_book = [task]
        self.current_book_id = book_id

        # накопили нужное количество книг
        if len(self.books_buffer) and self._books_count() >= self.books:
            batch = self.books_buffer
            self.books_buffer = []
            return batch

        return None

    def flush(self) -> list[Task] | None:
        if self.current_book:
            self.books_buffer.extend(self.current_book)
            self.current_book = []

        if self.books_buffer:
            batch = self.books_buffer
            self.books_buffer = []
            return batch

        return None

    def _books_count(self) -> int:
        """Сколько книг сейчас в books_buffer"""
        if not self.books_buffer:
            return 0

        last = None
        count = 0
        for t in self.books_buffer:
            if t.entity.book_id != last:
                count += 1
                last = t.entity.book_id

        return count