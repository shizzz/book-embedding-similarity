import asyncio
from typing import Tuple
from app.workers.base import BaseDbQueueWorker
from app.hnsw import IndexManager
from app.model import Model, generate_embeddings
from app.db import DB, BookRepository, EmbeddingsRepository, AuthorRepository, FeedbackRepository, ModelRepository
from app.models import Book, BookRegistry, Feedbacks, Task, TaskResult, Action, Dataset
from app.searchEngines.bookSearch import BookSearchEngineFactory

class GenerateEmbeddingsWorker(BaseDbQueueWorker):
    def __init__(self, max_batch_size: int = 50, **kwargs):
        super().__init__(**kwargs)
        self.hnsw = IndexManager(batch_size=10000, logger=self.logger)
        self.engine = BookSearchEngineFactory.create(BookSearchEngineFactory.INPIX, self.ui)
        self._get_book_idx: int = None
        self._book_id: int = 1
        self.max_batch_size: int = max_batch_size

        self._model = Model(self.max_workers)

    async def process(self, task: Task, _thread_id: int) -> TaskResult:
        result = await asyncio.to_thread(self._process_book, task.entity)
        done = len(task.entity)
        return task.to_result(
            done=done,
            db_queue_count=done,
            entity=result
        )
    
    async def prepare(self) -> None:
        self._get_book_idx = self.ui.add_progress("Парсинг книг", "книг")
        with DB() as conn:
            self._book_id = BookRepository.get_max_id(conn)
            self._remove_invalid_embeddings(conn)
            self._model_id = ModelRepository.get_or_create(
                conn=conn,
                name=self._model.name,
                uid=self._model.uid
            )
    
    async def get_total(self) -> int:
        total = await self.engine.get_total()
        await self.ui.update_total_async(total, self._get_book_idx)
        return total

    async def fin(self) -> None:
        with DB() as conn:
            embeddings = list[Tuple[int, bytes]](EmbeddingsRepository.get_all(conn))
            feedbacks = Feedbacks(FeedbackRepository.get_all(conn))
            books: list[Book] = [
                Book.map_row(row)
                for row in BookRepository.get_all(conn)
            ]
            
        self.hnsw.load_emb(embeddings)
        self.hnsw.rebuild(
            feedbacks=feedbacks,
            books=books,
        )

        self.logger.info("Чистка базы даных")
        DB().vacuum()

    async def pull_queue(self) -> None:
        with DB() as conn:
            existed_books = set(BookRepository.get_names(conn))

        registries: dict[
            tuple[Action, frozenset[Dataset]],
            BookRegistry
        ] = {}

        async for book in self.engine.search_books():
            datasets: list[Dataset] = []

            # Определяем action и datasets
            if book.file_name in existed_books:
                action = Action.UPDATE
                book = await self._enrich_from_db(book)

                if book.text is None:
                    datasets.append(Dataset.BOOK)

                if (
                    book.embedding is None or
                    book.model_id != self._model_id
                ):
                    datasets.append(Dataset.EMBEDDING)

                if len(datasets) == 0:
                    await self.ui.done_async(self._get_book_idx)
                    await self.ui.decrease_total_async()
                    continue
            else:
                action = Action.INSERT

                datasets.extend([
                    Dataset.BOOK,
                    Dataset.EMBEDDING,
                    Dataset.AUTHOR
                ])

            if book.text is None:
                await self.engine.enrich_book_data(book)

            # Назначаем ID если отсутствует
            if book.id is None:
                book.id = self._book_id
                self._book_id += 1

            # Получаем registry по ключу
            key = (action, frozenset(datasets))

            registry = registries.get(key)

            if registry is None:
                registry = BookRegistry()
                registries[key] = registry

            book.model_id = self._model_id
            registry.append(book)

            await self.ui.done_async(self._get_book_idx)

            # Проверяем batch size именно этого registry
            batch_size = self._adaptive_batch_size(
                self.queue.qsize() + len(registry)
            )

            if len(registry) >= batch_size:
                first_book_name = getattr(registry.books[0], "file_name", "unknown")
                dataset_str = ":".join(ds.name for ds in key[1])

                await self.queue.put(
                    Task(
                        name=f"{first_book_name} {action.name} {dataset_str} ({len(registry)})",
                        entity=registry,
                        action=action,
                        dataset=list(key[1])
                    )
                )

                # создаём новый registry для этого ключа
                registries[key] = BookRegistry()

        # Финальный flush всех registry
        for (action, datasets), registry in registries.items():
            if len(registry) == 0:
                continue

            first_book_name = getattr(registry.books[0], "file_name", "unknown")
            dataset_str = ":".join(ds.name for ds in key[1])

            await self.queue.put(
                Task(
                    name=f"{first_book_name} {action.name} {dataset_str} ({len(registry)})",
                    entity=registry,
                    action=action,
                    dataset=list(datasets)
                )
            )

        # shutdown workers
        await self.enqueue_shutdown_signals_async()
        self._queue_pulled.set()

    def save_to_db(self, conn, task: Task) -> int:
        if Dataset.BOOK in task.dataset:
            BookRepository.save_bulk(conn, task.entity)
            
        if Dataset.EMBEDDING in task.dataset:
            EmbeddingsRepository.save_bulk(conn, task.entity)
            
        if Dataset.AUTHOR in task.dataset and task.action == Action.INSERT:
            AuthorRepository.save_bulk(conn, task.entity)

        return len(task.entity)

    def _process_book(self, registry: BookRegistry) -> BookRegistry:
        return generate_embeddings(self._model, registry)

    def _adaptive_batch_size(self, queue_size: int,) -> int:
        """
        Вычисляет адаптивный размер пакета для очереди.
        - queue_size: текущее количество элементов в очереди
        - max_batch: максимальный размер пакета
        """
        if queue_size < 10:
            # Если мало элементов, возвращаем число меньше 10
            return 5
        
        # Для больших чисел: округляем до ближайшего "красивого" числа
        # Красивое число — кратное 10, не больше max_batch
        batch = min(queue_size, self.max_batch_size)
        # Округление вниз до ближайшего кратного 10
        batch = (batch // 10) * 10
        return max(10, batch)
    
    async def _enrich_from_db(self, book: Book) -> Book:
        def _sync():
            with DB() as conn:
                return Book.map(BookRepository.get_full_by_file(conn, book.file_name))

        return await asyncio.to_thread(_sync)
             
    def _remove_invalid_embeddings(self, conn):
        to_delete = []
        expected_dim = None

        for book_id, emb in EmbeddingsRepository.get_all(conn):
            if expected_dim is None:
                expected_dim = emb.shape[0]

            if emb.shape[0] != expected_dim:
                print(f"Удаляем book_id={book_id} с размерностью {emb.shape}")
                to_delete.append(book_id)

        if to_delete:
            print(f"Удалено {len(to_delete)} записей")
        else:
            print("Все эмбеддинги корректны")