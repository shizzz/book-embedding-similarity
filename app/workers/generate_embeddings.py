import asyncio
from app.workers.base import BaseDbQueueWorker
from app.model import Model, generate_embeddings
from app.infrastructure.db import DBRouter, Migrator
from app.infrastructure.db.repositories import BookRepository, EmbeddingsRepository, AuthorRepository, ModelRepository, ChunkRepository
from app.infrastructure.models import Book, BookRegistry, Task, TaskResult, Action, Dataset
from app.searchEngines.bookSearch import BookSearchEngineFactory
from .sources.databaseReporter import DatabaseReporter
from app.settings import ChunkingConfig

class GenerateEmbeddingsWorker(BaseDbQueueWorker):
    def __init__(
            self, 
            max_batch_size: int = None,
            skip_embeddings: bool = False,
            **kwargs
        ):
        super().__init__(**kwargs)
        self._model = Model(self.max_workers)      
        self.ui.model_info = self._model.info
        self.engine = BookSearchEngineFactory.create(BookSearchEngineFactory.INPIX, self.ui)
        self._get_book_idx: int = None
        self._book_id: int = 1
        self._chunk_id: int = 1
        self._emb_id: int = 1
        self._skip_embeddings = skip_embeddings
        self._max_batch_size: int = max_batch_size or int(self._model.info.st_batch_size // (ChunkingConfig.CHUNKS_PER_BOOK + 2))
        self._searched_books = set()
        self._parsed = 0

    async def process(self, task: Task, _thread_id: int) -> TaskResult:
        if self._skip_embeddings:
            result = task.entity
        else:
            result = await asyncio.to_thread(self._process_book, task.entity)
        done = len(task.entity)
        return task.to_result(
            done=done,
            db_queue_count=done,
            entity=result
        )
    
    async def prepare(self) -> None:
        self._get_book_idx = self.ui.add_progress("Book parse", "book")
        
        self._model_id = ModelRepository(self._router).get_or_create(
            name=self._model.name,
            uid=self._model.info.uid
        )
        Migrator(self._router).migrate_embeddings(self._model.info.uid)
        
        self._book_id = BookRepository(self._router).get_max_id()
        self._chunk_id = ChunkRepository(self._router).get_max_id()
        self._emb_id = EmbeddingsRepository(self._router, self._model.info.uid).get_max_id()
    
    async def get_total(self) -> int:
        total = await self.engine.get_total()
        await self.ui.update_total_async(total, self._get_book_idx)
        return total

    async def pull_queue(self) -> None:
        file_to_id = BookRepository(self._router).get_file_to_id()
        chunk_to_book_id = ChunkRepository(self._router).get_ids()
        emb_to_book_id = EmbeddingsRepository(self._router, self._model.info.uid).get_ids()

        registries: dict[
            tuple[Action, frozenset[Dataset]],
            BookRegistry
        ] = {}

        async for book in self.engine.search_books():
            datasets: list[Dataset] = []
            self._searched_books.add(book.file_name)

            # Определяем action и datasets
            if book.file_name in file_to_id:
                book_id = file_to_id.get(book.file_name)

                already_done = book_id is not None and (book_id in chunk_to_book_id and book_id in emb_to_book_id)

                if not already_done:
                    action = Action.UPDATE
                    book = await self._enrich_from_db(book)

                    if not book.empty:
                        if len(book.chunks or []) == 0:
                            datasets.append(Dataset.CHUNK)
                        if len(book.embedding or []) == 0:
                            datasets.append(Dataset.EMBEDDING)

                if len(datasets) == 0 or book.empty or already_done:
                    await self._already_done()
                    del book
                    continue
            else:
                action = Action.INSERT
                datasets.extend([Dataset.BOOK, Dataset.CHUNK, Dataset.EMBEDDING, Dataset.AUTHOR])

            if len(book.chunks or []) == 0:
                await self.engine.enrich_book_data(book)
                book.empty = len(book.chunks) == 0
                if not book.empty:
                    for chunk in book.chunks:
                        chunk.chunk_id = self._chunk_id
                        self._chunk_id += 1

            # Назначаем ID если отсутствует
            if book.id is None:
                book.id = self._book_id
                self._book_id += 1
                for chunk in book.chunks:
                    chunk.book_id = book.id

            # Получаем registry по ключу
            key = (action, frozenset(datasets))
            registry = registries.setdefault(key, BookRegistry())
            registry.append(book)

            await self.ui.done_async(self._get_book_idx)

            if len(registry) >= self._max_batch_size:
                await self._queue_put(registry, key[1], action)
                # создаём новый registry для этого ключа
                registries[key] = BookRegistry()

        # Финальный flush всех registry
        for (action, datasets), registry in registries.items():
            if len(registry) > 0:
                await self._queue_put(registry, datasets, action)

        # shutdown workers
        await self.enqueue_shutdown_signals_async()
        self._queue_pulled.set()

    def save_to_db(self, router: DBRouter, task: Task) -> int:
        chunks = []
        embeddings = []
        for book in task.entity:
            if not book.empty:
                if len(book.chunks or []) > 0:
                    chunks.extend(book.chunks)
                if len(book.embedding or []) > 0:
                    embeddings.extend(book.embedding)

        with router.transaction() as tx:
            if Dataset.BOOK in task.dataset:
                BookRepository(router).save_bulk(
                    task.entity,
                    conn=tx.meta()
                )

            if Dataset.CHUNK in task.dataset and len(chunks) > 0:
                ChunkRepository(router).create_many(
                    chunks,
                    conn_meta=tx.meta(),
                    conn_chunks=tx.chunks()
                )

            if Dataset.EMBEDDING in task.dataset and len(embeddings) > 0:
                EmbeddingsRepository(router, self._model.info.uid).save_bulk(
                    embeddings,
                    conn=tx.embeddings(self._model.info.uid)
                )

            if Dataset.AUTHOR in task.dataset and task.action == Action.INSERT:
                AuthorRepository(router).save_bulk(
                    task.entity,
                    conn=tx.meta()
                )

        del chunks
        del embeddings

        return len(task.entity)

    async def fin(self) -> None: 
        self.logger.info("Generate report")
        report = DatabaseReporter(self._router, self._model.info.uid).generate(self._model.info.st_chunk_size, self._searched_books)
        self.ui.report(report)

    def _process_book(self, registry: BookRegistry) -> BookRegistry:
        result = generate_embeddings(self._model, registry)
        for book in result:
            if book.embedding:
                for emb in book.embedding:
                    emb.book_id = book.id
                    emb.id = self._emb_id
                    self._emb_id += 1
        return result

    async def _already_done(self): 
        await self.ui.done_async(self._get_book_idx)
        await self.ui.decrease_total_async()

    async def _queue_put(
            self, 
            registry: BookRegistry,
            datasets: list[Dataset],
            action: Action 
        ):
        first_book_name = getattr(registry.books[0], "file_name", "unknown")
        dataset_str = ":".join(ds.name for ds in datasets)

        await self.queue.put(
            Task(
                name=f"{first_book_name} {action.name} {dataset_str} ({len(registry)})",
                entity=registry,
                action=action,
                dataset=list(datasets)
            )
        )
    
    async def _enrich_from_db(self, book: Book) -> Book:
        def _sync(file_name: str):
            db_book = BookRepository(self._router).get_full_by_file(file_name)
            if not db_book:
                return None

            db_book.chunks = ChunkRepository(self._router).get_by_book(db_book.id)
            db_book.embedding = EmbeddingsRepository(self._router, self._model.info.uid).meta_only(db_book.id)

            return db_book

        db_book = await asyncio.to_thread(_sync, book.file_name)

        return book.merge_from(db_book)