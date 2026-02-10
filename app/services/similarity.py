import time
import asyncio
from typing import Optional, List, Tuple

from app.db import db, SimilarRepository
from app.models import Book, Embedding
from app.searchEngines import SimilarSearchEngineFactory
from app.services import SimilarSearchService

class TaskState:
    def __init__(self):
        self.progress: int = 0
        self.result: Optional[List[Tuple[float, int, int]]] = None
        self.error: Optional[str] = None
        self.start_time: float = time.perf_counter()
        self.done_event = asyncio.Event()

    def set_progress(self, percent: int):
        self.progress = percent

    def set_done(self, similars: List[Tuple[float, int, int]]):
        self.result = similars
        self.done_event.set()

    def set_error(self, msg: str):
        self.error = msg
        self.done_event.set()

class Similarity:
    def __init__(self):
        self.tasks: dict[str, TaskState] = {}

    def remove_task(self, file_name: str):
        self.tasks.pop(file_name, None)

    def update_progress(self, file_name: str, percent: int):
        if file_name in self.tasks:
            self.tasks[file_name].set_progress(percent)

    def compute_similar(
        self, 
        book: Book,
        embedding_bytes: bytes,
        limit: int,
        exclude_same_author: bool
    ):
        state = self.tasks.get(book.file_name)
        if not state:
            return

        try:
            engine = SimilarSearchEngineFactory.create(
                SimilarSearchEngineFactory.INDEX, limit, exclude_same_author, 1
            )
            service = SimilarSearchService(
                engine=engine,
                source=book,
                embedding=Embedding.from_db(embedding_bytes)
            )

            similars = service.run(
                progress_callback=lambda p: self.update_progress(book.file_name, p)
            )

            with db() as conn:
                SimilarRepository().replace(conn, similars)

            state.set_done(similars)

        except Exception as e:
            state.set_error(str(e))