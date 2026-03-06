from dataclasses import dataclass
from typing import Generic, List
from enum import Enum, IntEnum, auto
from app.common.types import TEntity
from .book import Book

class Action(IntEnum):
    INSERT = auto()
    DELETE = auto()
    UPDATE = auto()

class Dataset(Enum):
    BOOK = auto()
    EMBEDDING = auto()
    AUTHOR = auto()
    CHUNK = auto()

class BookAction(IntEnum):
    CHUNK_FROM_BOOK = auto()
    CHUNK_FROM_DB = auto()

@dataclass
class Task(Generic[TEntity]):
    id: int
    name: str
    entity: TEntity
    action: IntEnum = None
    dataset: List[Dataset] = None

    def to_result(
            self,
            done: int = 0,
            db_queue_count: int = 0,
            entity: TEntity = None
    ) -> "TaskResult[TEntity]":
        return TaskResult(
            id=self.id,
            name=self.name,
            action=self.action,
            dataset=self.dataset,
            entity=entity,
            done=done,
            db_queue_count=db_queue_count
        )

    def clone(self):
        return Task(self.entity, self.id)

@dataclass
class TaskResult(Task):
    done: int = 0
    db_queue_count: int = 0

    def to_task(self) -> "Task[TEntity]":
        return Task(
            id=self.id,
            name=self.name,
            action=self.action,
            dataset=self.dataset,
            entity=self.entity
        )
    
@dataclass
class BookTask:
    book: Book
    data: bytes
    action: BookAction