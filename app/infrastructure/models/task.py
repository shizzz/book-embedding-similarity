from dataclasses import dataclass, field
from typing import Generic, Optional, List
from enum import IntEnum, auto
from app.common.types import TEntity
from .book import Book

class Action(IntEnum):
    BOOK = auto()
    CHUNK = auto()
    EMBEDDING = auto()
    BOTH = auto()
    NONE = auto()

class Dataset(IntEnum):
    BOOK = auto()
    EMBEDDING = auto()
    AUTHOR = auto()
    CHUNK = auto()

class BookAction(IntEnum):
    BOOK = auto()
    CHUNK = auto()

@dataclass(slots=True)
class Task(Generic[TEntity]):
    id: int
    name: str
    entity: TEntity

    action: Optional[IntEnum] = None
    dataset: Optional[Dataset] = None

    done: int = 1
    db_queue_count: int = 0

    def clone(self, *, entity: Optional[TEntity] = None) -> "Task[TEntity]":
        return Task(
            id=self.id,
            name=self.name,
            entity=entity if entity is not None else self.entity,
            action=self.action,
            dataset=self.dataset,
            done=self.done,
            db_queue_count=self.db_queue_count,
        )
    
class BatchTask(Task[list[TEntity]]):
    pass

@dataclass
class BookTask:
    book: Book
    data: bytes
    action: BookAction