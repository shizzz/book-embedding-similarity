from dataclasses import dataclass
from typing import Generic, List
from enum import Enum, auto
from app.common.types import TEntity

class Action(Enum):
    INSERT = auto()
    DELETE = auto()
    UPDATE = auto()

class Dataset(Enum):
    BOOK = auto()
    EMBEDDING = auto()
    AUTHOR = auto()

@dataclass
class Task(Generic[TEntity]):
    name: str
    entity: TEntity
    action: Action
    dataset: List[Dataset] = None

    def to_result(
            self,
            done: int = 0,
            db_queue_count: int = 0,
            entity: TEntity = None
    ) -> "TaskResult[TEntity]":
        return TaskResult(
            name=self.name,
            action=self.action,
            dataset=self.dataset,
            entity=entity,
            done=done,
            db_queue_count=db_queue_count
        )

@dataclass
class TaskResult(Task):
    done: int = 0
    db_queue_count: int = 0

    def to_task(self) -> "Task[TEntity]":
        return Task(
            name=self.name,
            action=self.action,
            dataset=self.dataset,
            entity=self.entity
        )