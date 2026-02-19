from dataclasses import dataclass
from typing import Generic
from enum import Enum, auto
from app.common.types import TEntity

class Action(Enum):
    INSERT = auto()
    DELETE = auto()
    UPDATE = auto()

@dataclass
class Task(Generic[TEntity]):
    name: str
    entity: TEntity
    action: Action

    def to_result(self, done: int = 0, entity: TEntity = None) -> "TaskResult[TEntity]":
        return TaskResult(
            name=self.name,
            action=self.action,
            entity=entity,
            done=done
        )

@dataclass
class TaskResult(Task):
    done: int = 0

    def to_task(self) -> "Task[TEntity]":
        return Task(
            name=self.name,
            action=self.action,
            entity=self.entity
        )