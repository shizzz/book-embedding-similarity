from dataclasses import dataclass
from typing import Generic
from app.common.types import TEntity

@dataclass
class Task(Generic[TEntity]):
    name: str
    entity: TEntity