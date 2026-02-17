from dataclasses import dataclass
from typing import Any

@dataclass
class Task:
    name: str
    entity: Any

    def __init__(self, name: str, entity: Any):
        self.name = name
        self.entity = entity