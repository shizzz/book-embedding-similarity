from dataclasses import dataclass
from typing import List

@dataclass(frozen=True, slots=True)
class SearchResult:
    Source: int
    Candidate: int
    Score: float
    ChunkIds: List[int] = None