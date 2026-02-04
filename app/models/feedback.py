from pydantic import BaseModel
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Dict, Tuple

class FeedbackReq(BaseModel):
    source_file_name: str
    candidate_file_name: str
    label: int

@dataclass(frozen=True, slots=True)
class Feedback:
    source_file_name: str
    candidate_file_name: str
    label: int
    created_at: datetime = field(default_factory=datetime.now)

    def __post_init__(self):
        if self.label not in (1, -1):
            raise ValueError("label должен быть 1 или -1")

    @classmethod
    def from_row(cls, row):
        if len(row) >= 4:
            return cls(
                source_file_name=row[0],
                candidate_file_name=row[1],
                label=row[2],
                created_at=datetime.fromisoformat(row[3]) if row[3] else datetime.now(),
            )
        return None
        
@dataclass(slots=True)
class Feedbacks:
    feedbacks: List[Feedback]
    _pair_to_boost: Dict[Tuple[str, str], float] = field(default_factory=dict, init=False)

    def __post_init__(self):
        self._pair_to_boost = {}
        agg = {}
        for fb in self.feedbacks:
            key = (fb.source_file_name, fb.candidate_file_name)
            if key not in agg:
                agg[key] = []
            agg[key].append(fb.label)

        for key, labels in agg.items():
            avg = sum(labels) / len(labels)
            self._pair_to_boost[key] = avg

    def get_boost(self, source_fn: str, cand_fn: str, factor: float = 0.4) -> float:
            key = (source_fn, cand_fn)
            avg = self._pair_to_boost.get(key, 0.0)
            trust = 1.0
            return avg * factor * trust