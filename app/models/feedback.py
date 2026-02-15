from sqlite3 import Row
from pydantic import BaseModel
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Dict, Tuple
from app.settings.config import FEEDBACK_BOOST_FACTOR

class FeedbackReq(BaseModel):
    source_file_name: str
    candidate_file_name: str
    label: float

@dataclass(frozen=True, slots=True)
class Feedback:
    source_id: str
    candidate_id: str
    label: float
    created_at: datetime = field(default_factory=datetime.now)
          
    @staticmethod
    def map(row: Row) -> "Feedback":
        return Feedback(
            source_id=row[0],
            candidate_id=row[1],
            label=row[2],
            created_at=row[3] if len(row) > 3 else datetime.now()
        )

    def to_dict(self):
        return {
            "source_id": self.source_id,
            "candidate_id": self.candidate_id,
            "label": self.label
        }
    
@dataclass(slots=True)
class Feedbacks:
    items: List[Feedback]
    _pair_to_boost: Dict[Tuple[int, int], float] = field(default_factory=dict, init=False)

    def __init__(self, rows: list[Row] | None = None):
        if rows is None:
            self.items = []
        else:
            self.items = [Feedback.map(row) for row in rows]

        self._pair_to_boost = {}
        self._build_index()

    def _build_index(self):
        for fb in self.items:
            self._pair_to_boost[(fb.source_id, fb.candidate_id)] = fb.label

    def __post_init__(self):
        self._pair_to_boost = {}
        agg = {}
        for fb in self.items:
            key = (fb.source_id, fb.candidate_id)
            if key not in agg:
                agg[key] = []
            agg[key].append(fb.label)

        for key, labels in agg.items():
            avg = sum(labels) / len(labels)
            self._pair_to_boost[key] = avg

    def get_boost(self, source_fn: int, cand_fn: int, factor: float = FEEDBACK_BOOST_FACTOR) -> float:
            key = (source_fn, cand_fn)
            avg = self._pair_to_boost.get(key, 0.0)
            trust = 1.0
            return avg * factor * trust
    
    def get_rating(self, source_id: int, candidate_id: int) -> float:
        return self._pair_to_boost.get((source_id, candidate_id), 0.0)