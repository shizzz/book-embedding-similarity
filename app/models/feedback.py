from sqlite3 import Row
from pydantic import BaseModel
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Dict, Tuple, Optional
from app.db import db, FeedbackRepository
from app.settings.config import FEEDBACK_BOOST_FACTOR

class FeedbackReq(BaseModel):
    source_file_name: str
    candidate_file_name: str
    label: float

@dataclass(frozen=True, slots=True)
class Feedback:
    id: Optional[int]
    source_id: str
    candidate_id: str
    label: float
    created_at: datetime = field(default_factory=datetime.now)
          
    @staticmethod
    def map(row: Row) -> "Feedback":
        return Feedback(
            id=row[0],
            source_id=row[1],
            candidate_id=row[2],
            label=row[3],
            created_at=row[4] if len(row) > 3 else datetime.now()
        )

    @staticmethod
    def map_from_dict(data: dict) -> "Feedback":
        return Feedback(
            id=None,
            source_id=data["source_id"],
            candidate_id=data["candidate_id"],
            label=float(data["label"]),
            created_at=datetime.fromisoformat(data["created_at"])
            if "created_at" in data and data["created_at"]
            else datetime.now(),
        )
    
    def to_db_tuple(self) -> tuple:
        return (
            self.source_id,
            self.candidate_id,
            self.label,
            self.created_at,
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

    @classmethod
    def from_dicts(cls, data: list[dict]) -> "Feedbacks":
        obj = cls(None)

        obj.items = [
            Feedback(
                id=None,
                source_id=row["source_id"],
                candidate_id=row["candidate_id"],
                label=float(row["label"]),
                created_at=datetime.fromisoformat(row["created_at"])
                if "created_at" in row and row["created_at"]
                else datetime.now(),
            )
            for row in data
        ]

        obj._pair_to_boost = {}
        obj._build_index()

        return obj
    
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
    
    def insert_feedbacks(self, conn):
        FeedbackRepository.insert_many(
            conn,
            [fb.to_db_tuple() for fb in self.items]
        )