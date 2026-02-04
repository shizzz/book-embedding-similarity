from pydantic import BaseModel
from dataclasses import dataclass, field
from datetime import datetime
from typing import List

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
        
class Feedbacks:
    feedbacks: List[Feedback]

    def __init__(self, feedbacks: List[Feedback]):
        self.feedbacks = feedbacks

    def get_boost(self, source_file_name: str, candidate_file_name: str, factor: float = 0.4) -> float:
        relevant_labels = [
            fb.label for fb in self.feedbacks
            if fb.source_file_name == source_file_name
            and fb.candidate_file_name == candidate_file_name
        ]

        if not relevant_labels:
            return 0.0

        avg_label = sum(relevant_labels) / len(relevant_labels)
        count = len(relevant_labels)
        trust = min(count / 5.0, 1.0)  # доверие растёт до 5 оценок

        return avg_label * factor * trust