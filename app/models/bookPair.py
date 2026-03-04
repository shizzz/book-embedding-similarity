from dataclasses import dataclass, field
from typing import Optional
import numpy as np
from .book import Book
from .feedback import Feedback

@dataclass
class BookPair:
    source: Book
    candidate: Book
    source_emb: np.ndarray
    candidate_emb: np.ndarray
    label: Optional[float] = None
    score: Optional[float] = None
    meta: dict = field(default_factory=dict)

    # --- конвертер из Feedback ---
    @classmethod
    def fromFeedback(cls, fb: Feedback, books: dict[int, Book], embeddings: dict[int, np.ndarray]):
        src = books.get(fb.source_id)
        cand = books.get(fb.candidate_id)
        if not src or not cand:
            return None
        src_emb = embeddings.get(fb.source_id)
        cand_emb = embeddings.get(fb.candidate_id)
        if src_emb is None or cand_emb is None:
            return None
        return cls(
            source=src,
            candidate=cand,
            source_emb=src_emb,
            candidate_emb=cand_emb,
            label=fb.label
        )

    # --- конвертер из похожих кандидатов (FAISS/HNSW) ---
    @classmethod
    def fromSearch(cls, source_id: int, candidate_id: int, score: float, books: dict[int, Book], embeddings: dict[int, np.ndarray], meta: dict = None):
        src = books.get(source_id)
        cand = books.get(candidate_id)
        if not src or not cand:
            return None
        src_emb = embeddings.get(source_id)
        cand_emb = embeddings.get(candidate_id)
        if src_emb is None or cand_emb is None:
            return None
        return cls(
            source=src,
            candidate=cand,
            source_emb=src_emb,
            candidate_emb=cand_emb,
            score=score,
            meta=meta or {}
        )

    def asdict(self) -> dict:
        return {
            "source_id": self.source.id,
            "candidate_id": self.candidate.id,
            "source_emb": self.source_emb,
            "candidate_emb": self.candidate_emb,
            "label": self.label,
            "source": self.source,
            "candidate": self.candidate,
            **self.meta
        }