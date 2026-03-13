from dataclasses import dataclass, field
from typing import Optional
import numpy as np
from .book import Book
from .feedback import Feedback
from .constants import ChunkType
from .search_result import SearchResult

@dataclass
class BookPair:
    source: Book
    candidate: Book
    source_emb: np.ndarray
    candidate_emb: np.ndarray
    source_title_emb: Optional[np.ndarray] = None
    candidate_title_emb: Optional[np.ndarray] = None
    source_description_emb: Optional[np.ndarray] = None
    candidate_description_emb: Optional[np.ndarray] = None
    label: Optional[float] = None
    score: Optional[float] = None
    meta: dict = field(default_factory=dict)

    # --- конвертер из Feedback ---
    @classmethod
    def fromFeedback(
        cls, 
        fb: Feedback, 
        books: dict[int, Book], 
        embeddings: dict[int, dict[ChunkType, np.ndarray]]
    ):
        src = books.get(fb.source_id)
        cand = books.get(fb.candidate_id)

        if not src or not cand:
            return None
        
        src_map = embeddings.get(fb.source_id) or {}
        cand_map = embeddings.get(fb.candidate_id) or {}
        src_emb = src_map.get(ChunkType.TEXT)
        cand_emb = cand_map.get(ChunkType.TEXT)

        if src_emb is None or cand_emb is None:
            return None
        
        return cls(
            source=src,
            candidate=cand,
            source_emb=src_emb,
            candidate_emb=cand_emb,
            source_title_emb=src_map.get(ChunkType.TITLE),
            candidate_title_emb=cand_map.get(ChunkType.TITLE),
            source_description_emb=src_map.get(ChunkType.DESCRIPTION),
            candidate_description_emb=cand_map.get(ChunkType.DESCRIPTION),
            label=fb.label
        )

    # --- конвертер из похожих кандидатов (FAISS/HNSW) ---
    @classmethod
    def fromSearch(
        cls, 
        searchResult: SearchResult,
        books: dict[int, Book], 
        embeddings: dict[int, dict[ChunkType, np.ndarray]]
    ):
        src = books.get(searchResult.Source)
        cand = books.get(searchResult.Candidate)

        if not src or not cand:
            return None
        
        src_map = embeddings.get(searchResult.Source) or {}
        cand_map = embeddings.get(searchResult.Candidate) or {}
        src_emb = src_map.get(ChunkType.TEXT)
        cand_emb = cand_map.get(ChunkType.TEXT)

        if src_emb is None or cand_emb is None:
            return None
        
        return cls(
            source=src,
            candidate=cand,
            source_emb=src_emb,
            candidate_emb=cand_emb,
            source_title_emb=src_map.get(ChunkType.TITLE),
            candidate_title_emb=cand_map.get(ChunkType.TITLE),
            source_description_emb=src_map.get(ChunkType.DESCRIPTION),
            candidate_description_emb=cand_map.get(ChunkType.DESCRIPTION),
            score=searchResult.Score
        )

    def asdict(self) -> dict:
        return {
            "source_id": self.source.id,
            "candidate_id": self.candidate.id,
            "source_emb": self.source_emb,
            "candidate_emb": self.candidate_emb,
            "source_title_emb": self.source_title_emb,
            "candidate_title_emb": self.candidate_title_emb,
            "source_description_emb": self.source_description_emb,
            "candidate_description_emb": self.candidate_description_emb,
            "label": self.label,
            "source": self.source,
            "candidate": self.candidate,
            **self.meta
        }