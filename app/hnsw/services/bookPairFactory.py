import numpy as np
from app.models import BookPair, Feedbacks, Book

class BookPairFactory:
    def create_pairs(
        self, 
        feedbacks: Feedbacks, 
        books: dict[int, Book], 
        embeddings: dict[int, np.ndarray]
    ):
        pairs = []

        for fb in feedbacks:
            src = books.get(fb.source_id)
            cand = books.get(fb.candidate_id)

            src_emb = embeddings.get(fb.source_id)
            cand_emb = embeddings.get(fb.candidate_id)

            if not src or not cand or src_emb is None or cand_emb is None:
                continue

            pairs.append(
                BookPair(src, cand, src_emb, cand_emb, fb.label)
            )

        return pairs