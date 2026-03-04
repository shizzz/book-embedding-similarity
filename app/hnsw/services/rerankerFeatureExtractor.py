import numpy as np
from app.models import BookPair

class RerankerFeatureExtractor:
    def extract(self, pair: BookPair) -> list[float]:
        cosine = self._cosine(pair.source_emb, pair.candidate_emb)

        same_author = int(
            pair.source.author and
            pair.source.author == pair.candidate.author
        )

        genre_overlap = len(
            set(pair.source.generes or []) &
            set(pair.candidate.generes or [])
        )

        year_diff = abs((pair.source.year or 0) - (pair.candidate.year or 0)) / 50

        return [
            cosine,
            same_author,
            genre_overlap,
            year_diff
        ]

    def _cosine(self, a, b):
        return float(
            np.dot(a, b) /
            (np.linalg.norm(a) * np.linalg.norm(b) + 1e-8)
        )