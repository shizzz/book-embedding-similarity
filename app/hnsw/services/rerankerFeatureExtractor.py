import numpy as np
from app.models import BookPair

class RerankerFeatureExtractor:
    def extract(self, pair: BookPair) -> list[float]:
        """
        Возвращает признаки для reranker:
        [cosine_score, dot_score, same_author, same_serie, genre_overlap, year_diff]
        """
        # matched_chunks = pair.meta.get("matched_chunks")
        # if matched_chunks:
        #     a = np.mean([m["query_embedding"] for m in matched_chunks], axis=0)
        #     b = np.mean([m["embedding"] for m in matched_chunks], axis=0)
        # else:
        #     a = pair.source_emb
        #     b = pair.candidate_emb
        a = pair.source_emb
        b = pair.candidate_emb

        # косинус и dot
        dot_score = float(np.dot(a, b))
        cosine_score = dot_score / (np.linalg.norm(a) * np.linalg.norm(b) + 1e-8)

        # автор
        source_set = {x.strip() for x in (pair.source.author or "").split(",") if x.strip()}
        candidate_set = {x.strip() for x in (pair.candidate.author or "").split(",") if x.strip()}
        same_author = 1 if source_set & candidate_set else 0

        # серия
        source_serie = (pair.source.serie or "").strip()
        candidate_serie = (pair.candidate.serie or "").strip()
        same_serie = 1 if source_serie and candidate_serie and source_serie == candidate_serie else 0

        # жанры
        source_genres = set(pair.source.generes or [])
        candidate_genres = set(pair.candidate.generes or [])
        genre_overlap = len(source_genres & candidate_genres)

        # разница в годах (нормализуем на 50 лет)
        year_diff = abs((pair.source.year or 0) - (pair.candidate.year or 0)) / 50

        return [
            cosine_score,
            dot_score,
            same_author,
            same_serie,
            genre_overlap,
            year_diff
        ]