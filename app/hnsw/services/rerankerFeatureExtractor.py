import numpy as np
from app.infrastructure.models import BookPair

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
        def sim(a: np.ndarray | None, b: np.ndarray | None) -> tuple[float, float, int]:
            if a is None or b is None:
                return 0.0, 0.0, 0
            dot = float(np.dot(a, b))
            cos = dot / (np.linalg.norm(a) * np.linalg.norm(b) + 1e-8)
            return cos, dot, 1

        # TEXT
        cosine_score, dot_score, has_text = sim(pair.source_emb, pair.candidate_emb)
        # TITLE (может отсутствовать)
        title_cosine, title_dot, has_title = sim(pair.source_title_emb, pair.candidate_title_emb)
        # DESCRIPTION (может отсутствовать)
        desc_cosine, desc_dot, has_desc = sim(pair.source_description_emb, pair.candidate_description_emb)

        # автор
        source_set = {x.strip() for x in (pair.source.author or "").split("||") if x.strip()}
        candidate_set = {x.strip() for x in (pair.candidate.author or "").split("||") if x.strip()}
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
            title_cosine,
            title_dot,
            has_title,
            desc_cosine,
            desc_dot,
            has_desc,
            same_author,
            same_serie,
            genre_overlap,
            year_diff
        ]