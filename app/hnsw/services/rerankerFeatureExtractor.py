import numpy as np
from typing import List
from app.infrastructure.models import BookPair, BookTag, ChunkType

class RerankerFeatureExtractor:
    def __init__(
        self,
        tag_ids: List[int],
        cnt_ids: List[int],
    ):
        self.tag_ids = tag_ids      # все возможные genre_id тегов
        self.cnt_ids = cnt_ids      # все возможные id центроидов

    def extract(self, pair: BookPair) -> list[float]:
        """
        Возвращает признаки для reranker на основе тегов и центроидов:
        [cosine_score_tags, dot_score_tags, cosine_score_centroids, dot_score_centroids,
         same_author, same_serie, genre_overlap, year_diff]
        """
        def dict_to_vector(d: dict[int, float], all_keys: list[int]) -> np.ndarray:
            # Вектор фиксированной длины по всем ключам
            vec = np.array([d.get(k, 0.0) for k in all_keys], dtype=np.float32)
            # Нормализация для стабильного cosine/dot
            norm = np.linalg.norm(vec)
            if norm > 0:
                vec /= norm
            return vec

        def tags_to_dict(tags: list[BookTag] | None) -> dict[int, float]:
            # Преобразует список тегов в {id: distance}
            if not tags:
                return {}
            result = {}
            for tag in tags:
                if tag.genre_id is not None:
                    dist = float(np.linalg.norm(tag.distance)) if tag.distance is not None and np.linalg.norm(tag.distance) > 1e-6 else 1.0
                    result[tag.genre_id] = dist
            return result

        def cosine_dot(a: list[BookTag] | None, b: list[BookTag] | None, all_keys: list[int]) -> tuple[float, float]:
            if not a or not b:
                return 0.0, 0.0

            va = dict_to_vector(tags_to_dict(a), all_keys)
            vb = dict_to_vector(tags_to_dict(b), all_keys)
            dot = float(np.dot(va, vb))
            cos = float(np.dot(va, vb))  # нормализованные вектора → dot == cosine
            return cos, dot
        
        def sim(a: np.ndarray | None, b: np.ndarray | None) -> tuple[float, float, int]:
            if a is None or b is None:
                return 0.0, 0.0, 0
            dot = float(np.dot(a, b))
            cos = dot / (np.linalg.norm(a) * np.linalg.norm(b) + 1e-8)
            return cos, dot, 1

        # теги
        cosine_tags, dot_tags = cosine_dot(pair.source.tags, pair.candidate.tags, self.tag_ids)
        # центроиды
        cosine_centroids, dot_centroids = cosine_dot(pair.source.centroids, pair.candidate.centroids, self.cnt_ids)


        # TEXT
        text_cosine, text_dot, _ = sim(pair.source_emb, pair.candidate_emb)
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

        # жанры (оставляем как отдельный признак)
        source_genres = set(pair.source.generes or [])
        candidate_genres = set(pair.candidate.generes or [])
        genre_overlap = len(source_genres & candidate_genres)

        # разница в годах (нормализуем на 50 лет)
        year_diff = abs((pair.source.year or 0) - (pair.candidate.year or 0)) / 50

        return [
            # Данные исходного текста
            text_cosine,
            text_dot,

            # Данные названия книги
            has_title,
            title_cosine,
            title_dot,

            # Данные описания
            has_desc,
            desc_cosine,
            desc_dot,

            # Данные сгенерированных тэгов
            cosine_tags,
            dot_tags,

            # Данные центроидов
            cosine_centroids,
            dot_centroids,

            # Метаданные книги
            same_author,
            same_serie,
            genre_overlap,
            year_diff
        ]