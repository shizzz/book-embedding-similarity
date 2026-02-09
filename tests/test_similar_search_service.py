import unittest
from contextlib import contextmanager
from unittest.mock import patch

import numpy as np

from app.models import Book, Embedding
from app.services.similar_search_service import SimilarSearchService


class _FakeFeedbacks:
    def __init__(self, boost_by_pair=None):
        self._boost_by_pair = boost_by_pair or {}

    def get_boost(self, source_id, candidate_id):
        return float(self._boost_by_pair.get((source_id, candidate_id), 0.0))


def _vec_bytes(values):
    return np.asarray(values, dtype=np.float32).tobytes()


class TestSimilarSearchService(unittest.TestCase):
    def test_run_returns_empty_when_embedding_none_and_does_not_open_db_in_run(self):
        db_enters = {"count": 0}

        @contextmanager
        def fake_db():
            db_enters["count"] += 1
            yield object()

        source = Book(archive_name="a.zip", file_name="b.fb2", id=1, title="B")

        with patch("app.services.similar_search_service.db", fake_db), patch(
            "app.services.similar_search_service.BookRepository.count_embeddings",
            return_value=0,
        ):
            service = SimilarSearchService(
                source=source,
                embedding=None,
                limit=10,
                step_percent=5,
            )

            # __init__ opens DB once for embeddings count
            self.assertEqual(db_enters["count"], 1)

            result = service.run()
            self.assertEqual(result, [])

            # run() should not open db() at all when embedding is None
            self.assertEqual(db_enters["count"], 1)

    def test_run_scores_sorts_and_applies_feedback_boost(self):
        @contextmanager
        def fake_db():
            yield object()

        source = Book(archive_name="a.zip", file_name="src.fb2", id=1, title="SRC")
        source_emb = Embedding.from_db(_vec_bytes([1.0, 0.0]))

        rows = [
            (2, _vec_bytes([1.0, 0.0])),   # sim ~ 1.0
            (3, _vec_bytes([0.0, 1.0])),   # sim ~ 0.0
            (4, _vec_bytes([-1.0, 0.0])),  # sim ~ -1.0
        ]

        # Big boost for id=4 to outrank id=2
        feedbacks = _FakeFeedbacks(
            {
                (1, 3): 0.5,
                # Make boosted score for id=4 strictly larger than for id=2
                # sim(1,[1,0]) ~= 1.0, sim(1,[-1,0]) ~= -1.0
                # => id=2: 1.0 + 0.0 = 1.0
                #    id=4: -1.0 + 3.0 = 2.0  (> 1.0)
                (1, 4): 3.0,
            }
        )

        def fake_get_many(self, conn, book_ids):
            return {
                2: Book(archive_name="a.zip", file_name="c2.fb2", id=2, title="C2"),
                3: Book(archive_name="a.zip", file_name="c3.fb2", id=3, title="C3"),
                4: Book(archive_name="a.zip", file_name="c4.fb2", id=4, title="C4"),
            }

        with patch("app.services.similar_search_service.db", fake_db), patch(
            "app.services.similar_search_service.BookRepository.count_embeddings",
            return_value=10,
        ), patch(
            "app.services.similar_search_service.FeedbackRepository.get",
            return_value=feedbacks,
        ), patch(
            "app.services.similar_search_service.EmbeddingsRepository.get_all",
            return_value=iter(rows),
        ), patch(
            "app.services.similar_search_service.BookRepository.get_many",
            new=fake_get_many,
        ):
            service = SimilarSearchService(
                source=source,
                embedding=source_emb,
                limit=2,
                step_percent=5,
            )

            result = service.run()

        self.assertEqual(len(result), 2)
        self.assertEqual([s.similar_book_id for s in result], [4, 2])
        self.assertTrue(result[0].score > result[1].score)
        self.assertEqual(result[0].book_id, 1)
        self.assertEqual(result[1].book_id, 1)

    def test_run_skips_bad_embeddings_and_missing_candidate_books(self):
        @contextmanager
        def fake_db():
            yield object()

        source = Book(archive_name="a.zip", file_name="src.fb2", id=1, title="SRC")
        source_emb = Embedding.from_db(_vec_bytes([1.0, 0.0]))

        # Candidate 2: zero vector -> Embedding.from_db returns None -> should be skipped
        # Candidate 99: good embedding but will be missing from get_many -> should be skipped
        # Candidate 3: good embedding + present -> should remain
        rows = [
            (2, _vec_bytes([0.0, 0.0])),
            (99, _vec_bytes([1.0, 0.0])),
            (3, _vec_bytes([0.0, 1.0])),
        ]

        def fake_get_many(self, conn, book_ids):
            # Intentionally omit id=99
            return {
                3: Book(archive_name="a.zip", file_name="c3.fb2", id=3, title="C3"),
            }

        with patch("app.services.similar_search_service.db", fake_db), patch(
            "app.services.similar_search_service.BookRepository.count_embeddings",
            return_value=10,
        ), patch(
            "app.services.similar_search_service.FeedbackRepository.get",
            return_value=_FakeFeedbacks({}),
        ), patch(
            "app.services.similar_search_service.EmbeddingsRepository.get_all",
            return_value=iter(rows),
        ), patch(
            "app.services.similar_search_service.BookRepository.get_many",
            new=fake_get_many,
        ):
            service = SimilarSearchService(
                source=source,
                embedding=source_emb,
                limit=10,
                step_percent=5,
            )
            result = service.run()

        # Only candidate 3 should survive (2 invalid embedding, 99 missing book)
        self.assertEqual([s.similar_book_id for s in result], [3])

    def test_progress_callback_emits_step_updates_and_final_100(self):
        @contextmanager
        def fake_db():
            yield object()

        source = Book(archive_name="a.zip", file_name="src.fb2", id=1, title="SRC")
        source_emb = Embedding.from_db(_vec_bytes([1.0, 0.0]))

        # total=10, step_percent=20 => step=2
        rows = [
            (2, _vec_bytes([1.0, 0.0])),
            (3, _vec_bytes([1.0, 0.0])),
            (4, _vec_bytes([1.0, 0.0])),
            (5, _vec_bytes([1.0, 0.0])),
            (6, _vec_bytes([1.0, 0.0])),
        ]

        def fake_get_many(self, conn, book_ids):
            return {
                bid: Book(archive_name="a.zip", file_name=f"c{bid}.fb2", id=bid, title=f"C{bid}")
                for bid in book_ids
            }

        calls = []

        def cb(p):
            calls.append(p)

        with patch("app.services.similar_search_service.db", fake_db), patch(
            "app.services.similar_search_service.BookRepository.count_embeddings",
            return_value=10,
        ), patch(
            "app.services.similar_search_service.FeedbackRepository.get",
            return_value=_FakeFeedbacks({}),
        ), patch(
            "app.services.similar_search_service.EmbeddingsRepository.get_all",
            return_value=iter(rows),
        ), patch(
            "app.services.similar_search_service.BookRepository.get_many",
            new=fake_get_many,
        ):
            service = SimilarSearchService(
                source=source,
                embedding=source_emb,
                limit=5,
                step_percent=20,
            )
            _ = service.run(progress_callback=cb)

        # For 5 processed rows with step=2: callbacks at current=2 (20%), 4 (40%), then final 100
        self.assertEqual(calls, [20, 40, 100])

    def test_index_mode_uses_hnsw_and_builds_similar_results(self):
        @contextmanager
        def fake_db():
            yield object()

        source = Book(archive_name="a.zip", file_name="src.fb2", id=1, title="SRC")
        source_emb = Embedding.from_db(_vec_bytes([1.0, 0.0]))

        # Мэппинг индексов HNSW -> book_id будет [2, 3]
        rows = [
            (2, _vec_bytes([1.0, 0.0])),
            (3, _vec_bytes([0.0, 1.0])),
        ]

        class FakeIndex:
            def __init__(self):
                self.ntotal = 2

            def search(self, query, k):
                # Для METRIC_INNER_PRODUCT search возвращает скалярные произведения,
                # чем больше значение, тем ближе кандидат к запросу.
                return [[0.9, 0.8]], [[0, 1]]

        class FakeHNSWService:
            def __init__(self, *args, **kwargs):
                pass

            def load_from_file(self):
                return FakeIndex()

        def fake_get_many(self, conn, book_ids):
            return {
                2: Book(archive_name="a.zip", file_name="c2.fb2", id=2, title="C2"),
                3: Book(archive_name="a.zip", file_name="c3.fb2", id=3, title="C3"),
            }

        with patch("app.services.similar_search_service.db", fake_db), patch(
            "app.services.similar_search_service.BookRepository.count_embeddings",
            return_value=2,
        ), patch(
            "app.services.similar_search_service.FeedbackRepository.get",
            return_value=_FakeFeedbacks({}),
        ), patch(
            "app.services.similar_search_service.EmbeddingsRepository.get_all",
            return_value=iter(rows),
        ), patch(
            "app.services.similar_search_service.BookRepository.get_many",
            new=fake_get_many,
        ), patch(
            "app.services.similar_search_service.HNSWService",
            FakeHNSWService,
        ):
            service = SimilarSearchService(
                source=source,
                embedding=source_emb,
                limit=2,
                step_percent=5,
                mode="index",
            )
            result = service.run()

        self.assertEqual(len(result), 2)
        # Должен соблюдаться порядок по скору: сначала book_id=2, потом 3
        self.assertEqual([s.similar_book_id for s in result], [2, 3])

    def test_index_mode_produces_same_result_as_bruteforce(self):
        @contextmanager
        def fake_db():
            yield object()

        source = Book(archive_name="a.zip", file_name="src.fb2", id=1, title="SRC")
        source_emb = Embedding.from_db(_vec_bytes([1.0, 0.0]))

        # Три кандидата с разной косинусной близостью
        rows = [
            (2, _vec_bytes([1.0, 0.0])),   # sim ~ 1.0
            (3, _vec_bytes([0.0, 1.0])),   # sim ~ 0.0
            (4, _vec_bytes([-1.0, 0.0])),  # sim ~ -1.0
        ]

        # Без фидбека, чтобы сравнивать только «сырую» похожесть
        feedbacks = _FakeFeedbacks({})

        # Предварительно считаем точные скоры, как делает брутфорс
        emb_vecs = [Embedding.from_db(b).vec for _, b in rows]
        sims = [float(np.dot(v, source_emb.vec)) for v in emb_vecs]

        class FakeIndex:
            def __init__(self, sims_local):
                self.ntotal = len(sims_local)
                self._sims = sims_local

            def search(self, query, k):
                # Имитация HNSW с METRIC_INNER_PRODUCT: search возвращает сами скалярные произведения.
                order = sorted(
                    range(len(self._sims)),
                    key=lambda i: self._sims[i],
                    reverse=True,
                )
                top = order[:k]
                dists = [self._sims[i] for i in top]
                return [dists], [top]

        class FakeHNSWService:
            def __init__(self, *args, **kwargs):
                pass

            def load_from_file(self):
                return FakeIndex(sims)

        def fake_get_many(self, conn, book_ids):
            return {
                2: Book(archive_name="a.zip", file_name="c2.fb2", id=2, title="C2"),
                3: Book(archive_name="a.zip", file_name="c3.fb2", id=3, title="C3"),
                4: Book(archive_name="a.zip", file_name="c4.fb2", id=4, title="C4"),
            }

        # Брутфорс — источник истины
        with patch("app.services.similar_search_service.db", fake_db), patch(
            "app.services.similar_search_service.BookRepository.count_embeddings",
            return_value=len(rows),
        ), patch(
            "app.services.similar_search_service.FeedbackRepository.get",
            return_value=feedbacks,
        ), patch(
            "app.services.similar_search_service.EmbeddingsRepository.get_all",
            side_effect=lambda conn: iter(rows),
        ), patch(
            "app.services.similar_search_service.BookRepository.get_many",
            new=fake_get_many,
        ):
            brute_service = SimilarSearchService(
                source=source,
                embedding=source_emb,
                limit=3,
                step_percent=5,
                mode="bruteforce",
            )
            brute_result = brute_service.run()

        # Индексный режим должен дать тот же результат
        with patch("app.services.similar_search_service.db", fake_db), patch(
            "app.services.similar_search_service.BookRepository.count_embeddings",
            return_value=len(rows),
        ), patch(
            "app.services.similar_search_service.FeedbackRepository.get",
            return_value=feedbacks,
        ), patch(
            "app.services.similar_search_service.EmbeddingsRepository.get_all",
            side_effect=lambda conn: iter(rows),
        ), patch(
            "app.services.similar_search_service.BookRepository.get_many",
            new=fake_get_many,
        ), patch(
            "app.services.similar_search_service.HNSWService",
            FakeHNSWService,
        ):
            index_service = SimilarSearchService(
                source=source,
                embedding=source_emb,
                limit=3,
                step_percent=5,
                mode="index",
            )
            index_result = index_service.run()

        # Сравниваем пары (similar_book_id, score) с допуском по флоату
        self.assertEqual(
            [s.similar_book_id for s in brute_result],
            [s.similar_book_id for s in index_result],
        )
        for b, i in zip(brute_result, index_result):
            self.assertAlmostEqual(b.score, i.score, places=6)


if __name__ == "__main__":
    unittest.main()

