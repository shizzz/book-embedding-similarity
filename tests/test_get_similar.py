import asyncio
import unittest
from contextlib import contextmanager, redirect_stdout
from io import StringIO
from unittest.mock import patch

from app.models import Book
from app import get_similar


class TestGetSimilarHelpers(unittest.TestCase):
    def test_make_lib_url_strips_fb2_suffix(self):
        with patch("app.get_similar.LIB_URL", "http://lib"):
            url = get_similar.make_lib_url("book.fb2")
        self.assertEqual(
            url,
            "http://lib/#/extended?page=1&limit=20&ex_file=book",
        )

    def test_print_similar_books_outputs_expected_lines(self):
        # Prepare a fake similar result
        class Candidate:
            file_name = "c1.fb2"
            title = "Candidate 1"

        class SimilarStub:
            score = 0.42  # 42%
            candidate = Candidate()

        out = StringIO()

        with patch("app.get_similar.LIB_URL", "http://lib"), patch(
            "time.perf_counter", return_value=100.0
        ):
            with redirect_stdout(out):
                get_similar.print_similar_books(
                    source_file="src.fb2",
                    similars=[SimilarStub()],
                    started_at=90.0,
                )

        output = out.getvalue()

        # Header and basic info
        self.assertIn("Топ-50 похожих книг для src.fb2", output)
        # Book row with percentage, file name, title and URL
        self.assertIn("42.00", output)
        self.assertIn("c1.fb2", output)
        self.assertIn("Candidate 1", output)
        self.assertIn("http://lib/#/extended?page=1&limit=20&ex_file=c1", output)


class TestGetSimilarMain(unittest.TestCase):
    @staticmethod
    @contextmanager
    def _fake_db():
        yield object()

    def test_main_prints_message_when_book_not_found(self):
        class Args:
            file_name = "missing.fb2"

        out = StringIO()

        with patch("app.get_similar.db", self._fake_db), patch(
            "argparse.ArgumentParser.parse_args", return_value=Args()
        ), patch(
            "app.get_similar.BookRepository.get_by_file", return_value=None
        ), patch(
            "app.get_similar.EmbeddingsRepository.get"
        ) as mock_get_embedding, redirect_stdout(
            out
        ):
            asyncio.run(get_similar.main())

        mock_get_embedding.assert_not_called()
        output = out.getvalue()
        self.assertIn("Книга missing.fb2 не найдена в реестре", output)

    def test_main_prints_message_when_no_embedding(self):
        class Args:
            file_name = "src.fb2"

        book = Book(
            archive_name="a.zip",
            file_name="src.fb2",
            id=1,
            title="SRC",
        )

        out = StringIO()

        with patch("app.get_similar.db", self._fake_db), patch(
            "argparse.ArgumentParser.parse_args", return_value=Args()
        ), patch(
            "app.get_similar.BookRepository.get_by_file", return_value=book
        ), patch(
            "app.get_similar.EmbeddingsRepository.get", return_value=None
        ), patch(
            "app.get_similar.SimilarSearchService"
        ) as mock_service, redirect_stdout(
            out
        ):
            asyncio.run(get_similar.main())

        mock_service.assert_not_called()
        output = out.getvalue()
        self.assertIn("У книги src.fb2 не сгенерирован вектор", output)

    def test_main_happy_path_saves_similars_and_prints(self):
        class Args:
            file_name = "src.fb2"

        book = Book(
            archive_name="a.zip",
            file_name="src.fb2",
            id=1,
            title="SRC",
        )

        # Fake similar result returned by the service
        class Candidate:
            file_name = "c1.fb2"
            title = "Candidate 1"

        class SimilarStub:
            score = 0.5
            candidate = Candidate()

        out = StringIO()

        with patch("app.get_similar.db", self._fake_db), patch(
            "argparse.ArgumentParser.parse_args", return_value=Args()
        ), patch(
            "app.get_similar.BookRepository.get_by_file", return_value=book
        ), patch(
            "app.get_similar.EmbeddingsRepository.get", return_value=b"dummy"
        ), patch(
            "app.get_similar.Embedding.from_db", return_value=object()
        ) as mock_from_db, patch(
            "app.get_similar.SimilarSearchService"
        ) as mock_service_cls, patch(
            "app.get_similar.SimilarRepository.replace"
        ) as mock_replace, patch(
            "app.get_similar.LIB_URL", "http://lib"
        ), redirect_stdout(
            out
        ):
            mock_service_cls.return_value.run.return_value = [SimilarStub()]
            asyncio.run(get_similar.main())

        # Embedding should be constructed from bytes
        mock_from_db.assert_called_once_with(b"dummy")
        # Service should be instantiated and run called
        mock_service_cls.assert_called_once()
        mock_service_cls.return_value.run.assert_called_once()
        # Results should be written via repository
        mock_replace.assert_called_once()

        output = out.getvalue()
        self.assertIn("Топ-50 похожих книг для src.fb2", output)
        self.assertIn("c1.fb2", output)
        self.assertIn("Candidate 1", output)


if __name__ == "__main__":
    unittest.main()

