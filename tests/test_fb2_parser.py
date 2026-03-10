import unittest
from pathlib import Path

from lxml import etree

from app.parsers.book.fb2_parser import FB2BookParser
from app.infrastructure.models import ChunkType


DATA_DIR = Path(__file__).parent / "data"


class TestFB2BookParser(unittest.TestCase):
    def _build_root(self, xml: bytes):
        parser = etree.XMLParser(recover=True, huge_tree=True, no_network=True)
        return etree.fromstring(xml, parser)

    def test_extract_paragraphs_includes_body_and_poem_lines(self):
        xml = b"""<?xml version="1.0" encoding="utf-8"?>
        <FictionBook xmlns="http://www.gribuser.ru/xml/fictionbook/2.0">
          <body>
            <section>
              <p>  First   paragraph  </p>
              <p>Second paragraph</p>
              <poem>
                <v> Line one </v>
                <v> Line two</v>
              </poem>
            </section>
          </body>
        </FictionBook>
        """
        root = self._build_root(xml)
        parser = FB2BookParser(filepath="dummy.fb2")

        paragraphs = parser._extract_paragraphs(root)

        self.assertEqual(
            paragraphs,
            [
                "First paragraph",
                "Second paragraph",
                "Line one",
                "Line two",
            ],
        )

    def test_get_description_chunk_truncates_and_sets_type(self):
        long_text = " ".join(["desc"] * 50)
        xml = f"""<?xml version="1.0" encoding="utf-8"?>
        <FictionBook xmlns="http://www.gribuser.ru/xml/fictionbook/2.0">
          <description>
            <title-info>
              <annotation>
                <p>{long_text}</p>
              </annotation>
            </title-info>
          </description>
        </FictionBook>
        """.encode("utf-8")

        root = self._build_root(xml)
        parser = FB2BookParser(filepath="dummy.fb2")

        max_chars = 40
        chunk = parser._get_description_chunk(root, max_description_chars=max_chars)

        self.assertIsNotNone(chunk)
        self.assertEqual(chunk.type, ChunkType.DESCRIPTION)
        self.assertLessEqual(len(chunk.text), max_chars)
        self.assertIn("desc", chunk.text)

    def test_parse_populates_book_metadata_and_chunks(self):
        xml = b"""<?xml version="1.0" encoding="utf-8"?>
        <FictionBook xmlns="http://www.gribuser.ru/xml/fictionbook/2.0">
          <description>
            <title-info>
              <book-title>  Sample  Title  </book-title>
              <author>
                <first-name>John</first-name>
                <last-name>Doe</last-name>
              </author>
              <annotation>
                <p>First line of description.</p>
                <p>Second line of description.</p>
              </annotation>
            </title-info>
            <document-info>
              <id>BOOK-ID-123</id>
            </document-info>
          </description>
          <body>
            <section>
              <p>Paragraph one text.</p>
              <p>Paragraph two text.</p>
              <poem>
                <v>Poem line one.</v>
                <v>Poem line two.</v>
              </poem>
            </section>
          </body>
        </FictionBook>
        """

        parser = FB2BookParser(filepath="sample.fb2")
        book = parser.parse(xml)

        # Basic metadata
        self.assertEqual(book.file_name, "sample.fb2")
        self.assertEqual(book.title, "Sample Title")
        self.assertEqual(book.uid, "BOOK-ID-123")
        self.assertEqual(book.authors, ["John Doe"])
        self.assertEqual(book.author, "John Doe")

        # Chunks and text length
        self.assertIsNotNone(book.chunks)
        self.assertGreaterEqual(len(book.chunks), 2)
        types = [c.type for c in book.chunks]
        self.assertIn(ChunkType.TITLE, types)
        self.assertIn(ChunkType.DESCRIPTION, types)
        self.assertIsNotNone(book.text_length)
        self.assertGreater(book.text_length, 0)

    def test_parse_real_fb2_file_from_disk(self):
        fb2_path = DATA_DIR / "sample_simple.fb2"
        self.assertTrue(fb2_path.is_file(), f"FB2 test file not found: {fb2_path}")

        with fb2_path.open("rb") as f:
            data = f.read()

        parser = FB2BookParser(filepath=str(fb2_path))
        book = parser.parse(data)

        # file_name should be set to the original filepath
        self.assertTrue(str(book.file_name).endswith("sample_simple.fb2"))

        # Metadata from the real FB2 file
        self.assertEqual(book.title, "Disk Sample Title")
        self.assertEqual(book.uid, "DISK-BOOK-ID-1")
        self.assertEqual(book.authors, ["Alice Smith"])
        self.assertEqual(book.author, "Alice Smith")

        # Chunks and text length should be populated
        self.assertIsNotNone(book.chunks)
        self.assertGreaterEqual(len(book.chunks), 2)
        types = {c.type for c in book.chunks}
        self.assertIn(ChunkType.TITLE, types)
        self.assertIn(ChunkType.DESCRIPTION, types)
        self.assertIsNotNone(book.text_length)
        self.assertGreater(book.text_length, 0)


if __name__ == "__main__":
    unittest.main()

