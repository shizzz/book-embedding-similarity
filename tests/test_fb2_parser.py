import unittest
from pathlib import Path
from lxml import etree
from app.parsers.book.fb2_parser import FB2BookParser
from app.infrastructure.models import ChunkType
from app.settings import ChunkingConfig

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

    def test_book_with_chords_at_end(self):
        fb2_path = DATA_DIR / "118674.fb2"
        self.assertTrue(fb2_path.is_file(), f"FB2 test file not found: {fb2_path}")

        with fb2_path.open("rb") as f:
            data = f.read()

        parser = FB2BookParser(filepath=str(fb2_path))
        book = parser.parse(data)

        # file_name should be set to the original filepath
        self.assertTrue(str(book.file_name).endswith("118674.fb2"))

        # Metadata from the real FB2 file
        self.assertEqual(book.title, "нрkqS J FъHI мZEIN uRзNлjbж")
        self.assertEqual(book.uid, "сlLfAKоB-VoVк-пncO-oJлb-lвьSцyUвrbEq")
        self.assertEqual(book.authors, ["pCLtпл GDьg hASдвmSъm"])
        self.assertEqual(book.author, "pCLtпл GDьg hASдвmSъm")

        # Chunks and text length should be populated
        self.assertIsNotNone(book.chunks)
        self.assertGreaterEqual(len(book.chunks), 2)
        types = {c.type for c in book.chunks}
        self.assertIn(ChunkType.TITLE, types)
        self.assertIn(ChunkType.DESCRIPTION, types)
        self.assertIsNotNone(book.text_length)
        self.assertGreater(book.text_length, 0)

        for chunk in book.chunks:
          if chunk.type == ChunkType.TEXT:
            self.assertGreater(chunk.length, 1000)

    def test_empty_book(self):
        fb2_path = DATA_DIR / "144825.fb2"
        self.assertTrue(fb2_path.is_file(), f"FB2 test file not found: {fb2_path}")

        with fb2_path.open("rb") as f:
            data = f.read()

        parser = FB2BookParser(filepath=str(fb2_path))
        book = parser.parse(data)

        # file_name should be set to the original filepath
        self.assertTrue(str(book.file_name).endswith("144825.fb2"))

        # Metadata from the real FB2 file
        self.assertEqual(book.title, "чzплrёpfсэбeдBPяcйeхYмсф")
        self.assertEqual(book.uid, "CotoфыгZ-цrtм-щоеU-SдSq-zzLюnфTLnFVi")
        self.assertEqual(book.authors, ["ъvэяnяъpMecwfMKqaztEyзOZqcсyjyъqтссK"])
        self.assertEqual(book.author, "ъvэяnяъpMecwfMKqaztEyзOZqcсyjyъqтссK")

        # Chunks and text length should be populated
        self.assertIsNotNone(book.chunks)
        self.assertEqual(len(book.chunks), 0)
        self.assertEqual(book.empty, True)
            
    def test_poem(self):
        fb2_path = DATA_DIR / "75252.fb2"
        self.assertTrue(fb2_path.is_file(), f"FB2 test file not found: {fb2_path}")

        with fb2_path.open("rb") as f:
            data = f.read()

        parser = FB2BookParser(filepath=str(fb2_path))
        book = parser.parse(data)

        # file_name should be set to the original filepath
        self.assertTrue(str(book.file_name).endswith("75252.fb2"))

        # Metadata from the real FB2 file
        self.assertEqual(book.title, "lвыщvщаDP")
        self.assertEqual(book.uid, "mуцfэEkm-PnэQ-mкKд-pмкU-EфclщZyцLhцA")
        self.assertEqual(book.authors, ["YEлхтSsD nlbGэuWRм эJhп"])
        self.assertEqual(book.author, "YEлхтSsD nlbGэuWRм эJhп")

        # Chunks and text length should be populated
        self.assertIsNotNone(book.chunks)
        self.assertGreaterEqual(len(book.chunks), 3)
        types = {c.type for c in book.chunks}
        self.assertIn(ChunkType.TITLE, types)
        self.assertIsNotNone(book.text_length)
        self.assertGreater(book.text_length, 0)

        for chunk in book.chunks:
          if chunk.type == ChunkType.TEXT:
            self.assertGreater(chunk.length, 1000)
            
    def test_normal_book(self):
        fb2_path = DATA_DIR / "1132.fb2"
        self.assertTrue(fb2_path.is_file(), f"FB2 test file not found: {fb2_path}")

        with fb2_path.open("rb") as f:
            data = f.read()

        parser = FB2BookParser(filepath=str(fb2_path))
        book = parser.parse(data)

        # file_name should be set to the original filepath
        self.assertTrue(str(book.file_name).endswith("1132.fb2"))

        # Metadata from the real FB2 file
        self.assertEqual(book.title, "PhиоьnRTGZ Qта")
        self.assertEqual(book.uid, "KHчъяslT-GхCq-рGjc-Hнjз-VтMяrбdшvсаъ")
        self.assertEqual(book.authors, ["PцmэkDk drьCж"])
        self.assertEqual(book.author, "PцmэkDk drьCж")

        # Chunks and text length should be populated
        self.assertIsNotNone(book.chunks)
        self.assertGreaterEqual(len(book.chunks), 2 + ChunkingConfig.CHUNKS_PER_BOOK)
        types = {c.type for c in book.chunks}
        self.assertIn(ChunkType.TITLE, types)
        self.assertIn(ChunkType.DESCRIPTION, types)
        self.assertIsNotNone(book.text_length)
        self.assertGreater(book.text_length, 0)

        total_length  = 0
        target_chunk_length = int(ChunkingConfig.ST_TARGET_CHARS * 0.95 / ChunkingConfig.CHUNKS_PER_BOOK)
        for chunk in book.chunks:
          if chunk.type == ChunkType.TEXT:
            total_length  += chunk.length
            self.assertGreater(chunk.length, target_chunk_length)
        self.assertGreater(total_length, ChunkingConfig.ST_TARGET_CHARS * 0.95)
            
if __name__ == "__main__":
    unittest.main()

