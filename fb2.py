from lxml import etree
import zipfile
from typing import List, Optional
from settings import BOOK_FOLDER

def get_file_bytes_from_zip(book) -> bytes:
    zip_path = f"{BOOK_FOLDER}/{book.archive_name}"

    with zipfile.ZipFile(zip_path, "r") as archive:
        with archive.open(book.file_name) as f:
            return f.read()

class FB2Book:
    NS = {"fb2": "http://www.gribuser.ru/xml/fictionbook/2.0"}

    def __init__(self, fb2_bytes: bytes):
        parser = etree.XMLParser(
            recover=True,
            huge_tree=True,
            no_network=True
        )
        self.root = etree.fromstring(fb2_bytes, parser)

    # =====================
    # TEXT
    # =====================
    def extract_text(self) -> str:
        paragraphs = self.root.xpath(
            ".//fb2:body//fb2:p/text()",
            namespaces=self.NS
        )
        return "\n".join(p.strip() for p in paragraphs if p.strip())

    # =====================
    # METADATA
    # =====================
    def get_title(self) -> Optional[str]:
        title = self.root.xpath(
            "string(.//fb2:book-title)",
            namespaces=self.NS
        )
        return title.strip() or None

    def get_authors(self) -> List[str]:
        authors = []

        author_nodes = self.root.xpath(
            ".//fb2:title-info/fb2:author",
            namespaces=self.NS
        )

        for a in author_nodes:
            parts = [
                a.findtext("fb2:first-name", namespaces=self.NS),
                a.findtext("fb2:middle-name", namespaces=self.NS),
                a.findtext("fb2:last-name", namespaces=self.NS),
            ]
            name = " ".join(p for p in parts if p)
            if name:
                authors.append(name)

        return authors

    def get_id(self) -> Optional[str]:
        book_id = self.root.xpath(
            "string(.//fb2:document-info/fb2:id)",
            namespaces=self.NS
        )
        return book_id.strip() or None
