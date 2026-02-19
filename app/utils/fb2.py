from lxml import etree
import re
from typing import List, Optional
from app.models import Book
from app.settings.config import ST_MIN_CHARS, ST_TARGET_CHARS, ST_MAX_TITLE_CHARS, ST_MAX_DESCRIPTION_CHARS

class FB2Book:
    NS = {"fb2": "http://www.gribuser.ru/xml/fictionbook/2.0"}

    def __init__(self, fb2_bytes: bytes):
        parser = etree.XMLParser(
            recover=True,
            huge_tree=True,
            no_network=True
        )
        self.root = etree.fromstring(fb2_bytes, parser)

    def enrich_book(self, book: Book):
        enrichers = (
            ("uid", self.get_id),
            ("title", self.get_title),
            ("authors", self.get_authors),
            ("author", lambda: ", ".join(book.authors)),
            ("text", self.extract_text)
        )

        for attr, getter in enrichers:
            if not getattr(book, attr):
                setattr(book, attr, getter())

        book.source_length = len(book.data) if book.data else 0
        book.token_length = len(book.text) if book.text else 0

    # =====================
    # TEXT
    # =====================
    def extract_text(
        self,
        target_chars: int = ST_TARGET_CHARS,
        min_chars: int = ST_MIN_CHARS,
        max_title_chars: int = ST_MAX_TITLE_CHARS,
        max_description_chars: int = ST_MAX_DESCRIPTION_CHARS,
    ) -> str:
        """
        Возвращает текст книги для embedding.

        Структура:
            [TITLE]
            title

            [DESCRIPTION]
            description

            [BODY]
            sampled body text (target_chars)
        """

        # -------- extract paragraphs safely --------
        nodes = self.root.xpath(
            ".//fb2:body//fb2:p[not(ancestor::fb2:annotation) and not(ancestor::fb2:note)]",
            namespaces=self.NS
        )

        paragraphs = []
        for node in nodes:
            text = "".join(node.xpath(".//text()"))
            text = re.sub(r"\s+", " ", text).strip()
            if text:
                paragraphs.append(text)

        if not paragraphs:
            paragraphs = []

        total_chars = sum(len(p) + 2 for p in paragraphs)

        # -------- prepare title / description --------
        parts = []

        title = self.get_title()
        if title:
            title = re.sub(r"\s+", " ", title).strip()[:max_title_chars]
            parts.append(f"[TITLE]\n{title}")

        description = self.get_description()
        if description:
            description = re.sub(r"\s+", " ", description).strip()[:max_description_chars]
            parts.append(f"[DESCRIPTION]\n{description}")

        # -------- short book fallback --------
        if total_chars <= target_chars:
            body_text = "\n\n".join(paragraphs)

            parts = []

            if title:
                parts.append(f"[TITLE]\n{title}")

            if description:
                parts.append(f"[DESCRIPTION]\n{description}")

            parts.append(f"[BODY]\n{body_text}")

            return "\n\n".join(parts)

        # -------- collect body --------
        total = len(paragraphs)
        used = set()
        used_add = used.add
        used_contains = used.__contains__
        body_parts = []

        remaining = target_chars

        def try_add(idx: int) -> bool:
            """Try to add paragraph without exceeding budget."""
            nonlocal remaining

            if idx in used:
                return False

            p = paragraphs[idx]
            size = len(p) + 2

            if size > remaining:
                return False

            if used_contains(idx):
                return False

            used_add(idx)
            body_parts.append(p)
            remaining -= size

            return True

        def collect_forward(start, budget):
            length = 0
            i = start

            while i < total and length < budget and remaining > 0:
                if try_add(i):
                    length += len(paragraphs[i]) + 2
                i += 1

        def collect_backward(start, budget):
            length = 0
            i = start

            while i >= 0 and length < budget and remaining > 0:
                if try_add(i):
                    length += len(paragraphs[i]) + 2
                i -= 1

        if total > 0 and remaining > 0:

            per_section = target_chars // 3

            # start
            collect_forward(0, per_section)

            # middle (balanced)
            mid = total // 2
            collect_forward(mid, per_section // 2)
            collect_backward(mid - 1, per_section // 2)

            # end
            collect_backward(total - 1, per_section)

        # -------- fallback if too small --------
        body_text = "\n\n".join(body_parts)

        if len(body_text) < min_chars:

            for i in range(total):
                if remaining <= 0:
                    break

                if try_add(i):
                    body_text = "\n\n".join(body_parts)

                if len(body_text) >= min_chars:
                    break

        # -------- hard safety trim --------
        body_text = body_text[:target_chars].strip()

        # -------- assemble final text --------
        if body_text:
            parts.append(f"[BODY]\n{body_text}")

        return "\n\n".join(parts).strip()

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

    def get_description(self) -> Optional[str]:
        """
        Возвращает описание/аннотацию книги (если есть)
        """
        desc_nodes = self.root.xpath(
            ".//fb2:title-info/fb2:annotation",
            namespaces=self.NS
        )
        if not desc_nodes:
            return None

        # берём текст внутри <annotation>, объединяя параграфы
        paragraphs = []
        for node in desc_nodes:
            ps = node.xpath(".//fb2:p/text()", namespaces=self.NS)
            paragraphs.extend([p.strip() for p in ps if p.strip()])
        return "\n".join(paragraphs) if paragraphs else None
    
    def get_id(self) -> Optional[str]:
        book_id = self.root.xpath(
            "string(.//fb2:document-info/fb2:id)",
            namespaces=self.NS
        )
        return book_id.strip() or None
