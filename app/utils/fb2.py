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
        sections: int = 7,
    ) -> str:
        """
        Возвращает текст книги для embedding.

        Структура:
            title

            description

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

        total = len(paragraphs)

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
            if body_text:
                parts.append(f"[BODY]\n{body_text}")
            return "\n\n".join(parts).strip()

        # -------- collect body --------
        used = set()
        body_parts = []
        remaining = target_chars

        def try_add(idx: int) -> bool:
            nonlocal remaining

            if idx in used:
                return False

            p = paragraphs[idx]
            size = len(p) + 2

            if size > remaining:
                return False

            used.add(idx)
            body_parts.append(p)
            remaining -= size
            return True

        # -------- multi-section sampling --------
        if total > 0 and remaining > 0:

            sections = max(1, min(sections, total))
            per_section_budget = target_chars // sections

            for s in range(sections):

                if remaining <= 0:
                    break

                # центр секции
                center = int((s + 0.5) * total / sections)

                # расширяемся вокруг центра
                left = center
                right = center + 1

                local_used = 0

                while local_used < per_section_budget and remaining > 0:

                    added = False

                    if left >= 0:
                        if try_add(left):
                            local_used += len(paragraphs[left]) + 2
                            added = True
                        left -= 1

                    if right < total and local_used < per_section_budget:
                        if try_add(right):
                            local_used += len(paragraphs[right]) + 2
                            added = True
                        right += 1

                    if not added:
                        break

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

        # -------- hard trim --------
        body_text = body_text[:target_chars].strip()

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
