from lxml import etree
import re
from typing import List, Optional
from app.models import Book, Chunk
from app.settings.config import ST_MIN_CHARS, ST_TARGET_CHARS, ST_MAX_DESCRIPTION_CHARS

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
            ("author", lambda: ", ".join(book.authors))
        )

        for attr, getter in enrichers:
            if not getattr(book, attr):
                setattr(book, attr, getter())

        # --------- создаем chunks ---------
        if not getattr(book, "chunks", None):
            raw_chunks = self.extract_chunks()
            book.chunks = [
                Chunk(book_id=book.id, text=text) for text in raw_chunks
            ]

    # =====================
    # TEXT
    # =====================
    def extract_chunks(
        self,
        target_chars: int = ST_TARGET_CHARS,
        min_chars: int = ST_MIN_CHARS,
        max_description_chars: int = ST_MAX_DESCRIPTION_CHARS,
        sections: int = 7,
    ) -> list[str]:
        """
        Возвращает список текстовых чанков книги без разрыва слов.
        Адаптировано для коротких книг и стихотворений.
        """
        # -------- extract paragraphs safely --------
        nodes = self.root.xpath(
            ".//fb2:body//fb2:p | .//fb2:body//fb2:poem//fb2:v",
            namespaces=self.NS
        )

        paragraphs = []
        for node in nodes:
            text = "".join(node.xpath(".//text()"))
            text = re.sub(r"\s+", " ", text).strip()
            if text:
                paragraphs.append(text)

        if not paragraphs:
            return []

        chunks = []

        # -------- description --------
        description = self.get_description()
        if description:
            description = re.sub(r"\s+", " ", description).strip()
            if len(description) > max_description_chars:
                cutoff = description.rfind(" ", 0, max_description_chars)
                if cutoff == -1:
                    cutoff = max_description_chars
                description = description[:cutoff].strip()
            if description:
                chunks.append(description)

        # -------- total book length --------
        total_chars = sum(len(p) + 2 for p in paragraphs)

        # -------- short book fallback --------
        if total_chars <= target_chars * 1.5:
            # Объединяем все параграфы, можно разбить на несколько чанков по max длине
            current_chunk = []
            current_len = 0
            for p in paragraphs:
                if current_len + len(p) + 2 > target_chars:
                    if current_chunk:
                        chunks.append("\n\n".join(current_chunk))
                    current_chunk = [p]
                    current_len = len(p) + 2
                else:
                    current_chunk.append(p)
                    current_len += len(p) + 2
            if current_chunk:
                chunks.append("\n\n".join(current_chunk))
            return chunks

        # -------- multi-section sampling (для больших книг) --------
        total = len(paragraphs)
        sections = max(1, min(sections, total))
        chunk_targets = [int(i * total / sections) for i in range(sections)]
        used = set()
        body_parts = []

        for idx in chunk_targets:
            left = idx
            right = idx
            current_chunk = []
            current_len = 0
            max_chunk_len = target_chars // sections
            while current_len < max_chunk_len and (left >= 0 or right < total):
                added = False
                # левый параграф
                if left >= 0 and left not in used:
                    p = paragraphs[left]
                    if current_len + len(p) + 2 > max_chunk_len:
                        cutoff = p.rfind(" ", 0, max_chunk_len - current_len)
                        if cutoff > 0:
                            current_chunk.append(p[:cutoff])
                            current_len += cutoff + 2
                        else:
                            current_chunk.append(p)
                            current_len += len(p) + 2
                        used.add(left)
                        break
                    else:
                        current_chunk.append(p)
                        current_len += len(p) + 2
                        used.add(left)
                        left -= 1
                        added = True
                # правый параграф
                if right < total and right not in used:
                    p = paragraphs[right]
                    if current_len + len(p) + 2 > max_chunk_len:
                        cutoff = p.rfind(" ", 0, max_chunk_len - current_len)
                        if cutoff > 0:
                            current_chunk.append(p[:cutoff])
                            current_len += cutoff + 2
                        else:
                            current_chunk.append(p)
                            current_len += len(p) + 2
                        used.add(right)
                        break
                    else:
                        current_chunk.append(p)
                        current_len += len(p) + 2
                        used.add(right)
                        right += 1
                        added = True
                if not added:
                    break
            if current_chunk:
                body_parts.append("\n\n".join(current_chunk))

        # -------- fallback если body_parts слишком маленькие --------
        combined_len = sum(len(p) for p in body_parts)
        i = 0
        while combined_len < min_chars and i < total:
            if i not in used:
                body_parts.append(paragraphs[i])
                combined_len += len(paragraphs[i]) + 2
                used.add(i)
            i += 1

        chunks.extend(body_parts)
        return chunks

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
