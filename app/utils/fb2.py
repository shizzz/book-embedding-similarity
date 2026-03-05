from lxml import etree
import re
from math import ceil
from typing import List, Optional
from app.infrastructure.models import Book, Chunk, Type
from app.settings.config import (
    ST_MIN_CHARS, 
    ST_TARGET_CHARS, 
    ST_MAX_DESCRIPTION_CHARS, 
    CHUNKS_PER_BOOK,
    PREFIX_BUFFER,
    SECTIONS_RATIO
)

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
            raw_chunks, text_length = self.extract_chunks()
            book.text_length = text_length
            for chunk in raw_chunks:
                chunk.book_id = book.id

            book.chunks = raw_chunks

    # =====================
    # TEXT
    # =====================
    def _extract_paragraphs(self) -> list[str]:
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
        return paragraphs

    def _get_description_chunk(self, max_description_chars: int) -> Chunk|None:
        description = self.get_description()
        if not description:
            return None
        description = re.sub(r"\s+", " ", description).strip()
        if len(description) > max_description_chars:
            cutoff = description.rfind(" ", 0, max_description_chars)
            if cutoff == -1:
                cutoff = max_description_chars
            description = description[:cutoff].strip()
        if description:
            return Chunk(text=description, type=Type.DESCRIPTION)
        return None

    def _compute_chunks_targets(self, paragraphs: list[str], desired_chars: int, sections: int) -> tuple[int,list[int]]:
        chunk_size = max(ST_MIN_CHARS, desired_chars // sections)
        num_chunks = min(sections, ceil(desired_chars / (chunk_size * 1.1)))
        num_chunks = min(num_chunks, len(paragraphs))
        chunk_targets = [int(i * len(paragraphs) / num_chunks) for i in range(num_chunks)]
        return chunk_size, chunk_targets

    def _build_chunk_around_target(
            self,
            paragraphs: list[str],
            target_idx: int,
            used: set,
            chunk_size: int,
            prefix_buffer: int
        ) -> Chunk|None:
        total_paragraphs = len(paragraphs)
        left = right = target_idx
        current_chunk = []
        current_len = 0

        while current_len < chunk_size and (left >= 0 or right < total_paragraphs):
            added = False
            # левый параграф
            if left >= 0 and left not in used:
                p = paragraphs[left]
                space_left = chunk_size - current_len - prefix_buffer
                if len(p) > space_left:
                    cutoff = p.rfind(" ", 0, space_left)
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
            if right < total_paragraphs and right not in used:
                p = paragraphs[right]
                space_left = chunk_size - current_len - prefix_buffer
                if len(p) > space_left:
                    cutoff = p.rfind(" ", 0, space_left)
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

        chunk_text = "\n\n".join(current_chunk).strip()
        if len(chunk_text) >= ST_MIN_CHARS:
            return Chunk(text=chunk_text, type=Type.TEXT)
        return None

    def extract_chunks(
        self,
        target_chars: int = ST_TARGET_CHARS,
        min_chars: int = ST_MIN_CHARS,
        max_description_chars: int = ST_MAX_DESCRIPTION_CHARS,
        sections: int = CHUNKS_PER_BOOK,
        prefix_buffer: int = PREFIX_BUFFER,
        sections_ratio: float = SECTIONS_RATIO,
    ) -> tuple[list[Chunk], int]:
        paragraphs = self._extract_paragraphs()
        if not paragraphs:
            return []

        chunks: list[Chunk] = []

        title_chunk = self.get_title()
        if title_chunk:
            chunks.append(Chunk(text=title_chunk, type=Type.TITLE))
        desc_chunk = self._get_description_chunk(max_description_chars)
        if desc_chunk:
            chunks.append(desc_chunk)

        total_chars = sum(len(p)+2 for p in paragraphs)
        desired_chars = min(int(total_chars * sections_ratio), target_chars)

        chunk_size, chunk_targets = self._compute_chunks_targets(paragraphs, desired_chars, sections)
        used = set()

        for idx in chunk_targets:
            chunk = self._build_chunk_around_target(paragraphs, idx, used, chunk_size, prefix_buffer)
            if chunk:
                chunks.append(chunk)

        # fallback для оставшихся параграфов
        combined_len = sum(c.length for c in chunks if c.type==Type.TEXT)
        i = 0
        while combined_len < min_chars and i < len(paragraphs):
            if i not in used:
                chunks.append(Chunk(text=paragraphs[i],type=Type.TEXT))
                combined_len += len(paragraphs[i]) + 2
                used.add(i)
            i += 1

        return chunks, total_chars

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
