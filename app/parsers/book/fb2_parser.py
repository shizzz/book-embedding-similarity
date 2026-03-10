import re
from lxml import etree
from math import ceil
from typing import List, Tuple, Optional
from app.infrastructure.models import Book, Chunk, ChunkType
from app.settings import ChunkingConfig
from .book_parser import BookParser

class FB2BookParser(BookParser):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._ns = {"fb2": "http://www.gribuser.ru/xml/fictionbook/2.0"}

    def parse(self, data: bytes) -> Tuple[Book, List[Chunk]]:
        book = Book(file_name=self.filepath)

        parser = etree.XMLParser(
            recover=True,
            huge_tree=True,
            no_network=True
        )
        root = etree.fromstring(data, parser)

        return self.enrich_book(book, root)

    def enrich_book(self, book: Book, root: any) -> Tuple[Book, List[Chunk]]:
        chunks: List[Chunk] = []
        enrichers = (
            ("uid", self.get_id),
            ("title", self.get_title),
            ("authors", self.get_authors)
        )

        for attr, getter in enrichers:
            if not getattr(book, attr):
                setattr(book, attr, getter(root))

        book.author = ", ".join(book.authors)

        # --------- создаем chunks ---------
        if not getattr(book, "chunks", None):
            chunks, text_length = self.extract_chunks(root)
            book.text_length = text_length
            if not any(chunk.type == ChunkType.TEXT for chunk in chunks):
                book.empty = True
            else:
                for chunk in chunks:
                    chunk.book_id = book.id

        return book, chunks

    # =====================
    # TEXT
    # =====================
    def _extract_paragraphs(self, root) -> list[str]:
        nodes = root.xpath(
            ".//fb2:body[not(@name='notes')]//fb2:p | .//fb2:body[not(@name='notes')]//fb2:poem//fb2:v",
            namespaces=self._ns
        )
        paragraphs = []
        for node in nodes:
            text = "".join(node.xpath(".//text()"))
            text = re.sub(r"\s+", " ", text).strip()
            if text:
                paragraphs.append(text)
        return paragraphs

    def _get_description_chunk(self, root, max_description_chars: int) -> Chunk|None:
        description = self.get_description(root)
        if not description:
            return None
        description = re.sub(r"\s+", " ", description).strip()
        if len(description) > max_description_chars:
            cutoff = description.rfind(" ", 0, max_description_chars)
            if cutoff == -1:
                cutoff = max_description_chars
            description = description[:cutoff].strip()
        if description:
            return Chunk(text=description, type=ChunkType.DESCRIPTION)
        return None

    def _compute_chunks_targets(
            self, paragraphs: list[str], 
            sections_ratio: float, 
            total_chars: int, 
            target_chars: int, 
            sections: int
        ) -> tuple[int,list[int]]:
        desired_chunk_size = target_chars // sections
        desired_chars = min(int(total_chars * sections_ratio), target_chars)
        num_chunks = min(sections, ceil(desired_chars / (desired_chunk_size * 1.1)))
        chunk_size = min(desired_chunk_size, total_chars // num_chunks)
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
        right += 1
        current_chunk = []
        current_len = 0

        while current_len < chunk_size and (left >= 0 or right < total_paragraphs):
            added = False
            # левый параграф
            if left >= 0 and left not in used:
                p = paragraphs[left]
                space_left = chunk_size - current_len - prefix_buffer
                if space_left <= 0:
                    break
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
                if space_left <= 0:
                    break
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
        if len(chunk_text) >= ChunkingConfig.ST_MIN_CHARS:
            return Chunk(text=chunk_text, type=ChunkType.TEXT)
        return None

    def extract_chunks(
        self,
        root: any,
        target_chars: int = ChunkingConfig.ST_TARGET_CHARS,
        min_chars: int = ChunkingConfig.ST_MIN_CHARS,
        max_description_chars: int = ChunkingConfig.ST_MAX_DESCRIPTION_CHARS,
        sections: int = ChunkingConfig.CHUNKS_PER_BOOK,
        prefix_buffer: int = ChunkingConfig.PREFIX_BUFFER,
        sections_ratio: float = ChunkingConfig.SECTIONS_RATIO,
    ) -> tuple[list[Chunk], int]:
        paragraphs = self._extract_paragraphs(root)
        if not paragraphs:
            return [], 0

        chunks: list[Chunk] = []

        title_chunk = self.get_title(root)
        if title_chunk:
            chunks.append(Chunk(text=title_chunk, type=ChunkType.TITLE))
        desc_chunk = self._get_description_chunk(root, max_description_chars)
        if desc_chunk:
            chunks.append(desc_chunk)

        total_chars = sum(len(p)+2 for p in paragraphs)

        chunk_size, chunk_targets = self._compute_chunks_targets(paragraphs, sections_ratio, total_chars, target_chars, sections)
        used = set()

        for idx in chunk_targets:
            chunk = self._build_chunk_around_target(paragraphs, idx, used, chunk_size, prefix_buffer)
            if chunk:
                if len(chunk.text) < chunk_size * 0.5 and chunks:
                    # attach to previous
                    prev = chunks[-1]
                    if prev.type == ChunkType.TEXT:
                        prev.text = prev.text.rstrip() + "\n\n" + chunk.text
                if len(chunk.text) < min_chars:
                    continue
                else:
                    chunks.append(chunk)

        # Fallback
        text_chunk_total = sum(1 for ch in chunks if ch.type == ChunkType.TEXT)

        if text_chunk_total == 0 and paragraphs:
            all_text = "\n\n".join(paragraphs)

            if len(all_text) >= min_chars:  
                if len(all_text) > target_chars:
                    all_text = all_text[:target_chars]

                chunks.append(
                    Chunk(
                        text=all_text,
                        type=ChunkType.TEXT
                    )
                )

        return chunks, total_chars

    # =====================
    # METADATA
    # =====================
    def get_title(self, root) -> Optional[str]:
        title = root.xpath(
            "string(.//fb2:book-title)",
            namespaces=self._ns
        )
        return title.strip() or None

    def get_authors(self, root) -> List[str]:
        authors = []

        author_nodes = root.xpath(
            ".//fb2:title-info/fb2:author",
            namespaces=self._ns
        )

        for a in author_nodes:
            parts = [
                a.findtext("fb2:first-name", namespaces=self._ns),
                a.findtext("fb2:middle-name", namespaces=self._ns),
                a.findtext("fb2:last-name", namespaces=self._ns),
            ]
            name = " ".join(p for p in parts if p)
            if name:
                authors.append(name)

        return authors

    def get_description(self, root) -> Optional[str]:
        """
        Возвращает описание/аннотацию книги (если есть)
        """
        desc_nodes = root.xpath(
            ".//fb2:title-info/fb2:annotation",
            namespaces=self._ns
        )
        if not desc_nodes:
            return None

        # берём текст внутри <annotation>, объединяя параграфы
        paragraphs = []
        for node in desc_nodes:
            ps = node.xpath(".//fb2:p/text()", namespaces=self._ns)
            paragraphs.extend([p.strip() for p in ps if p.strip()])
        return "\n".join(paragraphs) if paragraphs else None
    
    def get_id(self, root) -> Optional[str]:
        book_id = root.xpath(
            "string(.//fb2:document-info/fb2:id)",
            namespaces=self._ns
        )
        return book_id.strip() or None
