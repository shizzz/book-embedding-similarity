from lxml import etree
from typing import List, Optional

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
    def extract_text(self, paragraphs_per_part: int = 5) -> str:
        """
        Возвращает текст книги для embedding:
        - Берёт несколько абзацев из начала, середины и конца
        - Пропускает сноски и оглавление
        """
        # 1. Берём все параграфы <p>
        paragraphs = self.root.xpath(
            ".//fb2:body//fb2:p[not(ancestor::fb2:annotation) and not(ancestor::fb2:note)]/text()",
            namespaces=self.NS
        )
        # очищаем пустые строки
        paragraphs = [p.strip() for p in paragraphs if p.strip()]

        if not paragraphs:
            return ""

        total = len(paragraphs)

        # 2. Выбираем части: начало, середина, конец
        parts = []

        title = self.get_title()
        if title:
            parts.append(title)

        authors = self.get_authors()
        if authors:
            parts.append(", ".join(authors))

        description = self.get_description()
        if description:
            parts.append(description)

        # Начало
        parts.extend(paragraphs[:paragraphs_per_part])

        # Середина
        if total > paragraphs_per_part * 2:
            mid_start = max(paragraphs_per_part, total // 2 - paragraphs_per_part // 2)
            parts.extend(paragraphs[mid_start:mid_start + paragraphs_per_part])

        # Конец (эпилог)
        parts.extend(paragraphs[-paragraphs_per_part:])

        # 3. Собираем текст
        return "\n\n".join(parts)

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
