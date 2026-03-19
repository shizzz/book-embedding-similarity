from typing import List, Optional, Generator
from app.infrastructure.db import DBRouter
from app.infrastructure.models.tag import Tag

class GenresRepository:
    def __init__(self, router: DBRouter):
        self.router = router

    # ------------------------
    # READ
    # ------------------------
    def get_all(self, type) -> Generator[Tag, None, None]:
        with self.router.meta() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT id, parent_id, name_ru, name_en, NULL as data, 4 as type
                FROM genres
                ORDER BY id
                """
            )

            while True:
                row = cursor.fetchone()
                if not row:
                    break

                yield Tag.from_row(row)

    def get_by_id(self, tag_id: int) -> Optional[Tag]:
        with self.router.meta() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT id, parent_id, name_ru, name_en, NULL as data, 4 as type
                FROM genres
                WHERE id = ?
                """,
                (tag_id,)
            )

            row = cursor.fetchone()
            return Tag.from_row(row) if row else None

    def get_children(self, parent_id: int) -> List[Tag]:
        with self.router.meta() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT id, parent_id, name_ru, name_en, NULL as data, 4 as type
                FROM genres
                WHERE parent_id = ?
                """,
                (parent_id,)
            )

            return [Tag.from_row(row) for row in cursor.fetchall()]

    # ------------------------
    # CREATE
    # ------------------------
    def create(self, tag: Tag) -> int:
        with self.router.meta() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO genres(parent_id, name_ru, name_en)
                VALUES (?, ?, ?)
                """,
                tag.to_tuple()
            )
            return cursor.lastrowid

    def create_many(self, tags: List[Tag]) -> None:
        with self.router.meta() as conn:
            conn.executemany(
                """
                INSERT INTO genres(parent_id, name_ru, name_en)
                VALUES (?, ?, ?)
                """,
                [t.to_tuple() for t in tags]
            )

    # ------------------------
    # UPDATE
    # ------------------------
    def update(self, tag: Tag) -> None:
        if tag.id is None:
            raise ValueError("Tag id is required for update")

        with self.router.meta() as conn:
            conn.execute(
                """
                UPDATE genres
                SET parent_id = ?, name_ru = ?, name_en = ?
                WHERE id = ?
                """,
                (
                    tag.parent_id,
                    tag.name_ru,
                    tag.name_en,
                    tag.id
                )
            )

    # ------------------------
    # DELETE
    # ------------------------
    def delete(self, tag_id: int) -> None:
        with self.router.meta() as conn:
            conn.execute(
                "DELETE FROM genres WHERE id = ?",
                (tag_id,)
            )

    # ------------------------
    # COUNT
    # ------------------------
    def count(self) -> int:
        with self.router.meta() as conn:
            return conn.execute("SELECT COUNT(*) FROM genres").fetchone()[0]