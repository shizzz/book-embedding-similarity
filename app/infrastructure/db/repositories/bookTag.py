from typing import List
from app.infrastructure.db import DBRouter
from app.infrastructure.models.tag import BookTag

class BookTagsRepository:
    GENRES_TABLE: str = "book_genres"
    CENTOIDS_TABLE: str = "book_cenroids"

    def __init__(
            self, 
            router: DBRouter, 
            table: str = "book_genres"
        ):
        if table not in (BookTagsRepository.GENRES_TABLE, BookTagsRepository.CENTOIDS_TABLE):
            raise ValueError(f"Unknown table: {table}")
        
        self.router = router
        self.table = table

    # ------------------------
    # CREATE / INSERT
    # ------------------------
    def create_many(self, tags: List[BookTag]) -> None:
        """
        Вставка множества тегов в одну из таблиц: book_genres или book_cenroids
        """
        with self.router.meta() as conn:
            conn.executemany(
                f"""
                INSERT OR REPLACE INTO {self.table}(book_id, genre_id, model_id, distance)
                VALUES (?, ?, ?, ?)
                """,
                [t.to_tuple() for t in tags]
            )

    # ------------------------
    # READ
    # ------------------------
    def get_by_book(self, book_id: int) -> List[BookTag]:
        """
        Получение всех тегов (BookTag) для конкретной книги.
        """
        with self.router.meta() as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"""
                SELECT id, book_id, genre_id, model_id, distance
                FROM {self.table}
                WHERE book_id = ?
                """,
                (book_id,)
            )

            rows = cursor.fetchall()
            return [BookTag.from_row(row) for row in rows]

    # ------------------------
    # DELETE
    # ------------------------
    def delete_by_model(self, model_id: int) -> None:
        """
        Удаление всех тегов для модели
        """
        with self.router.meta() as conn:
            conn.execute(f"DELETE FROM {self.table} WHERE model_id = ?", (model_id,))