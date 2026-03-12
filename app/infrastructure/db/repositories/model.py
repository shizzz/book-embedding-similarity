from ..router import DBRouter

class ModelRepository:
    def __init__(self, router: DBRouter):
        self.router = router

    def get_or_create(self, uid: str, name: str) -> int:
        with self.router.meta() as conn:
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT id
                FROM models
                WHERE uid = ?
                """,
                (uid,)
            )
            row = cursor.fetchone()

            if row is not None:
                return row[0]
            
            cursor.execute("UPDATE models SET active = 0")
            
            cursor.execute(
                """
                INSERT INTO models (uid, name, active)
                VALUES (?, ?, 1)
                """,
                (uid, name)
            )
            
            return cursor.lastrowid
        
    def get_latest_uid(self, name: str) -> str:
        """
        Возвращает последний uid модели по имени, сортируя по дате создания.
        """
        with self.router.meta() as conn:
            cursor = conn.cursor()
            
            # Try to get the first active model
            cursor.execute(
                """
                SELECT uid
                FROM models
                WHERE name = ? AND active = 1
                ORDER BY uid
                LIMIT 1
                """,
                (name,)
            )
            row = cursor.fetchone()
            if row:
                return row[0]
            
            # If no active model, return the first one regardless of active status
            cursor.execute(
                """
                SELECT uid
                FROM models
                WHERE active = 1
                ORDER BY uid
                LIMIT 1
                """,
            )
            row = cursor.fetchone()
            return row[0] if row else None