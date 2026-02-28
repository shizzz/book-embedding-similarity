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
            
            cursor.execute(
                """
                INSERT INTO models (uid, name)
                VALUES (?, ?)
                """,
                (uid, name)
            )
            
            return cursor.lastrowid