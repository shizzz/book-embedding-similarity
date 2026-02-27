class ModelRepository:
    @staticmethod
    def get_or_create(conn, uid: str, name: str) -> int:
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