from typing import Any, List, Tuple

class SimilarRepository:
    GET_QUERY: str = """
            SELECT
                score, 
                book_id as source_id,
                similar_book_id as similar_book_id
            FROM similar
    """
    DELETE_QUERY: str = "DELETE FROM similar"

    @staticmethod
    def save(conn, similars: List[Tuple[float, int, int]]):
        cur = conn.cursor()
        cur.executemany(
            "INSERT INTO similar (book_id, similar_book_id, score) VALUES (?, ?, ?)",
            [
                (similar[1], similar[2], float(similar[0]))
                for similar in similars
            ]
        )

    @staticmethod
    def replace(conn, similars: List[Tuple[float, int, int]]):
        if len(similars) == 0:
            return
        
        SimilarRepository.delete_many(conn, similars)
        SimilarRepository.save(conn, similars)

    @staticmethod
    def get(conn, book_id: int, limit: int) -> List[Tuple[float, int, int]]:
        cursor = conn.execute(f"{SimilarRepository.GET_QUERY} WHERE book_id = ? ORDER BY score DESC LIMIT ?",(book_id, limit))

        return [
            (row["score"], row["source_id"], row["similar_book_id"])
            for row in cursor
        ]
    
    @staticmethod
    def get_score(conn, book_id: int, candidate_id: int) -> float:
        row = conn.execute(
            f"{SimilarRepository.GET_QUERY} WHERE book_id = ? AND similar_book_id = ?",
            (book_id, candidate_id)
        ).fetchone()

        if row is None:
            return 0.0

        return float(row["score"])
    
    @staticmethod
    def clear(conn):
        conn.execute(f"{SimilarRepository.DELETE_QUERY}")
    
    @staticmethod
    def delete(conn, book_id: int, similar_book_id: int):
        conn.execute(f"{SimilarRepository.DELETE_QUERY} WHERE book_id = ? AND similar_book_id = ?", (book_id, similar_book_id, ))

    @staticmethod
    def delete_many(conn, similars: List[Tuple[float, int, int]]):
        books = list[int]({s[1] for s in similars})
        placeholders = ",".join("?" * len(books))

        conn.execute(f"{SimilarRepository.DELETE_QUERY} WHERE book_id IN ({placeholders})", books)
     
    @staticmethod   
    def has_similar(conn, book_id: int) -> bool:
        return conn.execute(
            "SELECT 1 FROM similar WHERE book_id = ? LIMIT 1",
            (book_id,)
        ).fetchone() is not None
