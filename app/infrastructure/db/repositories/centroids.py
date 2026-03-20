from typing import Generator
from app.infrastructure.db import DBRouter
from app.infrastructure.models.tag import Tag
from app.infrastructure.models.constants import ChunkType
from .model import ModelRepository
from app.settings import ProcessConfig

class CentroidsRepository:
    def __init__(self, router: DBRouter, model_uid: str = None):
        self.router = router
        self.model_uid = model_uid or ModelRepository(router).get_latest_uid(ProcessConfig.MODEL_NAME)[0]

    ACCEPTED_TYPES = [ChunkType.TAG, ChunkType.CENTROID]
    # ------------------------
    # READ
    # ------------------------
    def get_all(self, type: ChunkType) -> Generator[Tag, None, None]:
        if type not in CentroidsRepository.ACCEPTED_TYPES:
            raise TypeError(f"Unknown tag type {type}")

        with self.router.embeddings(self.model_uid) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT 
                    id,
                    source_id as parent_id,
                    name as name_ru,
                    NULL as name_en,
                    data,
                    [type]
                FROM embeddings
                WHERE [type] = ?
                ORDER BY id
                """,
                (type,)
            )

            while True:
                row = cursor.fetchone()
                if not row:
                    break

                yield Tag.from_row(row)

    def get_ids(self) -> list[int]:
        with self.router.embeddings(self.model_uid) as conn:
            rows = conn.execute("SELECT source_id FROM embeddings WHERE [type] = ?", (ChunkType.CENTROID,)).fetchall()
            return [r[0] for r in rows]
        
    # ------------------------
    # COUNT
    # ------------------------
    def count(self) -> int:
        with self.router.embeddings(self.model_uid) as conn:
            return conn.execute("SELECT COUNT(*) FROM embeddings WHERE [type] = ?", (ChunkType.CENTROID,)).fetchone()[0]