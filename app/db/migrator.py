from pathlib import Path
from app.db.router import DBRouter
from app.settings.config import BASE_DIR

class Migrator:
    def __init__(self, router: DBRouter):
        self.router = router
        self.base_path = BASE_DIR / "db" / "migrations"

    # ---------- public ----------
    def migrate_all(self, model_uids: list[str]):
        self._migrate_meta()
        self._migrate_chunks()

        for uid in model_uids:
            self._migrate_embeddings(uid)

    def migrate_meta(self):
        self._apply_dir(
            self.base_path / "meta",
            self.router.meta()
        )

    def migrate_chunks(self):
        self._apply_dir(
            self.base_path / "chunks",
            self.router.chunks()
        )

    def migrate_embeddings(self, model_uid: str):
        self._apply_dir(
            self.base_path / "embeddings",
            self.router.embeddings(model_uid)
        )

    # ---------- internal ----------
    def _apply_dir(self, path: Path, connection_cm):
        if not path.exists():
            return
        files = sorted(path.glob("*.sql"))

        with connection_cm as conn:
            for file in files:
                sql = file.read_text(encoding="utf-8")
                conn.executescript(sql)