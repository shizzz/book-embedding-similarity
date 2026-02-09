from pathlib import Path
from .connection import db

class Migrator:
    def apply_schema(self):
        schema_path = Path(__file__).with_name("schema.sql")
        schema = schema_path.read_text(encoding="utf-8")

        with db() as conn:
            conn.executescript(schema)
