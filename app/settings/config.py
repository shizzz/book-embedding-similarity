from pathlib import Path
import os

BASE_DIR = Path(__file__).resolve().parent.parent

SITE_BASE_PATH = os.getenv("LIB_URL", "")
LIB_URL = os.getenv("LIB_URL", "https://lib.ooosh.ru")

DB_FILE = Path(os.getenv("DB_FILE", str(BASE_DIR / "data/data.db")))
INDEX_FILE = Path(os.getenv("INDEX_FILE", str(BASE_DIR / "data/hnsw_index.index")))

BOOK_FOLDER = os.getenv("BOOK_FOLDER","/mnt/data/librusec/lib/lib.rus.ec/")

MODEL_NAME = os.getenv("MODEL_NAME","all-MiniLM-L6-v2")

MAX_WORKERS = int(os.getenv("MAX_WORKERS","7"))

SIMILARS_PER_BOOK = int(os.getenv("SIMILARS_PER_BOOK","100"))

DATABASE_QUEUE_BATCH_SIZE = int(os.getenv("DATABASE_QUEUE_BATCH_SIZE","20000"))

HNSW_M: int = 32
HNSW_EF_CONSTRUCTION: int = 200
HNSW_EF_SEARCH: int = 64
FEEDBACK_BOOST_FACTOR: float = 0.4