from pathlib import Path
import os

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

SITE_BASE_PATH = os.getenv("SITE_BASE_PATH", "")
LIB_URL = os.getenv("LIB_URL", "https://lib.some.ru")

DB_FILE = Path(os.getenv("DB_FILE", str(DATA_DIR / "data.db")))
INDEX_FILE = Path(os.getenv("INDEX_FILE", str(DATA_DIR / "index.faiss")))
RERANKER_FILE = Path(os.getenv("RERANKER_FILE", str(DATA_DIR / "reranker.lgb")))
MODEL_NAME = os.getenv("MODEL_NAME","all-MiniLM-L6-v2")

BOOK_FOLDER = os.getenv("BOOK_FOLDER","/mnt/data/librusec/lib/lib.rus.ec/")
INPX_FOLDER = os.getenv("BOOK_FOLDER","/mnt/data/librusec/lib/librusec_local_fb2.inpx")

MAX_WORKERS = int(os.getenv("MAX_WORKERS","7"))

SIMILARS_PER_BOOK = int(os.getenv("SIMILARS_PER_BOOK","100"))

DATABASE_QUEUE_BATCH_SIZE = int(os.getenv("DATABASE_QUEUE_BATCH_SIZE","20000"))

HNSW_M: int = 32
HNSW_EF_CONSTRUCTION: int = 200
HNSW_EF_SEARCH: int = 64
FEEDBACK_BOOST_FACTOR: float = 0.4

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY","")
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY","")
LM_STUDIO_BASE_URL = os.getenv("LM_STUDIO_BASE_URL","")