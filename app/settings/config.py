from pathlib import Path
import os

BASE_DIR = Path(__file__).resolve().parent.parent

data_dir_env = os.environ.get("DATA_DIR")
DATA_DIR = Path(data_dir_env) if data_dir_env else BASE_DIR.parent / "data"
CACHE_DIR = DATA_DIR / "cache"

SITE_BASE_PATH = os.getenv("SITE_BASE_PATH", "")
LIB_URL = os.getenv("LIB_URL", "https://lib.some.ru")

DB_FILE = Path(os.getenv("DB_FILE", str(DATA_DIR / "data.db")))
INDEX_FILE = Path(os.getenv("INDEX_FILE", str(DATA_DIR / "index.faiss")))
RERANKER_FILE = Path(os.getenv("RERANKER_FILE", str(DATA_DIR / "reranker.lgb")))
TRANSFORM_FILE = Path(os.getenv("TRANSFORM_FILE", str(DATA_DIR / "embedding_transform.npy")))

BOOK_FOLDER = os.getenv("BOOK_FOLDER","/mnt/data/librusec/lib/lib.rus.ec/")
INPX_FOLDER = os.getenv("INPX_FOLDER","/mnt/data/librusec/lib/librusec_local_fb2.inpx")

MAX_WORKERS = int(os.getenv("MAX_WORKERS",2))

SIMILARS_PER_BOOK = int(os.getenv("SIMILARS_PER_BOOK",100))

DATABASE_QUEUE_BATCH_SIZE = int(os.getenv("DATABASE_QUEUE_BATCH_SIZE",20000))


MODEL_NAME: str = os.getenv("MODEL_NAME","intfloat/multilingual-e5-large")
ST_CHUNK_SIZE: int  = int(os.getenv("ST_CHUNK_SIZE",2500))
ST_OVERLAP: int = int(os.getenv("ST_OVERLAP",300))
ST_BATCH_SIZE: int = int(os.getenv("ST_BATCH_SIZE",8))

ST_MIN_CHARS: int = int(os.getenv("ST_MIN_CHARS",3000))
ST_TARGET_CHARS: int = int(os.getenv("ST_TARGET_CHARS",16000))
ST_MAX_TITLE_CHARS: int = int(os.getenv("ST_MAX_TITLE_CHARS",300))
ST_MAX_DESCRIPTION_CHARS: int = int(os.getenv("ST_MAX_DESCRIPTION_CHARS",2000))

HNSW_M: int = 32
HNSW_EF_CONSTRUCTION: int = 200
HNSW_EF_SEARCH: int = 64
FEEDBACK_BOOST_FACTOR: float = 0.4

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY","")
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY","")
LM_STUDIO_BASE_URL = os.getenv("LM_STUDIO_BASE_URL","")