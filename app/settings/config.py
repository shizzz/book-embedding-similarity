from pathlib import Path
from enum import Enum
import os

class IndexLevel(str, Enum):
    CHUNK = "chunk"
    DOCUMENT = "document"
    BOTH = "both"

class KnownModels(str, Enum):
    all_MiniLM_L6_v2 = "all-MiniLM-L6-v2" # Очень плохой результат, но очень хорошо для тестов
    multilingual_e5_base = "intfloat/multilingual-e5-base" # ОК
    multilingual_e5_large = "intfloat/multilingual-e5-large" # Долго, но хорошо

BASE_DIR = Path(__file__).resolve().parent.parent

data_dir_env = os.environ.get("DATA_DIR")
DATA_DIR = Path(data_dir_env) if data_dir_env else BASE_DIR.parent / "data"
CACHE_DIR = DATA_DIR / "cache"

SITE_BASE_PATH = os.getenv("SITE_BASE_PATH", "")
LIB_URL = os.getenv("LIB_URL", "https://lib.some.ru")

INDEX_FILE = Path(os.getenv("INDEX_FILE", str(DATA_DIR / "index.faiss")))
RERANKER_FILE = Path(os.getenv("RERANKER_FILE", str(DATA_DIR / "reranker.lgb")))
TRANSFORM_FILE = Path(os.getenv("TRANSFORM_FILE", str(DATA_DIR / "embedding_transform.npy")))

BOOK_FOLDER = os.getenv("BOOK_FOLDER","/mnt/data/librusec/lib/lib.rus.ec/")
INPX_FOLDER = os.getenv("INPX_FOLDER","/mnt/data/librusec/lib/librusec_local_fb2.inpx")

MAX_WORKERS = int(os.getenv("MAX_WORKERS",2))

SIMILARS_PER_BOOK = int(os.getenv("SIMILARS_PER_BOOK",100))

DATABASE_QUEUE_BATCH_SIZE = int(os.getenv("DATABASE_QUEUE_BATCH_SIZE",20000))

# Участвует в поиске по индексу
# Если нам нужно получить 100 результатов, это означает, что в индексе нужно найти больше значений
# Поскольку часть просто отфильтруется
# Еще часть отфильтрует ML
# Чем хуже ембеддинг, тем больше у нас нагрузка на ML и тем больше должен быть OVERFETCH_FACTOR
OVERFETCH_FACTOR: float = float(os.getenv("OVERFETCH_FACTOR", 25))
MODEL_NAME: str = os.getenv("MODEL_NAME", KnownModels.all_MiniLM_L6_v2.value)
CHUNKS_PER_BOOK: int = int(os.getenv("CHUNKS_PER_BOOK",7))
ST_MIN_CHARS: int = int(os.getenv("ST_MIN_CHARS",8000))
ST_TARGET_CHARS: int = int(os.getenv("ST_TARGET_CHARS",24000))
ST_MAX_TITLE_CHARS: int = int(os.getenv("ST_MAX_TITLE_CHARS",300))
ST_MAX_DESCRIPTION_CHARS: int = int(os.getenv("ST_MAX_DESCRIPTION_CHARS",4000))

BUILD_INDEX_LEVEL = IndexLevel(
    os.getenv("BUILD_INDEX_LEVEL", IndexLevel.BOTH.value).lower()
)
HNSW_MMAP = False
HNSW_M: int = 32
HNSW_EF_CONSTRUCTION: int = 200
HNSW_EF_SEARCH: int = 64
FEEDBACK_BOOST_FACTOR: float = 0.4

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY","")
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY","")
LM_STUDIO_BASE_URL = os.getenv("LM_STUDIO_BASE_URL","")