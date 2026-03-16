from dataclasses import dataclass
from pathlib import Path
from enum import Enum
import os

# ==========================
# ==== ENUMS ===============
# ==========================
class IndexLevel(str, Enum):
    CHUNK = "chunk" #: генерируем индекс для всех имеющихся текстовых кусков
    DOCUMENT = "document" #: при создании индекса, сливаем все текстовые куски в один mean вектор
    BOTH = "both"  #: генерировать оба

class SearchIndexLevel(str, Enum):
    CHUNK = "chunk"  #: выполняем поиск каждого куска отдельно
    DOCUMENT = "document"  #: сливаем куски в один

class KnownModels(str, Enum):
    all_MiniLM_L6_v2 = "all-MiniLM-L6-v2"  #: Очень плохой результат, но хорошо для тестов
    multilingual_e5_base = "intfloat/multilingual-e5-base"  #: Теоретически оптимальный вариант
    multilingual_e5_large = "intfloat/multilingual-e5-large"  #: Долго, сомнительное увеличение качества
    bge_m3 = "BAAI/bge-m3"  #: Лучше base, но дольше
    sbert_large_nlu_ru = "ai-forever/sbert_large_nlu_ru"  #: Медленнее all_MiniLM_L6_v2, но хуже multilingual_e5_base

# ==========================
# ==== PATHS & DIRS ========
# ==========================
_data_dir_env = os.environ.get("DATA_DIR")
_base_dir = Path(__file__).resolve().parent.parent

@dataclass(frozen=True)
class PathsConfig:
    #: Папка для хранения результатов процессов (БД, индексы, модели)
    BASE_DIR=_base_dir
    #: Папка для хранения результатов процессов (БД, индексы, модели)
    DATA_DIR=Path(_data_dir_env) if _data_dir_env else _base_dir.parent / "data"
    #: Кэш источников (книги)
    CACHE_DIR=(Path(_data_dir_env) if _data_dir_env else _base_dir.parent / "data") / "cache"
    #: Директория сайта. Полезно для проксирования nginx https://lib.some.ru/something/similar например
    SITE_BASE_PATH=os.getenv("SITE_BASE_PATH", "")
    #: URL сайта
    LIB_URL=os.getenv("LIB_URL", "https://lib.some.ru")
    #: Файл ML модели
    RERANKER_FILE=Path(os.getenv("RERANKER_FILE", str((Path(_data_dir_env) if _data_dir_env else _base_dir.parent / "data") / "reranker.lgb")))
    #: Используется для переобучения модели
    TRANSFORM_FILE=Path(os.getenv("TRANSFORM_FILE", str((Path(_data_dir_env) if _data_dir_env else _base_dir.parent / "data") / "embedding_transform.npy")))
    #: Папка с книгами может быть удаленный путь ssh://user:pass@127.0.0.1/books/
    BOOK_FOLDER=os.getenv("BOOK_FOLDER", "/mnt/data/librusec/lib/lib.rus.ec/")
    #: INPX файл книг может быть удаленный путь ssh://user:pass@127.0.0.1/books/librusec_local_fb2.inpx
    INPX_FOLDER=os.getenv("INPX_FOLDER", "/mnt/data/librusec/lib/librusec_local_fb2.inpx")

# ==========================
# ==== PROCESS CONFIG ======
# ==========================
@dataclass(frozen=True)
class ProcessConfig:
    MAX_WORKERS=int(os.getenv("MAX_WORKERS", 2))  #: Количество одновременных задач обработки книг
    SIMILARS_PER_BOOK=int(os.getenv("SIMILARS_PER_BOOK", 100))  #: Количество похожих книг, генерируемых воркером
    DATABASE_QUEUE_BATCH_SIZE=int(os.getenv("DATABASE_QUEUE_BATCH_SIZE", 20000))  #: Размер батча для массового сохранения в БД
    MODEL_NAME=os.getenv("MODEL_NAME", KnownModels.all_MiniLM_L6_v2.value)  #: Наименование модели
    MODEL_EMBEDDING_DTYPE = os.getenv("MODEL_EMBEDDING_DTYPE", "float32")  # float16 | float32
    STORAGE_EMBEDDING_DTYPE = os.getenv("STORAGE_EMBEDDING_DTYPE", "float32")  # float16 | float32

# ==========================
# ==== CHUNKING / TEXT =====
# ==========================
@dataclass(frozen=True)
class ChunkingConfig:
    CHUNKS_PER_BOOK=int(os.getenv("CHUNKS_PER_BOOK", 7))  #: Целевое количество частей на книгу
    PREFIX_BUFFER=int(os.getenv("PREFIX_BUFFER", 15))  #: Размер префикса для генерации
    ST_MIN_CHARS=int(os.getenv("ST_MIN_CHARS", 250))  #: Минимальная длина части книги
    ST_TARGET_CHARS=int(os.getenv("ST_TARGET_CHARS", 24000))  #: Целевое суммарное количество символов всех частей книги
    ST_MAX_TITLE_CHARS=int(os.getenv("ST_MAX_TITLE_CHARS", 300))  #: Максимальное количество символов названия книги
    ST_MAX_DESCRIPTION_CHARS=int(os.getenv("ST_MAX_DESCRIPTION_CHARS", 4000))  #: Максимальное количество символов описания книги
    SECTIONS_RATIO=float(os.getenv("SECTIONS_RATIO", 0.6))  #: Регулирует деление книги на части. Если ST_TARGET_CHARS больше чем количество символов книги * SECTIONS_RATIO, CHUNKS_PER_BOOK будет занижаться  
    #: Участвует в поиске по индексу
    #: Если нам нужно получить 100 результатов, это означает, что в индексе нужно найти больше значений
    #: Поскольку часть просто отфильтруется
    #: Еще часть отфильтрует ML
    #: Чем хуже ембеддинг, тем больше у нас нагрузка на ML и тем больше должен быть OVERFETCH_FACTOR
    OVERFETCH_FACTOR=float(os.getenv("OVERFETCH_FACTOR", 25))  #: Количество найденных элементов больше, чем нужно, для фильтра

# ==========================
# ==== INDEX CONFIG ========
# ==========================
@dataclass(frozen=True)
class IndexConfig:
    BUILD_INDEX_LEVEL:IndexLevel=os.getenv("BUILD_INDEX_LEVEL", IndexLevel.BOTH.value).lower()  #: Тип создаваемого индекса
    SEARCH_INDEX_LEVEL:SearchIndexLevel=os.getenv("SEARCH_INDEX_LEVEL", SearchIndexLevel.CHUNK.value).lower()  #: Тип создаваемого индекса
    HNSW_MMAP=bool(os.getenv("HNSW_MMAP", False))  #: Не загружаем индекс в память, читаем с диска
    HNSW_M=int(os.getenv("HNSW_M", 32))  #: Максимальное количество связей (соседей) на уровне графа
    HNSW_EF_CONSTRUCTION=int(os.getenv("HNSW_EF_CONSTRUCTION", 200))  #: Размер списка кандидатов при построении графа
    HNSW_EF_SEARCH=int(os.getenv("HNSW_EF_SEARCH", 64))  #: Размер списка кандидатов при поиске

# ==========================
# ==== OTHER SETTINGS ======
# ==========================
@dataclass(frozen=True)
class OtherConfig:
    FEEDBACK_BOOST_FACTOR=0.4  #: Не используется после перехода на ML
    OPENAI_API_KEY=os.getenv("OPENAI_API_KEY", "")
    DEEPSEEK_API_KEY=os.getenv("DEEPSEEK_API_KEY", "")
    LM_STUDIO_BASE_URL=os.getenv("LM_STUDIO_BASE_URL", "")
