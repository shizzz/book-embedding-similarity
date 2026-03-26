from enum import IntEnum, StrEnum, auto

class ChunkType(IntEnum):
    TITLE = 0
    DESCRIPTION = 1
    TEXT = 2
    MEAN = 3
    TAG = 4
    CENTROID = 5

    def supports_tokenization(self) -> bool:
        return self in {
            ChunkType.TITLE,
            ChunkType.DESCRIPTION,
            ChunkType.TEXT,
            ChunkType.TAG,
        }

class Stages(StrEnum):
    PRODUCER = "Producer"
    BOOK_SEARCH = "BookSearchProducer"
    PARSER = "Parser"
    SEARCH = "Search"
    MERGER = "Merger"
    TOKENIZER = "Tokenizer"
    SIMILAR = "Similar"
    INDEX = "Index"
    CHUNK = "Chunk"
    EMBEDDING = "Embedding"
    TAG = "TAG"
    DB = "DB"

class BookSearchEngineType(IntEnum):
    ZIP = auto()
    INPIX = auto()
    DB = auto()

    def __str__(self):
        return self.name

class SimilarSearchEngineType(StrEnum):
    INDEX = "index"
    BRUTEFORCE = "bruteforce"

    def __str__(self):
        return self.name

class SearchIndexLevel(StrEnum):
    CHUNK = "chunk"  #: выполняем поиск каждого куска отдельно
    DOCUMENT = "document"  #: сливаем куски в один

    def __str__(self):
        return self.name
    
class IndexLevel(StrEnum):
    CHUNK = "chunk" #: генерируем индекс для всех имеющихся текстовых кусков
    DOCUMENT = "document" #: при создании индекса, сливаем все текстовые куски в один mean вектор
    BOTH = "both"  #: генерировать оба
    CENTROIDS = "centroids"  #: индекс центроидов
    TAGS = "tags"  #: индекс тэгов

    def __str__(self):
        return self.value