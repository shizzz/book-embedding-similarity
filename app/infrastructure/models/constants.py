from enum import IntEnum, StrEnum, auto

class ChunkType(IntEnum):
    TITLE = 0
    DESCRIPTION = 1
    TEXT = 2

class Stages(StrEnum):
    BOOK_SEARCH = "BookSearchProducer"
    PARSER = "Parser"
    SEARCH = "Search"
    MERGER = "Merger"
    TOKENIZER = "Tokenizer"
    SIMILAR = "Similar"
    INDEX = "Index"
    CHUNK = "Chunk"
    EMBEDDING = "Embedding"
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