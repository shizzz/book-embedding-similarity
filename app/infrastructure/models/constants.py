from enum import IntEnum, StrEnum

class ChunkType(IntEnum):
    TITLE = 0
    DESCRIPTION = 1
    TEXT = 2

class Stages(StrEnum):
    BOOK_SEARCH = "BookSearchProducer"
    CHUNK = "Chunk"
    EMBEDDING = "Embedding"
    DB = "DB"