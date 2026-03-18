from dataclasses import dataclass, field
from app.settings import ChunkingConfig

@dataclass
class ParserConfig:
    target_chars: int = field(default_factory=lambda: ChunkingConfig.ST_TARGET_CHARS)
    min_chars: int = field(default_factory=lambda: ChunkingConfig.ST_MIN_CHARS)
    max_description_chars: int = field(default_factory=lambda: ChunkingConfig.ST_MAX_DESCRIPTION_CHARS)
    sections: int = field(default_factory=lambda: ChunkingConfig.CHUNKS_PER_BOOK)
    prefix_buffer: int = field(default_factory=lambda: ChunkingConfig.PREFIX_BUFFER)
    sections_ratio: float = field(default_factory=lambda: ChunkingConfig.SECTIONS_RATIO)