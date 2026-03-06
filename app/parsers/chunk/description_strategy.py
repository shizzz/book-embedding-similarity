from .chunk_strategy import ChunkStrategy

class DescriptionStrategy(ChunkStrategy):
    prefix = "description: "

    def split(self, text, max_chars, min_chars, overlap, single_chunk_mode):
        if len(text) <= max_chars:
            return [text]
        step = max_chars - overlap
        return [text[i:i+max_chars] for i in range(0, len(text), step)]