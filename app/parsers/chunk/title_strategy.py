from .chunk_strategy import ChunkStrategy

class TitleStrategy(ChunkStrategy):
    prefix = "title: "

    def split(self, text, max_chars, min_chars, overlap, single_chunk_mode):
        return [text]