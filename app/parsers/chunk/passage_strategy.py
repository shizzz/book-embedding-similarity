from .chunk_strategy import ChunkStrategy

class PassageStrategy(ChunkStrategy):
    prefix = "passage: "

    def split(self, text, max_chars, min_chars, overlap, single_chunk_mode):
        if len(text) <= max_chars:
            if len(text) < min_chars and not single_chunk_mode:
                return []
            return [text]

        step = max_chars - overlap if max_chars > overlap else max_chars
        parts = []
        for start in range(0, len(text), step):
            sub = text[start:start + max_chars]
            if len(sub) < min_chars and not single_chunk_mode:
                continue
            parts.append(sub)
        return parts or [text]