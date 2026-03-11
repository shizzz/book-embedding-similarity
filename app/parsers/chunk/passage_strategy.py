from .chunk_strategy import ChunkStrategy


class PassageStrategy(ChunkStrategy):
    prefix = "passage: "

    def split(self, text, max_chars, min_chars, overlap, single_chunk_mode):
        n = len(text)

        if n <= max_chars:
            if n < min_chars and not single_chunk_mode:
                return []
            return [text]

        # минимальное количество чанков
        chunks = (n + max_chars - 1) // max_chars

        # равномерный размер
        chunk_size = (n + chunks - 1) // chunks

        parts = []

        for i in range(chunks):
            start = i * chunk_size
            end = min(start + chunk_size, n)

            if overlap and i > 0:
                start = max(0, start - overlap)

            sub = text[start:end]

            if len(sub) < min_chars and not single_chunk_mode:
                continue

            parts.append(sub)

        return parts