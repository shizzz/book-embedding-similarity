from collections import defaultdict
from app.infrastructure.models import Embedding, BookRegistry
from app.model import Model
import numpy as np
import torch

def generate_embeddings(model: Model, registry: BookRegistry) -> BookRegistry:

    max_chars = model.info.st_chunk_size
    overlap = model.info.st_overlap
    batch_size = model.info.st_batch_size
    min_chars = max(100, int(max_chars * 0.15))

    texts, meta = collect_chunks(
        registry,
        max_chars,
        min_chars,
        overlap
    )

    if not texts:
        return registry

    embeddings = model.transformer.encode(
        texts,
        batch_size=batch_size,
        convert_to_numpy=True,
        normalize_embeddings=True,
        show_progress_bar=False
    )

    if torch.cuda.is_available():
        torch.cuda.empty_cache()

    embeddings = embeddings.astype(np.float32, copy=False)

    assign_embeddings(embeddings, meta)

    return registry

class ChunkStrategy:
    prefix = ""

    def prepare(self, text: str) -> str:
        return self.prefix + text
    def split(self, text, max_chars, min_chars, overlap, single_chunk_mode):
        raise NotImplementedError

class TitleStrategy(ChunkStrategy):
    prefix = "title: "
    def split(self, text, max_chars, min_chars, overlap, single_chunk_mode):
        return [text]
    
class DescriptionStrategy(ChunkStrategy):
    prefix = "description: "
    def split(self, text, max_chars, min_chars, overlap, single_chunk_mode):
        if len(text) <= max_chars:
            return [text]

        step = max_chars - overlap
        parts = []

        for start in range(0, len(text), step):
            sub = text[start:start + max_chars]
            if not sub:
                break
            parts.append(sub)

        return parts
    
class PassageStrategy(ChunkStrategy):
    prefix = "passage: "
    def split(self, text, max_chars, min_chars, overlap, single_chunk_mode):
        text_len = len(text)

        if text_len <= max_chars:
            if text_len < min_chars and not single_chunk_mode:
                return []
            return [text]

        parts = []
        for start in range(0, text_len, max_chars):
            sub = text[start:start + max_chars]
            if len(sub) < min_chars:
                continue

            parts.append(sub)

        if not parts:
            return [text]

        return parts

STRATEGIES = {
    0: TitleStrategy(),
    1: DescriptionStrategy(),
    2: PassageStrategy(),
}
    
def collect_chunks(registry, max_chars, min_chars, overlap):
    texts = []
    meta = []

    for book in registry:
        if not getattr(book, "chunks", None):
            continue

        single_chunk_mode = len(book.chunks) == 1
        for chunk in book.chunks:
            if not chunk.text:
                continue

            strategy = STRATEGIES.get(chunk.type, PassageStrategy())
            prepared = strategy.prepare(chunk.text)
            parts = strategy.split(
                prepared,
                max_chars,
                min_chars,
                overlap,
                single_chunk_mode
            )

            for idx, part in enumerate(parts):
                texts.append(part)
                meta.append((book, chunk, idx))

    return texts, meta

def assign_embeddings(embeddings, meta):
    chunk_seq_counter = defaultdict(int)
    for emb_vector, (book, chunk, _) in zip(embeddings, meta):
        if not getattr(book, "embedding", None):
            book.embedding = []

        seq = chunk_seq_counter[(book.id, chunk.chunk_id)]
        chunk_seq_counter[(book.id, chunk.chunk_id)] += 1

        emb = Embedding(
            book_id=book.id,
            chunk_id=chunk.chunk_id,
            data=emb_vector,
            shape=emb_vector.shape[0],
            seq=seq
        )

        book.embedding.append(emb)