from collections import defaultdict
from app.models import Embedding, BookRegistry
from app.model import Model
import numpy as np
import torch


def generate_embeddings(model: Model, registry: BookRegistry) -> BookRegistry:
    """
    Generate embeddings for books and their chunks.

    Key behavior:
    ------------

    1. Multi-embedding per chunk
       If chunk.text exceeds model.st_chunk_size, it is split into subchunks.
       Each subchunk produces its own embedding.

    2. No overlap
       Chunks are already semantically meaningful.
       Overlap would create duplicate semantic vectors and hurt retrieval quality.

    3. Small subchunks are skipped
       Subchunks smaller than 15% of model.st_chunk_size (min 100 chars) are ignored,
       because very small text produces noisy embeddings.

    Result:
    -------
    Populates book.embedding: List[Embedding]
    """

    max_chars = model.st_chunk_size
    min_chars = max(100, int(max_chars * 0.15))
    batch_size = model.st_batch_size

    all_subchunks: list[str] = []
    subchunk_meta: list[tuple] = []

    # -------- collect subchunks --------
    for book in registry:

        if not getattr(book, "chunks", None):
            continue

        for chunk in book.chunks:
            text = chunk.text
            if not text:
                continue

            text_len = len(text)

            # skip extremely small original chunks
            if text_len < min_chars:
                continue

            # split WITHOUT overlap
            for sub_idx, start in enumerate(range(0, text_len, max_chars)):
                sub_text = text[start:start + max_chars]
                if len(sub_text) < min_chars:
                    continue

                all_subchunks.append(sub_text)
                subchunk_meta.append((book, chunk, sub_idx))

    # -------- generate embeddings --------
    if not all_subchunks:
        return registry

    embeddings_np = model.transformer.encode(
        all_subchunks,
        batch_size=batch_size,
        convert_to_numpy=True,
        normalize_embeddings=True,
        show_progress_bar=False
    )

    # free GPU memory immediately
    if torch.cuda.is_available():
        torch.cuda.empty_cache()

    embeddings_np = embeddings_np.astype(np.float32, copy=False)

    # -------- assign embeddings --------
    chunk_seq_counter = defaultdict(int)

    for emb_vector, (book, chunk, _) in zip(embeddings_np, subchunk_meta):

        if not getattr(book, "embedding", None):
            book.embedding = []

        seq = chunk_seq_counter[(book.id, chunk.chunk_id)]
        chunk_seq_counter[(book.id, chunk.chunk_id)] += 1

        emb = Embedding(
            book_id=book.id,
            chunk_id=chunk.chunk_id,
            data=emb_vector,
            shape=emb_vector.shape[0],
            seq=seq  # important for uniqueness
        )

        book.embedding.append(emb)

    return registry