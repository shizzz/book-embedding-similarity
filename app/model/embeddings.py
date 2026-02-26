from collections import defaultdict
import numpy as np
import torch
from collections import defaultdict
from app.models import BookRegistry
from .model import Model

def generate_embeddings(model: "Model", registry: "BookRegistry") -> "BookRegistry":
    all_chunks = []
    book_to_chunks = defaultdict(list)

    # разбиваем книги на chunks
    for idx, book in enumerate(registry):
        text = book.text
        if not text:
            continue  # обработаем пустые тексты позже

        start = 0
        while start < len(text):
            end = start + model.st_chunk_size
            chunk = text[start:end]
            all_chunks.append(chunk)
            book_to_chunks[idx].append(len(all_chunks) - 1)
            start += model.st_chunk_size - model.st_overlap

    # определяем размерность эмбеддингов модели
    dummy_emb = model.transformer.encode(
        ["dummy"],
        convert_to_numpy=True,
        normalize_embeddings=True
    )[0]
    emb_dim = dummy_emb.shape[0]

    if all_chunks:
        # кодируем все chunks батчами
        embeddings_chunks = model.transformer.encode(
            all_chunks,
            batch_size=model.st_batch_size,
            convert_to_numpy=True,
            normalize_embeddings=True
        )
        torch.cuda.empty_cache()
    else:
        embeddings_chunks = np.empty((0, emb_dim), dtype=np.float32)

    # усредняем embeddings по каждой книге
    for idx, book in enumerate(registry):
        chunk_indices = book_to_chunks.get(idx)
        if not chunk_indices:
            # если нет chunks, создаём нулевой эмбеддинг
            final_embedding = np.zeros(emb_dim, dtype=np.float32)
        else:
            chunks_embedding = embeddings_chunks[chunk_indices]
            # на всякий случай проверяем shape
            if len(chunks_embedding) == 0:
                final_embedding = np.zeros(emb_dim, dtype=np.float32)
            else:
                final_embedding = chunks_embedding.mean(axis=0)

        book.embedding = final_embedding
        book.model_id = model.uid
        del final_embedding

    return registry