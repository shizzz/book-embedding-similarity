from collections import defaultdict
from app.models import Embedding, BookRegistry
from app.model import Model
import numpy as np

def generate_embeddings(model: Model, registry: BookRegistry) -> BookRegistry:
    """
    Для каждой книги в registry генерирует embeddings для каждого chunk
    и сохраняет их в book.embedding: List[Embedding]
    """

    all_chunks = []             # все тексты для батчевой обработки
    chunk_to_book = []          # индексы книг для каждого чанка
    chunk_objs = []             # соответствующие Chunk объекты

    # собираем все chunks из всех книг
    for book_idx, book in enumerate(registry):
        if not getattr(book, "chunks", None):
            continue

        for chunk in book.chunks:
            all_chunks.append(chunk.text)
            chunk_objs.append(chunk)
            chunk_to_book.append(book_idx)

    # определяем размерность эмбеддингов
    if all_chunks:
        dummy_emb = model.transformer.encode(
            ["dummy"],
            convert_to_numpy=True,
            normalize_embeddings=True
        )[0]
        emb_dim = dummy_emb.shape[0]

        # кодируем все chunks батчами
        embeddings_chunks = model.transformer.encode(
            all_chunks,
            batch_size=model.st_batch_size,
            convert_to_numpy=True,
            normalize_embeddings=True
        )
        # освобождаем память GPU
        import torch
        torch.cuda.empty_cache()
    else:
        embeddings_chunks = np.empty((0, model.st_chunk_size), dtype=np.float32)
        emb_dim = model.st_chunk_size

    # -------- assign embeddings to chunks and books --------
    for i, emb_vector in enumerate(embeddings_chunks):
        chunk = chunk_objs[i]
        book_idx = chunk_to_book[i]
        book = registry.books[book_idx]

        if not getattr(book, "embedding", None):
            book.embedding = []

        # создаем объект Embedding
        emb = Embedding(
            book_id=book.id,
            chunk_id=chunk.chunk_id,
            data=emb_vector,
            shape=emb_vector.shape[0]
        )
        book.embedding.append(emb)

    return registry