import torch
from collections import defaultdict
from app.models import BookRegistry
from .model import Model
from app.settings.config import ST_CHUNK_SIZE, ST_OVERLAP, ST_BATCH_SIZE

def generate_embeddings(model: Model, registry: BookRegistry) -> BookRegistry:
        all_chunks = []
        book_to_chunks = defaultdict(list)

        # разбиваем книги на chunks
        for idx, book in enumerate(registry):
            text = book.text
            if not text:
                continue

            start = 0
            while start < len(text):
                end = start + ST_CHUNK_SIZE
                chunk = text[start:end]
                all_chunks.append(chunk)
                book_to_chunks[idx].append(len(all_chunks) - 1)
                start += ST_CHUNK_SIZE - ST_OVERLAP

        if not all_chunks:
            return registry

        # кодируем все chunks батчами
        embeddings_chunks = model.transformer.encode(
            all_chunks,
            batch_size=ST_BATCH_SIZE,
            convert_to_numpy=True,
            normalize_embeddings=True
        )
        torch.cuda.empty_cache()

        # усредняем embeddings по каждой книге
        for idx, book in enumerate(registry):
            chunk_indices = book_to_chunks.get(idx)
            if not chunk_indices:
                continue

            chunks_embedding = embeddings_chunks[chunk_indices]
            final_embedding = chunks_embedding.mean(axis=0)

            book.embedding = final_embedding
            book.model_id = model.uid
            
        return registry