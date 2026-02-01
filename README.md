# Book Embedding Similarity

🚧 **WORK IN PROGRESS** 🚧
Expect breaking changes.

A tool for semantic similarity search across large FB2 libraries using
sentence embeddings, SQLite and cosine similarity.

The project is designed for offline processing of large FB2 collections
stored inside ZIP archives and focuses on correctness, reproducibility
and simplicity rather than raw processing speed.

---

## Features

- Works directly with FB2 files inside ZIP archives
- Sentence embeddings via `sentence-transformers`
- Cosine similarity search
- SQLite storage (persistent across restarts)
- Docker & docker-compose ready

---

## Notes on Performance

Embedding generation is a computationally expensive, offline process.
For large libraries it may take many hours or even days depending on
hardware, model choice and dataset size.

This project prioritizes correctness and transparency of processing
over aggressive optimization.

---

## Entry Points

The container provides several scripts:

| Script | Description |
|------|-------------|
| `generate_authors.py` | Extract and normalize authors |
| `generate_embeddings.py` | Generate embeddings for books |
| `generate_similar.py` | Compute similarity relationships |
| `get_similar.py` | Query top-N similar books |

---

## Configuration

All settings are configurable via environment variables:

| Variable | Default |
|--------|--------|
| `LIB_URL` | `https://lib.ooosh.ru` |
| `BOOK_FOLDER` | `/books` |
| `DB_FILE` | `/data/data.db` |
| `MODEL_NAME` | `all-MiniLM-L6-v2` |
| `MAX_WORKERS` | `7` |

---

## Docker Usage

```bash
docker compose build
docker compose run --rm book-sim generate_embeddings.py
docker compose run --rm book-sim get_similar.py 10419.fb2
