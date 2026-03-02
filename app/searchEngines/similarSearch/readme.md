# Index & Bruteforce Similar Books Engine

## `find_similar_books` Contract

### Return Value

Returns a `List[Dict]`, where each dict represents a candidate for the reranker:

```python
{
    "source_id": int,          # ID of the source book
    "candidate_id": int,       # ID of the candidate book
    "score": float,            # aggregated score between source and candidate
    "matched_chunks": List[Dict] # list of embeddings used for reranking
        Each dict:
        {
            "query_chunk_id": Optional[int],  # None if virtual chunk
            "query_embedding": np.ndarray,    # embedding of the source
            "chunk_id": Optional[int],        # None if virtual chunk
            "embedding": np.ndarray,          # embedding of the candidate
            "score": float                    # score between query and this chunk
        }
}
```

### Why It's Structured This Way

1. **Memory Constraints**
   - ~300k books with 5–10 chunks each → millions of embeddings.
   - Using dataclasses instead of dicts increases memory footprint.
   - Web UI only has 3 GB RAM; keeping full index/results in memory is impossible.

2. **Performance**
   - Bruteforce iterates all embeddings, but batching via `EmbeddingsBatchIterable` + minimal source embedding cache keeps it feasible.
   - Creating dataclass instances for each `matched_chunk` would noticeably slow down operations.

3. **Contract**
   - The reranker expects nested dicts with `matched_chunks`.
   - Simplifying or removing `matched_chunks` breaks downstream code.
   - Nested dicts are a compromise for memory, speed, and compatibility.

4. **Why Not Dataclass**
   - Python overhead for hundreds of thousands of objects.
   - Increased GC and memory usage.
   - Dicts are optimal for speed and memory, while preserving reranker compatibility.

### Summary

- Code is "messy", but **memory-safe**.
- Preserves **reranker contract**.
- Works on web UI with limited memory.
- Changing `matched_chunks` structure or using dataclasses may cause **OOM** or slowdowns.

---

## Data Flow Diagram (Compact View)

```text
                    ┌───────────────────────┐
                    │   Источник (source)   │
                    │   book_id = 42        │
                    └─────────┬─────────────┘
                              │
                              │ reconstruct / EmbeddingsBatchIterable
                              ▼
                    ┌─────────────────────────┐
                    │  query_embedding(s)     │
                    │  (document и/или chunk) │
                    └─────────┬───────────────┘
                              │
         ┌────────────────────┴─────────────────────┐
         │                                          │
         ▼                                          ▼
┌─────────────────────┐                    ┌─────────────────────┐
│ DOCUMENT INDEX      │                    │ CHUNK INDEX         │
│ reconstruct book_id │                    │ reconstruct chunk_id│
│ search top-k books  │                    │ search top-k chunks │
└─────────┬───────────┘                    └─────────┬───────────┘
          │                                          │
          │                                          │
          │                                          │
          └───────────────┬──────────────────────────┘
                          ▼
                  ┌─────────────────────┐
                  │ candidate books     │
                  │ (candidate_id)      │
                  └─────────┬───────────┘
                            │
                            ▼
                  ┌─────────────────────┐
                  │ matched_chunks      │
                  │  - query_chunk_id   │
                  │  - query_embedding  │
                  │  - chunk_id         │
                  │  - embedding        │
                  │  - score            │
                  └─────────┬───────────┘
                            │
                            ▼
                  ┌─────────────────────┐
                  │ reranker / scoring  │
                  │  - cosine / dot     │
                  │  - filters (author,│
                  │    genre, etc.)    │
                  └─────────┬───────────┘
                            │
                            ▼
                  ┌─────────────────────┐
                  │ final result        │
                  │  List[Dict]:        │
                  │  - source_id        │
                  │  - candidate_id     │
                  │  - score            │
                  │  - matched_chunks   │
                  └─────────────────────┘
```

### Notes

- `matched_chunks` always exist, even for virtual chunks (e.g., document embeddings treated as single chunk).
- Contract preserved for downstream reranker.
- Memory-safe: bruteforce uses batching / minimal cache.
- Supports **DOCUMENT-only**, **CHUNK-only**, or **BOTH** indexing modes.