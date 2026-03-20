import lazy_loader as lazy
from typing import TYPE_CHECKING

__all__ = [
    "BookProducer",
    "Parser",
    "DbWorker",
    "DbChunkProducer",
    "EmbeddingWorker",
    "EmbeddingProducer",
    "EmbeddingMeger",
    "Indexer",
    "SimilarStage",
    "TokenizerStage",
    "TagProducer",
    "BookTagger",
    "CentroidsProducer",
]

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submodules=[
        "book_search_producer",
        "parser",
        "db_worker",
        "db_chunk_producer",
        "embedding_generation",
        "emb_producer",
        "merge_embedding",
        "index_creation",
        "similar",
        "tokenizer",
        "tag_producer",
        "book_tagger",
        "centroids_producer",
    ],
    submod_attrs={
        "book_search_producer": ["BookProducer"],
        "parser": ["Parser"],
        "db_worker": ["DbWorker"],
        "db_chunk_producer": ["DbChunkProducer"],
        "embedding_generation": ["EmbeddingWorker"],
        "emb_producer": ["EmbeddingProducer"],
        "merge_embedding": ["EmbeddingMeger"],
        "index_creation": ["Indexer"],
        "similar": ["SimilarStage"],
        "tokenizer": ["TokenizerStage"],
        "tag_producer": ["TagProducer"],
        "book_tagger": ["BookTagger"],
        "centroids_producer": ["CentroidsProducer"],
    }
)

# --- для IDE подсветки и автокомплита ---
if TYPE_CHECKING:
    from .book_search_producer import BookProducer
    from .parser import Parser
    from .db_worker import DbWorker
    from .embedding_generation import EmbeddingWorker
    from .db_chunk_producer import DbChunkProducer
    from .emb_producer import EmbeddingProducer
    from .merge_embedding import EmbeddingMeger
    from .index_creation import Indexer
    from .similar import SimilarStage
    from .tokenizer import TokenizerStage
    from .tag_producer import TagProducer
    from .book_tagger import BookTagger
    from .centroids_producer import CentroidsProducer