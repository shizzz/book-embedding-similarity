import lazy_loader as lazy
from typing import TYPE_CHECKING

__all__ = [
    "Pipeline",
    "BookScanPipeline",
    "EmbeddingPipeline",
    "IndexPipeline",
    "SimilarSearchPipeline",
    "DbPipeline",
    "TagIndexerPipeline",
    "TaggerPipeline",
]

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submodules=[
        "pipeline",
        "book_scan_pipeline",
        "embeddingPipeline",
        "indexPipeline",
        "similarSearchPipeline",
        "dbPipeline",
        "tag_indexer_pipeline",
        "tagger_pipeline",
    ],
    submod_attrs={
        "pipeline": ["Pipeline"],
        "book_scan_pipeline": ["BookScanPipeline"],
        "embeddingPipeline": ["EmbeddingPipeline"],
        "indexPipeline": ["IndexPipeline"],
        "similarSearchPipeline": ["SimilarSearchPipeline"],
        "dbPipeline": ["DbPipeline"],
        "tag_indexer_pipeline": ["TagIndexerPipeline"],
        "tagger_pipeline": ["TaggerPipeline"],
    }
)

# --- для IDE подсветки и автокомплита ---
if TYPE_CHECKING:
    from .pipeline import Pipeline
    from .book_scan_pipeline import BookScanPipeline
    from .embeddingPipeline import EmbeddingPipeline
    from .indexPipeline import IndexPipeline
    from .similarSearchPipeline import SimilarSearchPipeline
    from .dbPipeline import DbPipeline
    from .tag_indexer_pipeline import TagIndexerPipeline
    from .tagger_pipeline import TaggerPipeline