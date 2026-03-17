import lazy_loader as lazy
from typing import TYPE_CHECKING

__all__ = [
    "Pipeline",
    "BookScanPipeline",
    "EmbeddingPipeline",
    "IndexPipeline",
    "SimilarSearchPipeline",
    "DbPipeline",
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
    ],
    submod_attrs={
        "pipeline": ["Pipeline"],
        "book_scan_pipeline": ["BookScanPipeline"],
        "embeddingPipeline": ["EmbeddingPipeline"],
        "indexPipeline": ["IndexPipeline"],
        "similarSearchPipeline": ["SimilarSearchPipeline"],
        "dbPipeline": ["DbPipeline"],
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