import lazy_loader as lazy
from typing import TYPE_CHECKING

__all__ = [
    "Pipeline",
    "EmbeddingPipeline",
    "IndexPipeline",
]

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submodules=[
        "pipeline",
        "embeddingPipeline",
        "indexPipeline",
    ],
    submod_attrs={
        "pipeline": ["Pipeline"],
        "embeddingPipeline": ["EmbeddingPipeline"],
        "indexPipeline": ["IndexPipeline"],
    }
)

# --- для IDE подсветки и автокомплита ---
if TYPE_CHECKING:
    from .pipeline import Pipeline
    from .embeddingPipeline import EmbeddingPipeline
    from .indexPipeline import IndexPipeline