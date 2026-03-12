import lazy_loader as lazy
from typing import TYPE_CHECKING

__all__ = [
    "Pipeline",
    "EmbeddingPipeline",
]

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submodules=[
        "pipeline",
        "embeddingPipeline",
    ],
    submod_attrs={
        "pipeline": ["Pipeline"],
        "embeddingPipeline": ["EmbeddingPipeline"],
    }
)

# --- для IDE подсветки и автокомплита ---
if TYPE_CHECKING:
    from .pipeline import Pipeline
    from .embeddingPipeline import EmbeddingPipeline