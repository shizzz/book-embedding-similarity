import lazy_loader as lazy
from typing import TYPE_CHECKING

__all__ = [
    "GenerateEmbeddingsWorker",
    "GenerateSimilarWorker",
    "SimilarSearchWorker",
]

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submodules=[
        "generate_embeddings",
        "generate_similar",
        "similar_search",
    ],
    submod_attrs={
        "generate_embeddings": ["GenerateEmbeddingsWorker"],
        "generate_similar": ["GenerateSimilarWorker"],
        "similar_search": ["SimilarSearchWorker"],
    }
)

# --- для IDE подсветки и автокомплита ---
if TYPE_CHECKING:
    from .generate_embeddings import GenerateEmbeddingsWorker
    from .generate_similar import GenerateSimilarWorker
    from .similar_search import SimilarSearchWorker