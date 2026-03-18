import lazy_loader as lazy
from typing import TYPE_CHECKING

__all__ = [
    "ParseBooks",
    "GenerateEmbeddings",
    "GenerateAll",
    "GenerateSimilarWorker",
    "SimilarSearchWorker",
    "GenerateIndexWorker",
]

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submodules=[
        "parse_books",
        "generate_embeddings",
        "generate_all",
        "generate_similar",
        "similar_search",
        "generate_index",
    ],
    submod_attrs={
        "parse_books": ["ParseBooks"],
        "generate_embeddings": ["GenerateEmbeddings"],
        "generate_all": ["GenerateAll"],
        "generate_similar": ["GenerateSimilarWorker"],
        "similar_search": ["SimilarSearchWorker"],
        "generate_index": ["GenerateIndexWorker"],
    }
)

# --- для IDE подсветки и автокомплита ---
if TYPE_CHECKING:
    from .parse_books import ParseBooks
    from .generate_embeddings import GenerateEmbeddings
    from .generate_all import GenerateAll
    from .generate_similar import GenerateSimilarWorker
    from .similar_search import SimilarSearchWorker
    from .generate_index import GenerateIndexWorker