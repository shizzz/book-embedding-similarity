__all__ = [
    "GenerateEmbeddingsWorker",
    "GenerateSimilarWorker",
    "SimilarSearchWorker",
]

_lazy_mapping = {
    "GenerateEmbeddingsWorker": "generate_embeddings",
    "GenerateSimilarWorker": "generate_similar",
    "SimilarSearchWorker": "similar_search",
}

import importlib

def __getattr__(name):
    if name in _lazy_mapping:
        module_name = f".{_lazy_mapping[name]}"
        module = importlib.import_module(module_name, __name__)
        value = getattr(module, name)
        globals()[name] = value
        return value
    raise AttributeError(f"{__name__} has no attribute {name}")

def __dir__():
    return sorted(__all__)

# --- для IDE подсветки и автокомплита ---
if False:
    from .generate_embeddings import GenerateEmbeddingsWorker
    from .generate_similar import GenerateSimilarWorker
    from .similar_search import SimilarSearchWorker