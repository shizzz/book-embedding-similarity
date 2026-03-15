import lazy_loader as lazy
from typing import TYPE_CHECKING

__all__ = ["IndexManager", "FaissId"]

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submodules=["hnsw", "faissId"],
    submod_attrs={
        "hnsw": ["IndexManager"],
        "faissId": ["FaissId"],
    }
)

if TYPE_CHECKING:
    from .hnsw import IndexManager
    from .faissId import FaissId