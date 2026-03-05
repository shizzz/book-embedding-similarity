import lazy_loader as lazy
from typing import TYPE_CHECKING

__all__ = ["IndexManager"]

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submodules=["hnsw"],
    submod_attrs={"hnsw": ["IndexManager"]},
)

if TYPE_CHECKING:
    from .hnsw import IndexManager