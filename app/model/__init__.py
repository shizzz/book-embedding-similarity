import lazy_loader as lazy
from typing import TYPE_CHECKING

__all__ = ["Model",]

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submodules=["model", "embeddings"],
    submod_attrs={
        "model": ["Model"],
    }
)

if TYPE_CHECKING:
    from .model import Model