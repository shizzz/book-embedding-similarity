import lazy_loader as lazy
from typing import TYPE_CHECKING
from .timer import timer
from .memory_profiler import memory_profiler

__all__ = [
    "Html",
    "get_file_bytes_from_zip",
    "to_similar_list",
    "anonymize_fb2",
    "timer",
    "memory_profiler"]

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submodules=[
        "html",
        "files",
        "toSimilarBooks",
        "anonymize_fb2",
    ],
    submod_attrs={
        "html": ["Html"],
        "files": ["get_file_bytes_from_zip"],
        "toSimilarBooks": ["to_similar_list"],
        "anonymize_fb2": ["anonymize_fb2"],
    }
)

# --- для IDE подсветки и автокомплита ---
if TYPE_CHECKING:
    from .html import Html
    from .files import get_file_bytes_from_zip
    from .toSimilarBooks import to_similar_list
    from .anonymize_fb2 import anonymize_fb2