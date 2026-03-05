import lazy_loader as lazy
from typing import TYPE_CHECKING

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submodules=["ui", "live_ui", "dummy_ui"],
    submod_attrs={
        'ui': ['BaseUI'],
        'live_ui': ['LiveUI'],
        'dummy_ui': ['DummyUI'],
    }
)

# --- для IDE подсветки и автокомплита ---
if TYPE_CHECKING:
    from .ui import BaseUI
    from .live_ui import LiveUI
    from .dummy_ui import DummyUI