__all__ = [
    "BaseUI",
    "LiveUI",
    "DummyUI",
]

_lazy_mapping = {
    "BaseUI": "ui",
    "LiveUI": "live_ui",
    "DummyUI": "dummy_ui",
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
    from .ui import BaseUI
    from .live_ui import LiveUI
    from .dummy_ui import DummyUI