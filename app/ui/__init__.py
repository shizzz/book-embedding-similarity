import importlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .live_ui import LiveUI
    from .dummy_ui import DummyUI
    from .ui import BaseUI

class _LazyModule:
    def __init__(self, name):
        self._name = name
        self._modules = {}

    def __getattr__(self, attr):
        if attr in self._modules:
            return self._modules[attr]

        if attr == "LiveUI":
            module = importlib.import_module(f".live_ui", self._name)
            cls = getattr(module, "LiveUI")
        elif attr == "DummyUI":
            module = importlib.import_module(f".dummy_ui", self._name)
            cls = getattr(module, "DummyUI")
        elif attr == "BaseUI":
            module = importlib.import_module(f".ui", self._name)
            cls = getattr(module, "BaseUI")
        else:
            raise AttributeError(f"Module {self._name} has no attribute {attr}")

        self._modules[attr] = cls
        return cls


import sys
sys.modules[__name__] = _LazyModule(__name__)