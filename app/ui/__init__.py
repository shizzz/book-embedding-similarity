import lazy_loader as lazy

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submodules=["ui", "live_ui", "dummy_ui"],
    submod_attrs={
        'ui': ['BaseUI'],
        'live_ui': ['LiveUI'],
        'dummy_ui': ['DummyUI'],
    }
)