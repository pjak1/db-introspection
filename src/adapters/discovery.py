from __future__ import annotations

import importlib
import pkgutil

import src.adapters as adapters_pkg

_LOADED = False
_SKIP_MODULES = {"__init__", "base", "discovery", "factory", "normalization"}


def ensure_adapter_modules_loaded() -> None:
    """Import adapter modules once so subclass auto-registration can run."""
    global _LOADED
    if _LOADED:
        return
    package_path = adapters_pkg.__path__
    package_name = adapters_pkg.__name__
    for module_info in pkgutil.iter_modules(package_path):
        if module_info.name in _SKIP_MODULES:
            continue
        importlib.import_module(f"{package_name}.{module_info.name}")
    _LOADED = True
