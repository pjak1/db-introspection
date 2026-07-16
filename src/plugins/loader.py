from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path

from src.config import parse_bool
from src.plugins.api import PluginContext

# Environment flag that must be truthy for any plugin to be loaded. Defense in
# depth on top of the manual file installation: a stray plugin file left in
# `plugins/` does nothing unless this is explicitly enabled. May be set as a real
# environment variable or in the project-root `.env` (loaded by src.config). It
# is named for write plugins because guarding writes is why it exists, but it
# gates loading of every plugin regardless of what the plugin does.
ENABLE_ENV = "DB_INTROSPECTION_ENABLE_WRITE_PLUGINS"


def _log(message: str) -> None:
    """Emit an audit line to stderr (stdout is the MCP JSON-RPC channel)."""
    print(f"[plugins] {message}", file=sys.stderr, flush=True)


def _default_plugins_dir(context: PluginContext) -> Path:
    """Return the project-root `plugins/` installation directory."""
    return context.connection_registry.resolve_project_root() / "plugins"


def load_plugins(context: PluginContext, plugins_dir: Path | None = None) -> list[str]:
    """Load opt-in plugins if explicitly enabled.

    Inert by default: returns immediately unless DB_INTROSPECTION_ENABLE_WRITE_PLUGINS is truthy.
    Each `*.py` in the plugins directory is imported and its `register(context)`
    entry point is called. A broken or contract-violating plugin is logged to
    stderr and skipped so the read-only server keeps running.

    Returns the names of successfully registered plugin modules.
    """
    if not parse_bool(os.getenv(ENABLE_ENV), False):
        return []

    directory = plugins_dir if plugins_dir is not None else _default_plugins_dir(context)
    if not directory.exists() or not directory.is_dir():
        return []

    registered: list[str] = []
    for path in sorted(directory.glob("*.py")):
        if path.stem == "__init__" or path.stem.startswith("_"):
            continue
        try:
            module = _import_module_from_path(path)
            register = getattr(module, "register", None)
            if not callable(register):
                _log(f"skipped '{path.name}': no register(context) function")
                continue
            register(context)
            registered.append(path.stem)
            _log(f"loaded plugin '{path.name}'")
        except Exception as exc:  # noqa: BLE001 — one bad plugin must not crash the server
            _log(f"failed to load '{path.name}': {exc!r}")

    return registered


def _import_module_from_path(path: Path):
    """Import a standalone module from a file path outside the src package."""
    module_name = f"db_plugin_{path.stem}"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot create import spec for {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module
