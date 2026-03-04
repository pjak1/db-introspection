from __future__ import annotations

from src.adapters.base import DatabaseAdapter
from src.adapters.discovery import ensure_adapter_modules_loaded
from src.config import Settings
from src.errors import ConfigError


def create_adapter(settings: Settings) -> DatabaseAdapter:
    """Instantiate an adapter for the configured dialect."""
    ensure_adapter_modules_loaded()
    adapter_class = DatabaseAdapter.adapter_class_for(settings.db_dialect)
    if adapter_class is None:
        raise ConfigError("invalid_config",
                          f"Unsupported DB_DIALECT: {settings.db_dialect}")
    return adapter_class(dsn=settings.db_dsn)
