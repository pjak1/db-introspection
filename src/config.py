from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from src.adapters.base import DatabaseAdapter
from src.adapters.discovery import ensure_adapter_modules_loaded
from src.errors import ConfigError


def _default_conn_file_path() -> Path:
    """Return the default path to `db_conn.txt` located in the project root."""
    # src/config.py -> project root (directory containing server.py)
    return Path(__file__).resolve().parent.parent / "db_conn.txt"


def _parse_bool(value: str | None, default: bool) -> bool:
    """Parse a boolean-like environment variable value with a fallback default."""
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ConfigError("invalid_config", f"Invalid boolean value: {value}")


def _parse_csv(value: str | None, default: str) -> tuple[str, ...]:
    """Parse a comma-separated string into a non-empty tuple of trimmed items."""
    raw = value if value is not None else default
    items = tuple(part.strip() for part in raw.split(",") if part.strip())
    if not items:
        raise ConfigError("invalid_config",
                          "DB_ALLOWED_SCHEMAS cannot be empty.")
    return items


def read_connection_file(path: Path) -> dict[str, str]:
    """Load key-value pairs from a simple colon-separated connection file."""
    if not path.exists():
        return {}

    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip().lower()
        value = value.strip()
        if key and value:
            values[key] = value
    return values


def _resolve_adapter_class(db_dialect: str) -> type[DatabaseAdapter]:
    """Resolve and validate the adapter class for a given dialect."""
    ensure_adapter_modules_loaded()
    adapter_class = DatabaseAdapter.adapter_class_for(db_dialect)
    if adapter_class is None:
        raise ConfigError("invalid_config",
                          f"Unsupported DB_DIALECT: {db_dialect}")
    return adapter_class


def _resolve_required_conn_dialect(conn_values: dict[str, str]) -> str:
    """Extract and validate the required dialect from connection file values."""
    raw_dialect = conn_values.get("dialect", "").strip().lower()
    if not raw_dialect:
        raise ConfigError(
            "invalid_config",
            "Missing required 'dialect' in db_conn.txt.",
        )
    _resolve_adapter_class(raw_dialect)
    return raw_dialect


@dataclass(frozen=True)
class Settings:
    """Normalized runtime settings used by services and adapters."""
    db_dialect: str
    db_dsn: str
    allowed_schemas: tuple[str, ...]
    default_sample_limit: int
    max_sample_limit: int
    max_select_limit: int
    statement_timeout_ms: int
    include_system_schemas: bool

    @staticmethod
    def _build_common(
        *,
        db_dialect: str,
        conn_values: dict[str, str],
    ) -> "Settings":
        """Build validated settings from connection values and environment overrides."""
        adapter_class = _resolve_adapter_class(db_dialect)
        env_values = dict(os.environ)
        env_var = getattr(adapter_class, "dsn_env_var", None) or ""
        env_dsn = env_values.get(env_var, "").strip() if env_var else ""
        db_dsn = env_dsn or adapter_class.build_dsn(conn_values, env_values)
        if not db_dsn:
            env_label = env_var or "DB_DSN"
            raise ConfigError(
                "invalid_config",
                (
                    f"{env_label} is required or db_conn.txt must contain "
                    f"{db_dialect} connection fields."
                ),
            )

        fallback_schema = adapter_class.default_schema(conn_values)
        fallback_schema = conn_values.get("schema", fallback_schema)

        allowed_schemas = _parse_csv(
            os.getenv("DB_ALLOWED_SCHEMAS"), fallback_schema)
        default_sample_limit = int(os.getenv("DB_DEFAULT_SAMPLE_LIMIT", "10"))
        max_sample_limit = int(os.getenv("DB_MAX_SAMPLE_LIMIT", "100"))
        max_select_limit = int(os.getenv("DB_MAX_SELECT_LIMIT", "200"))
        statement_timeout_ms = int(
            os.getenv("DB_STATEMENT_TIMEOUT_MS", "5000"))
        include_system_schemas = _parse_bool(
            os.getenv("DB_INCLUDE_SYSTEM_SCHEMAS"), False)

        if default_sample_limit <= 0:
            raise ConfigError("invalid_config",
                              "DB_DEFAULT_SAMPLE_LIMIT must be > 0.")
        if max_sample_limit <= 0:
            raise ConfigError("invalid_config",
                              "DB_MAX_SAMPLE_LIMIT must be > 0.")
        if max_select_limit <= 0:
            raise ConfigError("invalid_config",
                              "DB_MAX_SELECT_LIMIT must be > 0.")
        if statement_timeout_ms <= 0:
            raise ConfigError("invalid_config",
                              "DB_STATEMENT_TIMEOUT_MS must be > 0.")
        if default_sample_limit > max_sample_limit:
            raise ConfigError(
                "invalid_config",
                "DB_DEFAULT_SAMPLE_LIMIT cannot be greater than DB_MAX_SAMPLE_LIMIT.",
            )

        return Settings(
            db_dialect=db_dialect,
            db_dsn=db_dsn,
            allowed_schemas=allowed_schemas,
            default_sample_limit=default_sample_limit,
            max_sample_limit=max_sample_limit,
            max_select_limit=max_select_limit,
            statement_timeout_ms=statement_timeout_ms,
            include_system_schemas=include_system_schemas,
        )

    @classmethod
    def from_connection_values(
        cls,
        *,
        conn_values: dict[str, str],
        dialect_override: str | None = None,
    ) -> "Settings":
        """Create settings from parsed connection values and optional dialect override."""
        db_dialect = (
            (dialect_override or "").strip().lower()
            or conn_values.get("dialect", "").strip().lower()
            or os.getenv("DB_DIALECT", "").strip().lower()
        )
        if not db_dialect:
            raise ConfigError(
                "invalid_config",
                "Missing DB dialect. Provide dialect_override, 'dialect' in db_conn.txt, or DB_DIALECT.",
            )
        return cls._build_common(db_dialect=db_dialect, conn_values=conn_values)

    @classmethod
    def from_env(cls) -> "Settings":
        """Create settings from the default connection file and environment values."""
        conn_path = _default_conn_file_path()
        if not conn_path.exists():
            raise ConfigError(
                "invalid_config",
                f"Missing required connection file: {conn_path}",
            )
        conn_values = read_connection_file(conn_path)
        db_dialect = _resolve_required_conn_dialect(conn_values)
        return cls._build_common(db_dialect=db_dialect, conn_values=conn_values)
