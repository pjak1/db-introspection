from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

from src.adapters.base import DatabaseAdapter
from src.adapters.discovery import ensure_adapter_modules_loaded
from src.errors import ConfigError

# Load secrets from a project-root `.env` once at import time. Real environment
# variables take precedence (override=False), and the file is optional.
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

# Connection-file values may reference environment variables with `${VAR}`, so
# secrets (username/password) can live in the environment instead of on disk.
# A literal `${...}` is written `$${...}`: the `$$` (only when it precedes `{`)
# is the escape and collapses to a single `$` without expanding. A bare `$` or
# `$$` not followed by `{` is left untouched, so existing values keep working.
_ENV_REF_PATTERN = re.compile(r"\$\$(?=\{)|\$\{([A-Za-z_][A-Za-z0-9_]*)\}")

# Default behavioral limits used when a connection's db_conn.txt does not set the
# corresponding key. These are per-connection settings (each db_conn.txt can
# override them); they are deliberately NOT read from the environment, so no
# global env value ever bleeds across connections.
_DEFAULT_SAMPLE_LIMIT = 10
_DEFAULT_MAX_SAMPLE_LIMIT = 100
_DEFAULT_MAX_SELECT_LIMIT = 200
_DEFAULT_STATEMENT_TIMEOUT_MS = 5000


def _default_conn_file_path() -> Path:
    """Return the default path to `db_conn.txt` located in the project root."""
    # src/config.py -> project root (directory containing server.py)
    return Path(__file__).resolve().parent.parent / "db_conn.txt"


def _expand_env_refs(key: str, value: str) -> str:
    """Replace `${VAR}` references with environment values; fail if one is unset.

    `$${` is an escape: it collapses to a literal `${` and is not expanded, so a
    value that must contain a literal `${...}` is written `$${...}`.
    """
    def _replace(match: re.Match[str]) -> str:
        var_name = match.group(1)
        if var_name is None:  # matched the `$$` escape before a `{`
            return "$"
        env_value = os.environ.get(var_name)
        if env_value is None:
            raise ConfigError(
                "invalid_config",
                f"Connection field '{key}' references undefined "
                f"environment variable '{var_name}'.",
            )
        return env_value

    return _ENV_REF_PATTERN.sub(_replace, value)


def parse_bool(value: str | None, default: bool) -> bool:
    """Parse a boolean-like config value (from db_conn.txt or env) with a default."""
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
                          "'schema' in db_conn.txt cannot be empty.")
    return items


def _parse_int(key: str, value: str | None, default: int) -> int:
    """Parse an integer connection-file value, or fall back to `default`.

    Raises ConfigError (naming the key) on a non-integer value, so a typo in
    db_conn.txt fails with a clear message instead of a bare ValueError.
    """
    if value is None or not value.strip():
        return default
    try:
        return int(value.strip())
    except ValueError as err:
        raise ConfigError(
            "invalid_config",
            f"Connection field '{key}' must be an integer, got: {value!r}.",
        ) from err


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
            values[key] = _expand_env_refs(key, value)
    return values


def _resolve_adapter_class(db_dialect: str) -> type[DatabaseAdapter]:
    """Resolve and validate the adapter class for a given dialect."""
    ensure_adapter_modules_loaded()
    adapter_class = DatabaseAdapter.adapter_class_for(db_dialect)
    if adapter_class is None:
        raise ConfigError("invalid_config",
                          f"Unsupported dialect: {db_dialect}")
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
        """Build validated settings from connection-file values.

        Every behavioral setting (schemas, limits, timeout, system-schema toggle)
        comes from `conn_values` (the connection's db_conn.txt) with per-setting
        defaults. Nothing here is read from the environment: secrets are already
        expanded into `conn_values` via `${VAR}` when the file is read, so no
        global env value can bleed across connections.
        """
        adapter_class = _resolve_adapter_class(db_dialect)
        db_dsn = adapter_class.build_dsn(conn_values)
        if not db_dsn:
            raise ConfigError(
                "invalid_config",
                f"db_conn.txt must contain the {db_dialect} connection fields.",
            )

        fallback_schema = adapter_class.default_schema(conn_values)
        allowed_schemas = _parse_csv(conn_values.get("schema"), fallback_schema)
        default_sample_limit = _parse_int(
            "default_sample_limit",
            conn_values.get("default_sample_limit"),
            _DEFAULT_SAMPLE_LIMIT,
        )
        max_sample_limit = _parse_int(
            "max_sample_limit",
            conn_values.get("max_sample_limit"),
            _DEFAULT_MAX_SAMPLE_LIMIT,
        )
        max_select_limit = _parse_int(
            "max_select_limit",
            conn_values.get("max_select_limit"),
            _DEFAULT_MAX_SELECT_LIMIT,
        )
        statement_timeout_ms = _parse_int(
            "statement_timeout_ms",
            conn_values.get("statement_timeout_ms"),
            _DEFAULT_STATEMENT_TIMEOUT_MS,
        )
        include_system_schemas = parse_bool(
            conn_values.get("include_system_schemas"), False)

        if default_sample_limit <= 0:
            raise ConfigError("invalid_config",
                              "default_sample_limit must be > 0.")
        if max_sample_limit <= 0:
            raise ConfigError("invalid_config",
                              "max_sample_limit must be > 0.")
        if max_select_limit <= 0:
            raise ConfigError("invalid_config",
                              "max_select_limit must be > 0.")
        if statement_timeout_ms <= 0:
            raise ConfigError("invalid_config",
                              "statement_timeout_ms must be > 0.")
        if default_sample_limit > max_sample_limit:
            raise ConfigError(
                "invalid_config",
                "default_sample_limit cannot be greater than max_sample_limit.",
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
        )
        if not db_dialect:
            raise ConfigError(
                "invalid_config",
                "Missing DB dialect. Provide dialect_override or 'dialect' in db_conn.txt.",
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
