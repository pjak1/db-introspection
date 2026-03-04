from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

from src.adapters.factory import create_adapter
from src.config import Settings, read_connection_file
from src.errors import ConfigError, ValidationError
from src.services.introspection_service import IntrospectionService
from src.services.select_service import SelectService


@dataclass
class _CachedServices:
    """Cache entry storing services and the source file modification timestamp."""
    mtime_ns: int
    introspection_service: IntrospectionService
    select_service: SelectService


class ConnectionRegistry:
    """Resolve and cache services backed by `DB_conns/<project>/<env>/<schema>/db_conn.txt`."""
    def __init__(self) -> None:
        """Initialize an empty in-memory cache and default connections directory name."""
        self._cache: dict[str, _CachedServices] = {}
        self._connections_dir_name = "DB_conns"

    def resolve_project_root(self) -> Path:
        """Return the MCP server project root derived from this source location."""
        return Path(__file__).resolve().parents[2]

    @staticmethod
    def _normalize_connection(connection: str | None) -> str:
        """Validate and normalize `project/env/schema` connection keys."""
        if connection is None or not connection.strip():
            raise ValidationError("missing_connection_schema", "connection is required.")

        normalized = re.sub(r"/+", "/", connection.strip().replace("\\", "/"))
        parts = normalized.split("/")
        if len(parts) != 3 or any(not part for part in parts):
            raise ValidationError(
                "missing_connection_schema",
                "connection must be in format 'project/environment/schema'.",
            )
        if any(part in {".", ".."} for part in parts):
            raise ValidationError(
                "missing_connection_schema",
                "connection contains invalid path segments.",
            )
        return "/".join(parts)

    def _resolve_connections_root(self) -> Path:
        """Return the directory expected to contain named connection subfolders."""
        return self.resolve_project_root() / self._connections_dir_name

    def list_connections(self) -> list[str]:
        """List canonical `project/environment/schema` keys that contain `db_conn.txt`."""
        connections_root = self._resolve_connections_root()
        if not connections_root.exists() or not connections_root.is_dir():
            return []

        connections: list[str] = []
        for conn_file in connections_root.rglob("db_conn.txt"):
            try:
                rel_parent = conn_file.parent.relative_to(connections_root)
            except ValueError:
                continue
            parts = rel_parent.parts
            if len(parts) != 3:
                continue
            connections.append("/".join(parts))
        connections.sort()
        return connections

    def resolve_conn_file(self, connection: str) -> Path:
        """Resolve and validate `db_conn.txt` path for a canonical connection key."""
        normalized_connection = self._normalize_connection(connection)
        connections_root = self._resolve_connections_root()
        conn_file = connections_root / Path(*normalized_connection.split("/")) / "db_conn.txt"
        if not conn_file.exists():
            raise ConfigError(
                "invalid_config",
                (
                    f"Connection file not found for connection='{normalized_connection}'. "
                    "Expected file at "
                    f"'{connections_root}/<project>/<environment>/<schema>/db_conn.txt'. "
                    f"Resolved path: {conn_file}"
                ),
            )
        return conn_file

    def _build_services(self, conn_file: Path) -> _CachedServices:
        """Build fresh introspection/select services from a connection file."""
        conn_values = read_connection_file(conn_file)
        if not conn_values:
            raise ConfigError("invalid_config", f"Connection file is empty: {conn_file}")

        settings = Settings.from_connection_values(conn_values=conn_values)
        adapter = create_adapter(settings)
        introspection_service = IntrospectionService(adapter=adapter, settings=settings)
        select_service = SelectService(adapter=adapter, settings=settings)
        return _CachedServices(
            mtime_ns=conn_file.stat().st_mtime_ns,
            introspection_service=introspection_service,
            select_service=select_service,
        )

    def get_services(self, connection: str | None) -> tuple[IntrospectionService, SelectService]:
        """Get cached services for a connection or rebuild when config file changes."""
        normalized_connection = self._normalize_connection(connection)
        conn_file = self.resolve_conn_file(normalized_connection)
        mtime_ns = conn_file.stat().st_mtime_ns
        cached = self._cache.get(normalized_connection)
        if cached and cached.mtime_ns == mtime_ns:
            return cached.introspection_service, cached.select_service

        # Invalidate cache automatically whenever the connection file timestamp changes.
        rebuilt = self._build_services(conn_file)
        self._cache[normalized_connection] = rebuilt
        return rebuilt.introspection_service, rebuilt.select_service
