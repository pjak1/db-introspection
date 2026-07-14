from __future__ import annotations

import os
from dataclasses import dataclass

from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations

from src.adapters.base import DatabaseAdapter
from src.adapters.factory import create_adapter
from src.config import Settings
from src.contracts import Envelope, error_envelope, success_envelope
from src.errors import DatabaseError, ValidationError
from src.services._response_helpers import (
    Ok,
    elapsed_ms,
    error_from_exception,
    success_from_result,
)
from src.services.connection_registry import ConnectionRegistry, normalize_connection_key

# Re-exported for plugin convenience so plugins depend on this stable surface
# instead of reaching into internal modules.
__all__ = [
    "MUTATING",
    "PluginContext",
    "Envelope",
    "success_envelope",
    "success_from_result",
    "Ok",
    "error_envelope",
    "error_from_exception",
    "elapsed_ms",
    "ValidationError",
    "DatabaseError",
]

# Tool annotations for mutating (write/DDL) tools — the opposite of the
# READ_ONLY annotations used by the built-in tools in server.py. Plugins should
# pass this when registering their tools so MCP clients advertise them correctly.
MUTATING = ToolAnnotations(
    readOnlyHint=False,
    destructiveHint=True,
    idempotentHint=False,
    openWorldHint=True,
)

# Name of the environment variable holding the per-connection write allowlist.
WRITABLE_CONNECTIONS_ENV = "DB_WRITABLE_CONNECTIONS"


def _writable_connections() -> frozenset[str]:
    """Return the set of connection keys explicitly allowed to perform writes."""
    raw = os.getenv(WRITABLE_CONNECTIONS_ENV, "")
    return frozenset(
        normalize_connection_key(part) for part in raw.split(",") if part.strip()
    )


@dataclass(frozen=True)
class PluginContext:
    """Stable, read-only surface passed to each installed plugin's `register`.

    A plugin uses `mcp.add_tool(...)` to register its own write/DDL tools and the
    helpers below to resolve connection settings and enforce the write allowlist.
    This object exposes no mutation primitive — the plugin opens its own writable
    connection from the resolved DSN.
    """
    mcp: FastMCP
    connection_registry: ConnectionRegistry

    def settings_for(self, connection: str) -> Settings:
        """Resolve a `project/environment/schema` key to Settings (DSN, dialect).

        This is a read-only fact lookup reusing the same config path as reads.
        """
        return self.connection_registry.build_settings(connection)

    def adapter_for(self, connection: str) -> DatabaseAdapter:
        """Build the dialect adapter for a connection (same one reads use).

        The returned adapter exposes `open_connection()` so a plugin can open a
        connection exactly like the read-only path, without importing any driver.
        """
        return create_adapter(self.settings_for(connection))

    def is_write_allowed(self, connection: str) -> bool:
        """Return True if `connection` is on the DB_WRITABLE_CONNECTIONS allowlist."""
        return normalize_connection_key(connection) in _writable_connections()

    def require_writable(self, connection: str) -> None:
        """Raise ValidationError unless `connection` is on the write allowlist.

        Plugins MUST call this before executing any write/DDL statement.
        """
        if not self.is_write_allowed(connection):
            raise ValidationError(
                "write_not_allowed",
                (
                    f"Writes are not enabled for connection '{connection}'. "
                    f"Add its canonical key to {WRITABLE_CONNECTIONS_ENV}."
                ),
            )
