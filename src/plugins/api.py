from __future__ import annotations

from dataclasses import dataclass

from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations

from src.adapters.base import DatabaseAdapter
from src.adapters.factory import create_adapter
from src.config import Settings, parse_bool, read_connection_file
from src.contracts import Envelope, error_envelope, success_envelope
from src.errors import ConfigError, DatabaseError, ValidationError
from src.services.response import (
    Ok,
    elapsed_ms,
    error_from_exception,
    success_from_result,
)
from src.services.connection_registry import ConnectionRegistry

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

# Per-connection config key (in db_conn.txt) that opts a connection in to writes.
# Absent or falsey keeps the connection strictly read-only. Lives beside the
# connection's other settings so write permission is configured in one place.
WRITABLE_CONN_KEY = "writable"


@dataclass(frozen=True)
class PluginContext:
    """Stable, read-only surface passed to each installed plugin's `register`.

    A plugin uses `mcp.add_tool(...)` to register its own write/DDL tools and the
    helpers below to resolve connection settings and enforce per-connection write
    permission. This object exposes no mutation primitive — the plugin opens its
    own writable connection from the resolved DSN.
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
        """Return True if the connection's db_conn.txt opts it in to writes.

        Write permission is a per-connection config property (`writable: true` in
        that connection's db_conn.txt), kept beside its other settings instead of
        in a separate global list. Fail-closed: any resolution or parse problem
        (missing file, malformed key) is treated as not writable.
        """
        try:
            conn_file = self.connection_registry.resolve_conn_file(connection)
            values = read_connection_file(conn_file)
            return parse_bool(values.get(WRITABLE_CONN_KEY), False)
        except (ConfigError, ValidationError):
            return False

    def require_writable(self, connection: str) -> None:
        """Raise ValidationError unless the connection opts in to writes.

        Plugins MUST call this before executing any write/DDL statement.
        """
        if not self.is_write_allowed(connection):
            raise ValidationError(
                "write_not_allowed",
                (
                    f"Writes are not enabled for connection '{connection}'. "
                    f"Set '{WRITABLE_CONN_KEY}: true' in its db_conn.txt."
                ),
            )
