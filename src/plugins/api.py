"""Stable API surface for opt-in capability plugins.

A plugin is any manually installed module that registers extra MCP tools via the
`PluginContext` below. The helpers here cover what a plugin commonly needs:
resolving a connection's settings/adapter (a read-only lookup reusing the same
config path as the built-in tools), reading its own per-connection configuration,
and, for plugins that mutate data, enforcing the per-connection write permission
gate. The server core stays strictly read-only; nothing here performs a mutation
itself.

Per-connection config ownership in `db_conn.txt` follows one rule:

* **Flat keys are core-owned** — connection fields, limits, and the security flag
  `writable`. The core reads and enforces these.
* **`plugin.<name>.<key>` keys belong to that plugin** — the core never
  interprets them; a plugin reads its own via `PluginContext.plugin_config`, so a
  dropped-in plugin can add per-connection settings without any core change.
"""
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

# Tool annotations a plugin can attach to a mutating (write/DDL) tool — the
# opposite of the READ_ONLY annotations used by the built-in tools in server.py.
# Plugins that expose such tools should pass this when registering them so MCP
# clients advertise them correctly. Read-only plugin tools should use their own
# read-only annotations instead.
MUTATING = ToolAnnotations(
    readOnlyHint=False,
    destructiveHint=True,
    idempotentHint=False,
    openWorldHint=True,
)

# Core-owned per-connection config key (in db_conn.txt) that opts a connection in
# to writes. Absent or falsey keeps the connection strictly read-only. It is a
# flat key (not `plugin.*`) because write permission is a core security boundary,
# enforced here for every mutating plugin via `require_writable`.
WRITABLE_CONN_KEY = "writable"

# Prefix marking plugin-owned keys in db_conn.txt: `plugin.<name>.<key>`. The core
# never interprets these; a plugin reads its own subset via `plugin_config`.
PLUGIN_KEY_PREFIX = "plugin."


@dataclass(frozen=True)
class PluginContext:
    """Stable, read-only surface passed to each installed plugin's `register`.

    A plugin uses `mcp.add_tool(...)` to register its own tools and the helpers
    below to resolve a connection's settings and adapter. This object exposes no
    mutation primitive: a plugin that needs to write opens its own connection
    from the resolved adapter and must gate it with `require_writable` first.
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

    def connection_config(self, connection: str) -> dict[str, str]:
        """Return the raw parsed `db_conn.txt` for a connection.

        This is every key in the file (core and `plugin.*` alike), with values
        already `${VAR}`-expanded, so plugin config can also reference env
        secrets. Raises ConfigError/ValidationError for an unresolvable or empty
        connection, mirroring `settings_for`. Prefer `plugin_config` to read a
        specific plugin's own keys.
        """
        conn_file = self.connection_registry.resolve_conn_file(connection)
        return read_connection_file(conn_file)

    def plugin_config(self, connection: str, plugin_name: str) -> dict[str, str]:
        """Return one plugin's per-connection settings from `db_conn.txt`.

        Selects keys named `plugin.<plugin_name>.<key>` and returns them with the
        `plugin.<plugin_name>.` prefix stripped (so `plugin.write.mode` becomes
        `mode`). `plugin_name` is matched case-insensitively because the file
        parser lower-cases keys. Returns `{}` when the connection defines none.
        This is the drop-in surface a plugin reads its own config through, with no
        core change required to add new keys.
        """
        prefix = f"{PLUGIN_KEY_PREFIX}{plugin_name.strip().lower()}."
        values = self.connection_config(connection)
        return {
            key[len(prefix):]: value
            for key, value in values.items()
            if key.startswith(prefix)
        }

    def is_write_allowed(self, connection: str) -> bool:
        """Return True if the connection's db_conn.txt opts it in to writes.

        Write permission is a core-owned per-connection property (`writable: true`
        in that connection's db_conn.txt), kept beside its other settings instead
        of in a separate global list. Fail-closed: any resolution or parse problem
        (missing file, malformed key) is treated as not writable.
        """
        try:
            values = self.connection_config(connection)
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
