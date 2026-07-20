# Write/DDL capability plugins (opt-in)

This directory is **empty by default** and everything in it except this README is
gitignored. The server ships **strictly read-only** against the database: all 20
built-in tools are read-only (the two export tools additionally write a result
file to the configured export directory) and no database-mutation (write/DDL)
code exists in the codebase.

Write and DDL capabilities can only be added by **manually installing a plugin
here**. An MCP client/agent cannot register tools or install plugins — only the
server process can, at startup, and only when the steps below are all satisfied.

## How to enable writes

1. **Obtain a plugin** (distributed separately — it is not part of this repo) and
   copy it into this folder, e.g. `plugins/write_tools.py`. A reference
   implementation lives at [`docs/plugins/example_write_plugin.py.example`](../docs/plugins/example_write_plugin.py.example).
2. **Enable plugin loading**: set the environment variable
   `DB_INTROSPECTION_ENABLE_WRITE_PLUGINS=1` (a real environment variable or in the
   project-root `.env`). Without it, plugin files here are ignored.
3. **Mark specific connections writable**: in each connection's
   `DB_conns/<project>/<environment>/<schema>/db_conn.txt`, add a line `writable: true`.
   Connections without it stay strictly read-only even with a plugin loaded. This
   keeps write permission beside the rest of that connection's configuration.
4. **Restart the server.** Loaded plugins and registered tools are logged to stderr.

Remove the plugin file (or unset `DB_INTROSPECTION_ENABLE_WRITE_PLUGINS`, or set the
connection's `writable` back to `false`) and restart to disable writes again.

## Plugin contract

A plugin is a Python module exposing:

```python
from src.plugins.api import MUTATING

def register(context):
    context.mcp.add_tool(my_write_tool, name="my_write_tool", annotations=MUTATING)
```

`context` is a `src.plugins.api.PluginContext` providing:

- `mcp` — the FastMCP server; use `mcp.add_tool(fn, name=..., annotations=MUTATING)`.
- `connection_registry` — for connection resolution.
- `settings_for(connection)` — resolve a connection key to `Settings` (DSN, dialect).
- `adapter_for(connection)` — build the dialect adapter for a connection (the same
  one reads use); call `adapter.open_connection()` to open a connection without
  importing any driver.
- `plugin_config(connection, name)` — read this plugin's own per-connection
  settings (see below).
- `connection_config(connection)` — the raw parsed `db_conn.txt` (all keys), if a
  plugin needs more than its own namespace.
- `require_writable(connection)` — **call before any write**; raises
  `ValidationError("write_not_allowed")` unless the connection's db_conn.txt sets
  `writable: true`.
- `is_write_allowed(connection)` — non-raising, fail-closed check of that flag.

The plugin opens its own database connection via `adapter_for(connection).open_connection()`
and is responsible for committing. See the example for a full, commented implementation.

## Per-connection plugin configuration

A plugin can carry its own per-connection settings **in that connection's
`db_conn.txt`**, alongside the connection fields. Config ownership follows one
rule:

- **Flat keys are core-owned** — connection fields, limits, and `writable`. Don't
  reuse these names for plugin settings.
- **`plugin.<name>.<key>` keys belong to your plugin** — the core never
  interprets them. Namespace them by your plugin's name to avoid collisions with
  the core and with other plugins.

Read them with `plugin_config`, which strips the `plugin.<name>.` prefix:

```txt
# in db_conn.txt
writable:true
plugin.write.mode:dry_run
plugin.write.max_rows:100
```

```python
cfg = context.plugin_config(connection, "write")
# -> {"mode": "dry_run", "max_rows": "100"}
```

Keys are lower-cased by the file parser (so `name` and keys are
case-insensitive) and values support the same secret sources as any other field
— `${VAR}` env references and `credential://<name>` OS-keychain lookups (see the
main README). `writable` stays core-owned: gate writes with `require_writable`,
not by reading it yourself.

## Security note

This mechanism controls the **MCP tool surface**: the product ships write-disabled
and enabling it is a deliberate, auditable, out-of-tree human action. It is **not**
an OS sandbox — an installed plugin is arbitrary Python running with the server's
privileges, so only install plugins you trust.
