# db-introspection MCP server

## Multi-connection mode

Server runs as one MCP process and switches connection at tool call time using required `connection`.

## Connection layout

Connections must be stored under:
- `DB_conns/<project>/<environment>/<schema>/db_conn.txt`
- `DB_conns/PROJECT_A/DEV/schema_a/db_conn.txt`
- `DB_conns/PROJECT_A/INT/schema_a/db_conn.txt`
- `DB_conns/PROJECT_B/DEFAULT/schema_b/db_conn.txt`

Each `db_conn.txt` must contain `dialect` (`postgres|oracle|mssql`) and matching connection fields.
If a DB user has access to multiple schemas for the same connection, you can list them in `db_conn.txt` as a comma-separated value in `schema` (for example: `schema:schema_a,schema_b,schema_c`). The `schema` value is also the connection's `allowed_schemas` whitelist that every tool call is validated against.
For SQL Server, the only supported dialect value is exactly `mssql` (aliases like `sqlserver`, `sql_server`, `sql-server` are not supported). The field `driver` is optional.

### Per-connection behavior (optional keys)

All behavioral settings are configured **per connection, in that connection's `db_conn.txt`** — never through global environment variables, so one connection's limits can never bleed into another. Each key is optional and falls back to the default below:

| Key | Default | Meaning |
|---|---|---|
| `default_sample_limit` | `10` | Rows returned by `db_sample_table`/`db_select_columns` when no `limit` is given. |
| `max_sample_limit` | `100` | Hard cap for sample/select-columns `limit` (requests above it are truncated with a warning). |
| `max_select_limit` | `200` | Hard cap for `db_run_select` result rows. |
| `statement_timeout_ms` | `5000` | Statement timeout applied to `db_run_select`. |
| `include_system_schemas` | `false` | Default for `db_list_tables(include_system=...)`. |

Example with overrides:
```txt
dialect:postgres
host:db-host.example.local
db_name:app_db
port:5432
username:${APP_DEV_MAIN_USERNAME}
password:${APP_DEV_MAIN_PASSWORD}
schema:schema_a,schema_b
max_select_limit:500
statement_timeout_ms:10000
```

The environment (or `.env`) holds only DB access secrets referenced via `${VAR}` (see below) and the write-plugin master switch `DB_INTROSPECTION_ENABLE_WRITE_PLUGINS`. No connection behavior is read from the environment.

Example MSSQL `db_conn.txt`:
```txt
dialect:mssql
host:sql-host.example.local
db_name:app_db
port:1433
username:app_user
password:change_me
schema:schema_a,schema_b
driver:ODBC Driver 18 for SQL Server
```

`connection` parameter identifies a full path key in format `project/environment/schema`.
Server accepts both separators (`/` and `\`) in input, but `db_list_connections()` always returns canonical keys with `/`.

## Secrets via environment variables

To keep credentials out of plaintext files, any value in `db_conn.txt` can reference an environment variable with `${VAR}`. The reference is resolved when the file is read; if the variable is unset, the connection fails with a clear `invalid_config` error naming the missing variable (never its value).

```txt
dialect:mssql
host:sql-host.example.local
db_name:app_db
port:1433
username:${EXAMPLE_PROJECT_ENV_SCHEMA_USERNAME}
password:${EXAMPLE_PROJECT_ENV_SCHEMA_PASSWORD}
schema:schema_a,schema_b
driver:ODBC Driver 18 for SQL Server
```

Provide the values either as real environment variables or in a project-root `.env` file (gitignored). Copy `.env.example` to `.env` and fill in the secrets:

```
EXAMPLE_PROJECT_ENV_SCHEMA_USERNAME=example_user
EXAMPLE_PROJECT_ENV_SCHEMA_PASSWORD=change_me
```

Recommended variable naming: `<PROJECT>_<ENVIRONMENT>_<SCHEMA>_<FIELD>`. Real environment variables take precedence over `.env`. This is opt-in and backward compatible: values without `${...}` are still used literally, so at minimum move `password` (and ideally `username`) to `${VAR}` references.

## Write/DDL capabilities (opt-in plugin)

The server is **strictly read-only** by default and contains no write/DDL code. Write and DDL capabilities can only be added by **manually installing a plugin** into the gitignored `plugins/` directory — an MCP client/agent can never do this itself.

Enabling writes requires four deliberate steps:

1. Copy a plugin into `plugins/` (a reference implementation is at [`docs/plugins/example_write_plugin.py.example`](docs/plugins/example_write_plugin.py.example)).
2. Set `DB_INTROSPECTION_ENABLE_WRITE_PLUGINS=1` (without it, plugin files are ignored). Set it as a real environment variable or in the project-root `.env`.
3. Mark each writable connection in its own config: add `writable: true` to that connection's `DB_conns/<project>/<environment>/<schema>/db_conn.txt`. Connections without it stay strictly read-only even with a plugin loaded.
4. Restart the server (loaded plugins are logged to stderr).

Remove the plugin file or unset `DB_INTROSPECTION_ENABLE_WRITE_PLUGINS` and restart to fully disable writes again. See [`plugins/README.md`](plugins/README.md) for the plugin contract and security notes.

## Security / threat model

The read-only guarantee for `db_run_select` is enforced by `QueryGuard` (`src/services/query_guard.py`), a **lexical** check: it requires the statement to start with `SELECT`/`WITH`, rejects a blocklist of write/DDL keywords, forbids multiple statements, and strips comments and string literals so keywords hidden inside them are ignored.

Because this is lexical (not a full SQL parser plus execution sandbox), it **cannot see side-effecting functions** invoked from an otherwise valid `SELECT` — for example PostgreSQL `pg_sleep()`, `nextval()`, `dblink()`/`lo_export()`, Oracle `DBMS_*` packages, or SQL Server `xp_cmdshell`. A crafted `SELECT` that calls such a function passes the guard.

**Recommended defense in depth:** run this server against a **least-privilege, read-only database user** (grant only `SELECT`/catalog access, no write/DDL/exec rights). That way the database itself enforces read-only regardless of what SQL reaches it, and the lexical guard is only the first layer. Do not rely on `QueryGuard` alone as a security boundary against a hostile query author.

The opt-in write path (see above and [`plugins/README.md`](plugins/README.md)) is the only sanctioned way to mutate data, and even then only for connections whose `db_conn.txt` sets `writable: true`.

## Tool usage

- `db_list_connections()`
- `db_list_tables(connection="PROJECT_A/DEV/schema_a", schema="some_schema")`
- `db_list_columns(connection="PROJECT_A/DEV/schema_a", table="some_table", schema="some_schema")`
- `db_list_columns(...)` now includes `full_data_type` and a `comment` column (object/column descriptions) alongside the existing raw type fields.
- `db_list_constraints(connection="PROJECT_A/DEV/schema_a", schema="some_schema", table="some_table", constraint_type="PRIMARY KEY")`
- `db_list_indexes(connection="PROJECT_A/DEV/schema_a", schema="some_schema", table="some_table")` lists indexes (uniqueness, primary-key flag, type, indexed columns); `table` is optional.
- `db_get_ddl(connection="PROJECT_A/DEV/schema_a", schema="some_schema", object_name="some_view", object_type="view")` returns the object DDL/source as a row with a `ddl` field. `object_type` is one of `table`, `view`, `procedure`, `function`. Oracle returns authoritative DDL via `DBMS_METADATA`; on PostgreSQL and SQL Server the table DDL is reconstructed from the catalog (columns, constraints and indexes) and is flagged with a warning that it may differ slightly from the original `CREATE`.
- `db_search_objects(connection="PROJECT_A/DEV/schema_a", schema="some_schema", pattern="usr", object_types=["table", "view"])` finds objects by case-insensitive name substring. `object_types` accepts a list or CSV string from `table`, `view`, `sequence`, `procedure`, `function` and defaults to all of them.
- `db_list_sequences(connection="PROJECT_A/DEV/schema_a", schema="some_schema")`
- `db_list_procedures(connection="PROJECT_A/DEV/schema_a", schema="some_schema")`
- `db_list_functions(connection="PROJECT_A/DEV/schema_a", schema="some_schema")`
- `db_list_jobs(connection="PROJECT_A/DEV/schema_a", schema="some_schema")`
- `db_sample_table(connection="PROJECT_A/DEV/schema_a", table="some_table", schema="some_schema")`
- `db_select_columns(connection="PROJECT_A/DEV/schema_a", table="some_table", columns=["id", "name"], schema="some_schema")`
- `db_select_columns(connection="PROJECT_A/DEV/schema_a", table="some_table", columns="id,name", schema="some_schema")` (CSV is also supported)
- `db_run_select(connection="PROJECT_B/DEFAULT/schema_b", sql="SELECT 1")` (advanced/fallback)
- `db_run_select(connection="PROJECT_B/DEFAULT/schema_b", sql="SELECT 1", explain=True)` returns an estimated execution plan as rows with `plan_text`.

`schema` is required for introspection and table-preview tools and is always validated against `allowed_schemas` for the selected connection.
When `db_run_select(..., explain=True)` is used, the original validated SQL is planned without applying the tool `limit`; if `limit` is provided it is ignored and reported as a warning.

## Install in Codex (VS Code)

After installing the Codex VS Code extension, register this MCP server in the Codex CLI:

1. Open a terminal.
2. Register the server using `codex mcp add`:

### Windows (recommended)

```powershell
codex.cmd mcp add db-introspection -- "C:\ABSOLUTE\PATH\TO\PROJECT\venv\Scripts\python.exe" "C:\ABSOLUTE\PATH\TO\PROJECT\server.py"
```

### macOS / Linux

```bash
codex mcp add db-introspection -- /ABSOLUTE/PATH/TO/PROJECT/venv/bin/python /ABSOLUTE/PATH/TO/PROJECT/server.py
```

3. Verify registration:

```bash
codex mcp list
```

The server `db-introspection` should appear.

Windows note: if `codex` fails due to PowerShell execution policy (`codex.ps1`), use `codex.cmd`.

### Tool selection priority (Codex guidance)

Prefer specialized tools whenever possible:

1. `db_list_tables` for table discovery, or `db_search_objects` to locate objects by partial name across all object types.
2. `db_list_columns` for column discovery.
3. `db_list_constraints`, `db_list_indexes`, `db_list_sequences`, `db_list_procedures`, `db_list_functions`, `db_list_jobs` for metadata.
4. `db_get_ddl` to read the source/definition of a view, procedure or function (and tables on Oracle).
5. `db_sample_table` for row previews from one table.
6. `db_select_columns` for selecting explicit columns from one table.
7. `db_run_select` only as fallback for advanced SQL (JOIN, CTE, aggregates, complex filters, window functions).

All tools are read-only and are advertised to MCP clients with `readOnlyHint=true` / `destructiveHint=false` annotations.
