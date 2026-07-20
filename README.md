# db-introspection MCP server

A **read-only** [Model Context Protocol](https://modelcontextprotocol.io) server that lets an MCP client (Codex, Claude, …) introspect and safely query **Oracle, PostgreSQL and SQL Server** databases. It exposes 20 tools — list tables/columns/constraints/indexes/sequences/procedures/functions/jobs, read object DDL, search objects by name, map foreign-key relationships, report table statistics, surface top queries and health checks, sample rows, run guarded `SELECT`s, and stream large results to CSV/JSON files — across many named connections in a single process. All tools are read-only against the database; the two export tools additionally write a result file to a configured directory. Write/DDL to the database is not possible unless you deliberately install an opt-in plugin (see below).

## Requirements

- **Python 3.10+** (developed and tested on 3.13).
- Runtime packages from `requirements.txt` (installed below): `mcp`, `oracledb`, `psycopg[binary]`, `pyodbc`, `python-dotenv`, `keyring`.
- **Per-dialect prerequisites:**
  - **PostgreSQL** — nothing extra; `psycopg[binary]` bundles its own libpq.
  - **Oracle** — nothing extra; `oracledb` runs in *thin* mode (no Oracle Client install required).
  - **SQL Server** — an ODBC driver installed on the OS, e.g. *ODBC Driver 18 for SQL Server*; name it in each MSSQL connection's `driver:` field.
- **Optional (recommended) encrypted secrets** use the OS-native keychain via `keyring`: Windows Credential Manager / macOS Keychain / Linux Secret Service.

## Installation & setup

1. **Create a virtual environment and install dependencies.**

   Windows (PowerShell):
   ```powershell
   python -m venv venv
   venv\Scripts\Activate.ps1
   pip install -r requirements.txt
   ```
   macOS / Linux:
   ```bash
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```
   To also run the tests, add `pip install -r requirements-dev.txt`.

2. **Add at least one connection.** Create `DB_conns/<project>/<environment>/<schema>/db_conn.txt` (see [Connection layout](#connection-layout)). A ready-to-copy template is at [`DB_conns/.example_project/ENV/SCHEMA/db_conn.txt.example`](DB_conns/.example_project/ENV/SCHEMA/db_conn.txt.example).

3. **Provide credentials.** Inline in `db_conn.txt`, or keep them out of plaintext with `${VAR}`/`.env` or the OS keychain — see [Secrets](#secrets-via-environment-variables).

4. **Register the server in your MCP client.** The client launches the server over stdio; see [Install in Codex (VS Code)](#install-in-codex-vs-code).

5. **Verify.** Without any client you can smoke-test connection discovery and run the tests from the project root:
   ```bash
   venv/Scripts/python -c "from src.services.connection_registry import ConnectionRegistry as R; print(R().list_connections())"
   venv/Scripts/python -m pytest -q
   ```

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
| `max_export_rows` | `1000000` | Hard cap for the file-export tools (`db_export_table`/`db_export_query`). Exports stream to disk, so this is deliberately much higher than the interactive limits. |
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

The environment (or `.env`) holds only DB access secrets referenced via `${VAR}` (see below), the write-plugin master switch `DB_INTROSPECTION_ENABLE_WRITE_PLUGINS`, and the optional export-directory override `DB_INTROSPECTION_EXPORT_DIR` (where the export tools write files; defaults to `exports/` in the project root, which is gitignored). These are server-wide filesystem/feature switches, not per-connection behavior — no connection behavior is read from the environment.

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

### Encrypted credentials via the OS keychain (recommended, optional)

`.env` still stores secrets in plaintext. For encryption at rest — with no master password to manage — a value in `db_conn.txt` can instead reference a secret held in the **OS keychain** (Windows Credential Manager / macOS Keychain / Linux Secret Service) using a `credential://<name>` reference:

```txt
username:credential://REZA_DEV_RPP_REZA_USERNAME
password:credential://REZA_DEV_RPP_REZA_PASSWORD
```

The secret is decrypted transparently for the current OS user at connection-read time; it never sits in any file the agent can read. This is fully opt-in — the `${VAR}`/`.env` mechanism above keeps working unchanged, and you can mix both across connections.

Manage keychain secrets with the bundled CLI:

```powershell
python -m src.secrets set REZA_DEV_RPP_REZA_PASSWORD   # prompts without echo
python -m src.secrets list
python -m src.secrets get REZA_DEV_RPP_REZA_PASSWORD
python -m src.secrets delete REZA_DEV_RPP_REZA_PASSWORD
```

To migrate an existing `.env`, `python -m src.secrets import-env` copies every secret from `.env` into the keychain and prints the `${VAR}` → `credential://<name>` swaps to apply in each `db_conn.txt`. It does not modify your files or delete `.env`, so the switch stays manual and reversible; remove the plaintext values from `.env` once you have verified the connections still work.

If a `credential://` reference names a secret that is not stored (or the keychain is unavailable), the connection fails fast with a clear `invalid_config` error naming the field — exactly like an unset `${VAR}`.

## Write/DDL capabilities (opt-in plugin)

The server is **strictly read-only** by default and contains no write/DDL code. Write and DDL capabilities can only be added by **manually installing a plugin** into the gitignored `plugins/` directory — an MCP client/agent can never do this itself.

Enabling writes requires four deliberate steps:

1. Copy a plugin into `plugins/` (a reference implementation is at [`docs/plugins/example_write_plugin.py.example`](docs/plugins/example_write_plugin.py.example)).
2. Set `DB_INTROSPECTION_ENABLE_WRITE_PLUGINS=1` (without it, plugin files are ignored). Set it as a real environment variable or in the project-root `.env`.
3. Mark each writable connection in its own config: add `writable: true` to that connection's `DB_conns/<project>/<environment>/<schema>/db_conn.txt`. Connections without it stay strictly read-only even with a plugin loaded.
4. Restart the server (loaded plugins are logged to stderr).

Remove the plugin file or unset `DB_INTROSPECTION_ENABLE_WRITE_PLUGINS` and restart to fully disable writes again. See [`plugins/README.md`](plugins/README.md) for the plugin contract and security notes.

A plugin can also carry its own per-connection settings in `db_conn.txt` using `plugin.<name>.<key>` keys (for example `plugin.write.mode:dry_run`), which the core ignores and the plugin reads via `context.plugin_config(connection, "<name>")`. Flat keys (including `writable`) stay core-owned. See [`plugins/README.md`](plugins/README.md).

## Security / threat model

The read-only guarantee for `db_run_select` is enforced by `QueryGuard` (`src/services/query_guard.py`), a **lexical** check: it requires the statement to start with `SELECT`/`WITH`, rejects a blocklist of write/DDL keywords, forbids multiple statements, and strips comments and string literals so keywords hidden inside them are ignored.

Because this is lexical (not a full SQL parser plus execution sandbox), it **cannot see side-effecting functions** invoked from an otherwise valid `SELECT` — for example PostgreSQL `pg_sleep()`, `nextval()`, `dblink()`/`lo_export()`, Oracle `DBMS_*` packages, or SQL Server `xp_cmdshell`. A crafted `SELECT` that calls such a function passes the guard.

**Defense in depth (built in):** every read tool runs inside an engine-enforced read-only transaction where the dialect supports it — PostgreSQL (`read_only` session) and Oracle (`SET TRANSACTION READ ONLY`) reject any write outright. SQL Server has no read-only transaction mode, so reads there are never committed (always rolled back), discarding any side effect a read might have triggered.

**Recommended defense in depth (operational):** additionally run this server against a **least-privilege, read-only database user** (grant only `SELECT`/catalog access, no write/DDL/exec rights). That way the database enforces read-only at the permission level too, independent of the transaction mode and the lexical guard. Do not rely on `QueryGuard` alone as a security boundary against a hostile query author.

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
- `db_table_stats(connection="PROJECT_A/DEV/schema_a", schema="some_schema", table="some_table")` returns a row-count estimate (from catalog statistics), table/index/total size in bytes, column count and last-analyzed time. Byte fields may be null when the connection cannot read the size catalogs (reported as a warning).
- `db_list_foreign_keys(connection="PROJECT_A/DEV/schema_a", schema="some_schema", table="some_table")` returns FK edges (`constraint_name`, `table`, `columns`, `ref_table`, `ref_columns`, `on_delete`, `on_update`). `table` is optional and matches either side, so you can ask both what a table references and what references it.
- `db_sample_table(connection="PROJECT_A/DEV/schema_a", table="some_table", schema="some_schema", limit=20, order_by="id desc", offset=40, format="csv")` previews rows; all of `limit`/`order_by`/`offset`/`format` are optional. `offset` paginates (pair with `order_by` for stable pages); `format` is `rows` (default), `csv` or `json` (csv/json return the result serialized as a string).
- `db_select_columns(connection="PROJECT_A/DEV/schema_a", table="some_table", columns=["id", "name"], schema="some_schema", limit=20, offset=40, format="json")`
- `db_select_columns(connection="PROJECT_A/DEV/schema_a", table="some_table", columns="id,name", schema="some_schema")` (CSV is also supported)
- `db_run_select(connection="PROJECT_B/DEFAULT/schema_b", sql="SELECT 1", limit=100, timeout_ms=8000, format="csv")` (advanced/fallback); `limit` is capped at `max_select_limit`, `timeout_ms` overrides the connection's `statement_timeout_ms`, and `format` serializes the result as `csv`/`json`. The tool auto-wraps the query to enforce `limit` (SQL Server uses `TOP`, or `OFFSET/FETCH` when the query already has `ORDER BY`/`OFFSET`), so **do not add your own `TOP`** — it collides with the wrapper on SQL Server; use the `limit` parameter. For paging, add your own `ORDER BY ... OFFSET ... FETCH` (without `TOP`).
- `db_run_select(connection="PROJECT_B/DEFAULT/schema_b", sql="SELECT 1", explain=True)` returns an estimated execution plan as rows with `plan_text`.
- `db_top_queries(connection="PROJECT_A/DEV/schema_a", limit=20)` returns the most time-consuming queries the engine has recorded (`query_id`, `query`, `calls`, `total_ms`, `mean_ms`, `rows`). Requires engine query-stats access (PostgreSQL `pg_stat_statements`, Oracle `V$SQLSTATS`, SQL Server query-stats DMVs); when unavailable the result is empty with an explanatory warning instead of an error.
- `db_health_check(connection="PROJECT_A/DEV/schema_a")` runs dialect-specific health checks and returns one row per check (`check`, `status`, `value`, `detail`). Each check degrades independently to `status="unknown"` when the required catalog access is missing, so partial results are expected under a least-privilege user.
- `db_export_table(connection="PROJECT_A/DEV/schema_a", table="some_table", schema="some_schema", columns=["id","name"], order_by="id desc", format="csv", filename="dump", max_rows=500000)` streams a whole table to a file. `columns`/`order_by`/`filename`/`max_rows` are optional; `format` is `csv` (default) or `json`. Rows are fetched in batches and written straight to disk, so large tables never sit in memory or in the response — only a summary comes back: `{path, format, row_count, byte_size, truncated}`.
- `db_export_query(connection="PROJECT_B/DEFAULT/schema_b", sql="SELECT ...", format="csv", filename="report", max_rows=500000, timeout_ms=8000)` streams the result of a guarded read-only SELECT to a file, same batched streaming and summary payload as `db_export_table`.
- Export tools write into the export directory (`DB_INTROSPECTION_EXPORT_DIR`, default `exports/` in the project root, gitignored). `filename` must be a bare name (no path separators or `..`); when omitted an auto-generated name is used. `max_rows` is capped at the connection's `max_export_rows`; hitting the cap sets `truncated=true` with a warning. These are the only built-in tools that write to the local filesystem — they remain strictly read-only against the database and are advertised with `readOnlyHint=false` (file side effect) / `destructiveHint=false`.

`schema` is required for introspection and table-preview tools and is always validated against `allowed_schemas` for the selected connection.
When `db_run_select(..., explain=True)` is used, the original validated SQL is planned without applying the tool `limit`; if `limit` is provided it is ignored and reported as a warning.
The `db_top_queries` and `db_health_check` tools read instance-level catalogs/DMVs and therefore depend on the privileges granted to the connection's DB user; they are designed to degrade gracefully rather than fail.

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
3. `db_list_constraints`, `db_list_indexes`, `db_list_foreign_keys`, `db_list_sequences`, `db_list_procedures`, `db_list_functions`, `db_list_jobs` for metadata (`db_list_foreign_keys` for relationship/dependency questions).
4. `db_get_ddl` to read the source/definition of a view, procedure, function or table (table DDL is authoritative on Oracle and reconstructed from the catalog on PostgreSQL/SQL Server).
5. `db_table_stats` for size/row-count questions about one table.
6. `db_sample_table` for row previews from one table.
7. `db_select_columns` for selecting explicit columns from one table.
8. `db_run_select` only as fallback for advanced SQL (JOIN, CTE, aggregates, complex filters, window functions).
9. `db_top_queries` and `db_health_check` for performance/diagnostics questions (privilege-dependent; degrade gracefully).
10. `db_export_table` / `db_export_query` for large exports to a file (streamed to disk; return a summary, not the rows) — prefer these over `format=csv`/`json` on the read tools when the result is big.

All tools are strictly read-only against the database and are advertised to MCP clients with `destructiveHint=false`. Every tool carries `readOnlyHint=true` except the two export tools (`db_export_table`/`db_export_query`), which write a result file into the export directory and are therefore marked `readOnlyHint=false`; they still never mutate the database.
