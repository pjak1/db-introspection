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
If a DB user has access to multiple schemas for the same connection, you can list them in `db_conn.txt` as a comma-separated value in `schema` (for example: `schema:schema_a,schema_b,schema_c`).
For SQL Server, the only supported dialect value is exactly `mssql` (aliases like `sqlserver`, `sql_server`, `sql-server` are not supported). The field `driver` is optional.

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

## Tool usage

- `db_list_connections()`
- `db_list_tables(connection="PROJECT_A/DEV/schema_a", schema="some_schema")`
- `db_list_columns(connection="PROJECT_A/DEV/schema_a", table="some_table", schema="some_schema")`
- `db_sample_table(connection="PROJECT_A/DEV/schema_a", table="some_table", schema="some_schema")`
- `db_select_columns(connection="PROJECT_A/DEV/schema_a", table="some_table", columns=["id", "name"], schema="some_schema")`
- `db_select_columns(connection="PROJECT_A/DEV/schema_a", table="some_table", columns="id,name", schema="some_schema")` (CSV is also supported)
- `db_run_select(connection="PROJECT_B/DEFAULT/schema_b", sql="SELECT 1")` (advanced/fallback)

`schema` is required for introspection and table-preview tools and is always validated against `allowed_schemas` for the selected connection.

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

1. `db_list_tables` for table discovery.
2. `db_list_columns` for column discovery.
3. `db_list_constraints`, `db_list_sequences`, `db_list_procedures`, `db_list_functions`, `db_list_jobs` for metadata.
4. `db_sample_table` for row previews from one table.
5. `db_select_columns` for selecting explicit columns from one table.
6. `db_run_select` only as fallback for advanced SQL (JOIN, CTE, aggregates, complex filters, window functions).
