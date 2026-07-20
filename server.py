from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations

from src.contracts import Envelope, ErrorEnvelope, success_envelope
from src.services.response import elapsed_ms, envelope_for_error
from src.services.connection_registry import ConnectionRegistry
from src.services.introspection_service import IntrospectionService
from src.services.select_service import SelectService

mcp = FastMCP("db-introspection")
connection_registry = ConnectionRegistry()

# Every tool in this server is strictly read-only and never mutates the target
# database. These hints let MCP clients and agents treat the tools accordingly.
READ_ONLY = ToolAnnotations(
    readOnlyHint=True,
    destructiveHint=False,
    idempotentHint=True,
    openWorldHint=True,
)

# The export tools stay strictly read-only against the database (SELECT only) but
# do write a result file into the configured export directory. readOnlyHint is
# therefore False (they modify the local filesystem), while destructiveHint stays
# False: they only create/overwrite a file inside the export directory.
EXPORT = ToolAnnotations(
    readOnlyHint=False,
    destructiveHint=False,
    idempotentHint=True,
    openWorldHint=True,
)

# All connection-scoped tools expect `connection` as a canonical
# 'project/environment/schema' key. Call db_list_connections to discover the
# available keys.
_CONNECTION_HINT = (
    "`connection` is a 'project/environment/schema' key from db_list_connections."
)


def _error_envelope(err: Exception) -> ErrorEnvelope:
    """Convert an exception into the public MCP error envelope (untimed).

    Connection resolution happens before any dialect is known, so this reuses the
    shared error mapping with dialect 'unknown' and a zero duration.
    """
    return envelope_for_error("unknown", 0, err)


def _with_services(
    connection: str,
    handler: Callable[[IntrospectionService, SelectService], Envelope],
) -> Envelope:
    """Run a handler with resolved services or return a prepared error response."""
    try:
        introspection_service, select_service = connection_registry.get_services(
            connection=connection)
    except Exception as err:  # noqa: BLE001
        return _error_envelope(err)
    return handler(introspection_service, select_service)


def _normalize_str_list(value: Any) -> list[str]:
    """Accept either list[str] or CSV string and normalize to list[str].

    Shared by tools that take a list-or-CSV argument (column names, object-type
    filters).
    """
    if value is None:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, list):
        return value
    return []


@mcp.tool(annotations=READ_ONLY)
def db_list_connections() -> Envelope:
    """List available connections as canonical 'project/environment/schema' keys.

    Each item in `data` is the exact value to pass as the `connection` argument of
    the other tools.
    """
    started = time.perf_counter()
    try:
        return success_envelope(
            dialect="unknown",
            data=connection_registry.list_connections(),
            duration_ms=elapsed_ms(started),
        )
    except Exception as err:  # noqa: BLE001
        return _error_envelope(err)


@mcp.tool(annotations=READ_ONLY)
def db_list_tables(connection: str, schema: str, include_system: bool = False) -> Envelope:
    """List tables and views visible in the selected schema scope."""
    return _with_services(
        connection,
        lambda introspection_service, _: introspection_service.list_tables(
            schema=schema,
            include_system=include_system,
        ),
    )


@mcp.tool(annotations=READ_ONLY)
def db_list_columns(connection: str, schema: str, table: str = "") -> Envelope:
    """List columns for a table in the allowed schema scope."""
    return _with_services(
        connection,
        lambda introspection_service, _: introspection_service.list_columns(table=table, schema=schema),
    )


@mcp.tool(annotations=READ_ONLY)
def db_list_constraints(
    connection: str,
    schema: str,
    table: str | None = None,
    constraint_type: str | None = None,
) -> Envelope:
    """List table constraints optionally filtered by table and constraint type."""
    return _with_services(
        connection,
        lambda introspection_service, _: introspection_service.list_constraints(
            schema=schema,
            table=table,
            constraint_type=constraint_type,
        ),
    )


@mcp.tool(annotations=READ_ONLY)
def db_list_sequences(connection: str, schema: str) -> Envelope:
    """List sequences from schemas allowed by configuration."""
    return _with_services(
        connection,
        lambda introspection_service, _: introspection_service.list_sequences(schema=schema),
    )


@mcp.tool(annotations=READ_ONLY)
def db_list_procedures(connection: str, schema: str) -> Envelope:
    """List stored procedures from schemas allowed by configuration."""
    return _with_services(
        connection,
        lambda introspection_service, _: introspection_service.list_procedures(schema=schema),
    )


@mcp.tool(annotations=READ_ONLY)
def db_list_functions(connection: str, schema: str) -> Envelope:
    """List functions from schemas allowed by configuration."""
    return _with_services(
        connection,
        lambda introspection_service, _: introspection_service.list_functions(schema=schema),
    )


@mcp.tool(annotations=READ_ONLY)
def db_list_jobs(connection: str, schema: str) -> Envelope:
    """List scheduler jobs when supported by the selected database dialect."""
    return _with_services(
        connection,
        lambda introspection_service, _: introspection_service.list_jobs(schema=schema),
    )


@mcp.tool(annotations=READ_ONLY)
def db_sample_table(
    connection: str,
    schema: str,
    table: str = "",
    limit: int | None = None,
    order_by: str | None = None,
    offset: int | None = None,
    format: str = "rows",
) -> Envelope:
    """Return a bounded preview of table rows with optional ordering.

    `offset` skips leading rows for pagination (pair with `order_by` for stable
    pages). `format` is 'rows' (default), 'csv' or 'json' — csv/json return the
    result serialized as a string.
    """
    return _with_services(
        connection,
        lambda introspection_service, _: introspection_service.sample_table(
            table=table,
            schema=schema,
            limit=limit,
            order_by=order_by,
            offset=offset,
            output_format=format,
        ),
    )


@mcp.tool(annotations=READ_ONLY)
def db_select_columns(
    connection: str,
    schema: str,
    table: str = "",
    columns: Any = None,
    limit: int | None = None,
    offset: int | None = None,
    format: str = "rows",
) -> Envelope:
    """Return rows from a table restricted to selected columns.

    `offset` skips leading rows for pagination. `format` is 'rows' (default),
    'csv' or 'json' — csv/json return the result serialized as a string.
    """
    return _with_services(
        connection,
        lambda introspection_service, _: introspection_service.select_columns(
            table=table,
            columns=_normalize_str_list(columns),
            schema=schema,
            limit=limit,
            offset=offset,
            output_format=format,
        ),
    )


@mcp.tool(annotations=READ_ONLY)
def db_run_select(
    connection: str,
    sql: str = "",
    limit: int | None = None,
    timeout_ms: int | None = None,
    explain: bool = False,
    format: str = "rows",
) -> Envelope:
    """Run a guarded read-only SELECT query or return its estimated plan.

    Write your SQL WITHOUT a row-limiting clause: the tool automatically wraps the
    query to enforce `limit` (PostgreSQL/Oracle add LIMIT/FETCH; SQL Server adds
    TOP, or OFFSET/FETCH when the query already has an ORDER BY/OFFSET). Do NOT add
    your own `TOP` — on SQL Server it collides with the tool's OFFSET/FETCH wrapper.
    Use the `limit` parameter instead (capped at `max_select_limit`).
    For paging, add your own `ORDER BY ... OFFSET ... FETCH` (no `TOP`); the tool
    limits it in place rather than re-wrapping.
    `format` is 'rows' (default), 'csv' or 'json' — csv/json return the result
    serialized as a string. For large exports use db_export_query.
    `connection` is a 'project/environment/schema' key from db_list_connections.
    """
    return _with_services(
        connection,
        lambda _, select_service: select_service.run_select(
            sql_query=sql,
            limit=limit,
            timeout_ms=timeout_ms,
            explain=explain,
            output_format=format,
        ),
    )


@mcp.tool(annotations=EXPORT)
def db_export_query(
    connection: str,
    sql: str = "",
    format: str = "csv",
    filename: str | None = None,
    max_rows: int | None = None,
    timeout_ms: int | None = None,
) -> Envelope:
    """Stream a guarded read-only SELECT to a file and return a summary.

    For large result sets: rows stream straight to disk in batches, so nothing is
    held in memory or returned inline. `format` is 'csv' (default) or 'json'.
    `filename` is an optional bare name (no path) written inside the server's
    export directory; omit it for an auto-generated name. `max_rows` caps the
    export (defaults to the connection's max_export_rows). Returns
    {path, format, row_count, byte_size, truncated}.
    `connection` is a 'project/environment/schema' key from db_list_connections.
    """
    return _with_services(
        connection,
        lambda _, select_service: select_service.export_select(
            sql_query=sql,
            filename=filename,
            output_format=format,
            timeout_ms=timeout_ms,
            max_rows=max_rows,
        ),
    )


@mcp.tool(annotations=EXPORT)
def db_export_table(
    connection: str,
    schema: str,
    table: str = "",
    columns: Any = None,
    order_by: str | None = None,
    format: str = "csv",
    filename: str | None = None,
    max_rows: int | None = None,
) -> Envelope:
    """Stream a whole table (optionally projected/ordered) to a file.

    For large tables: rows stream straight to disk in batches. `columns` optionally
    restricts the projection (list or CSV string; all columns when omitted).
    `order_by` is 'column' or 'column ASC|DESC'. `format` is 'csv' (default) or
    'json'. `filename` is an optional bare name written inside the export
    directory. `max_rows` caps the export (defaults to the connection's
    max_export_rows). Returns {path, format, row_count, byte_size, truncated}.
    `connection` is a 'project/environment/schema' key from db_list_connections.
    """
    return _with_services(
        connection,
        lambda introspection_service, _: introspection_service.export_table(
            table=table,
            schema=schema,
            columns=_normalize_str_list(columns) or None,
            order_by=order_by,
            filename=filename,
            output_format=format,
            max_rows=max_rows,
        ),
    )


@mcp.tool(annotations=READ_ONLY)
def db_list_indexes(connection: str, schema: str, table: str | None = None) -> Envelope:
    """List indexes in the allowed schema scope, optionally filtered by table.

    Returns index name, uniqueness, primary-key flag, type and indexed columns.
    `connection` is a 'project/environment/schema' key from db_list_connections.
    """
    return _with_services(
        connection,
        lambda introspection_service, _: introspection_service.list_indexes(
            schema=schema,
            table=table,
        ),
    )


@mcp.tool(annotations=READ_ONLY)
def db_get_ddl(
    connection: str,
    schema: str,
    object_name: str,
    object_type: str,
) -> Envelope:
    """Return the DDL/source of a database object.

    object_type is one of 'table', 'view', 'procedure', 'function'. Oracle returns
    authoritative DDL via DBMS_METADATA; on PostgreSQL and SQL Server the table
    DDL is reconstructed from the catalog (columns, constraints and indexes) and
    may differ slightly from the original CREATE statement.
    `connection` is a 'project/environment/schema' key from db_list_connections.
    """
    return _with_services(
        connection,
        lambda introspection_service, _: introspection_service.get_ddl(
            schema=schema,
            object_name=object_name,
            object_type=object_type,
        ),
    )


@mcp.tool(annotations=READ_ONLY)
def db_search_objects(
    connection: str,
    schema: str,
    pattern: str,
    object_types: Any = None,
) -> Envelope:
    """Find objects whose name contains a case-insensitive substring.

    object_types optionally restricts the search to a subset of
    'table', 'view', 'sequence', 'procedure', 'function' (list or CSV string);
    defaults to all of them.
    `connection` is a 'project/environment/schema' key from db_list_connections.
    """
    return _with_services(
        connection,
        lambda introspection_service, _: introspection_service.search_objects(
            schema=schema,
            pattern=pattern,
            object_types=_normalize_str_list(object_types) or None,
        ),
    )


@mcp.tool(annotations=READ_ONLY)
def db_table_stats(connection: str, schema: str, table: str) -> Envelope:
    """Return row-count estimate and size statistics for a single table.

    Data row: row_estimate (from catalog statistics, may be stale), table_bytes,
    index_bytes, total_bytes, column_count, last_analyzed. Byte fields may be null
    when the connection lacks access to the size catalogs (reported as a warning).
    `connection` is a 'project/environment/schema' key from db_list_connections.
    """
    return _with_services(
        connection,
        lambda introspection_service, _: introspection_service.table_stats(
            schema=schema,
            table=table,
        ),
    )


@mcp.tool(annotations=READ_ONLY)
def db_list_foreign_keys(connection: str, schema: str, table: str | None = None) -> Envelope:
    """List foreign-key relationships (edges) in the allowed schema scope.

    Each row: constraint_name, schema, table, columns, ref_schema, ref_table,
    ref_columns, on_delete, on_update. When `table` is given it matches either
    side, so you can ask both what a table references and what references it.
    `connection` is a 'project/environment/schema' key from db_list_connections.
    """
    return _with_services(
        connection,
        lambda introspection_service, _: introspection_service.list_foreign_keys(
            schema=schema,
            table=table,
        ),
    )


@mcp.tool(annotations=READ_ONLY)
def db_top_queries(connection: str, limit: int | None = None) -> Envelope:
    """Return the most resource-intensive queries recorded by the engine.

    Ordered by total execution time. Requires engine query-stats access
    (pg_stat_statements / V$SQLSTATS / query-stats DMVs); when unavailable the
    result is empty with an explanatory warning rather than an error.
    `connection` is a 'project/environment/schema' key from db_list_connections.
    """
    return _with_services(
        connection,
        lambda introspection_service, _: introspection_service.top_queries(limit=limit),
    )


@mcp.tool(annotations=READ_ONLY)
def db_health_check(connection: str) -> Envelope:
    """Run database health checks and return one row per check.

    Each row: check, status ('ok' or 'unknown'), value, detail. Checks are
    dialect-specific and degrade independently to 'unknown' when the required
    catalog access is missing, so partial results are expected under a
    least-privilege user.
    `connection` is a 'project/environment/schema' key from db_list_connections.
    """
    return _with_services(
        connection,
        lambda introspection_service, _: introspection_service.health_check(),
    )


if __name__ == "__main__":
    # Load opt-in plugins (inert unless DB_INTROSPECTION_ENABLE_WRITE_PLUGINS is set
    # and a plugin file is manually installed in plugins/). Done here rather than
    # at import time so importing this module (e.g. in tests) never activates them.
    from src.plugins.api import PluginContext
    from src.plugins.loader import load_plugins

    load_plugins(PluginContext(mcp=mcp, connection_registry=connection_registry))
    mcp.run(transport="stdio")
