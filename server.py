from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations

from src.contracts import Envelope, ErrorEnvelope, error_envelope
from src.errors import AppError
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

# All connection-scoped tools expect `connection` as a canonical
# 'project/environment/schema' key. Call db_list_connections to discover the
# available keys.
_CONNECTION_HINT = (
    "`connection` is a 'project/environment/schema' key from db_list_connections."
)


@dataclass
class ServiceResolution:
    """Resolved services or an error envelope when resolution fails."""
    introspection_service: IntrospectionService | None = None
    select_service: SelectService | None = None
    error: ErrorEnvelope | None = None

    @property
    def is_error(self) -> bool:
        return self.error is not None

    @classmethod
    def success(
        cls,
        introspection_service: IntrospectionService,
        select_service: SelectService,
    ) -> "ServiceResolution":
        return cls(
            introspection_service=introspection_service,
            select_service=select_service,
            error=None,
        )

    @classmethod
    def failure(cls, error: ErrorEnvelope) -> "ServiceResolution":
        return cls(error=error)


def _error_envelope(err: Exception) -> ErrorEnvelope:
    """Convert any exception into the public MCP error envelope format."""
    if isinstance(err, AppError):
        return error_envelope(
            dialect="unknown",
            code=err.code,
            message=err.message,
            duration_ms=0,
            details=err.details,
        )
    return error_envelope(
        dialect="unknown",
        code="internal_error",
        message="Unexpected internal error.",
        duration_ms=0,
        details=str(err),
    )


def _services_for(connection: str) -> ServiceResolution:
    """Resolve services for a connection, returning an error envelope on failure."""
    try:
        introspection_service, select_service = connection_registry.get_services(connection=connection)
        return ServiceResolution.success(introspection_service, select_service)
    except Exception as err:  # noqa: BLE001
        return ServiceResolution.failure(_error_envelope(err))


def _with_services(
    connection: str,
    handler: Callable[[IntrospectionService, SelectService], Envelope],
) -> Envelope:
    """Run a handler with resolved services or return a prepared error response."""
    resolution = _services_for(connection=connection)
    if resolution.is_error:
        if resolution.error is None:
            return error_envelope(
                dialect="unknown",
                code="internal_error",
                message="Unexpected internal error.",
                duration_ms=0,
                details="Service resolution flagged an error without payload.",
            )
        return resolution.error

    if resolution.introspection_service is None or resolution.select_service is None:
        return error_envelope(
            dialect="unknown",
            code="internal_error",
            message="Unexpected internal error.",
            duration_ms=0,
            details="Service resolution returned incomplete state.",
        )
    return handler(resolution.introspection_service, resolution.select_service)


def _normalize_columns(columns: Any) -> list[str]:
    """Accept either list[str] or CSV string and normalize to list[str]."""
    if columns is None:
        return []
    if isinstance(columns, str):
        return [item.strip() for item in columns.split(",") if item.strip()]
    if isinstance(columns, list):
        return columns
    return []


@mcp.tool(annotations=READ_ONLY)
def db_list_connections() -> dict:
    """List available connections as canonical 'project/environment/schema' keys.

    Each returned key is the exact value to pass as the `connection` argument of
    the other tools.
    """
    try:
        return {
            "ok": True,
            "connections": connection_registry.list_connections(),
        }
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
) -> Envelope:
    """Return a bounded preview of table rows with optional ordering."""
    return _with_services(
        connection,
        lambda introspection_service, _: introspection_service.sample_table(
            table=table,
            schema=schema,
            limit=limit,
            order_by=order_by,
        ),
    )


@mcp.tool(annotations=READ_ONLY)
def db_select_columns(
    connection: str,
    schema: str,
    table: str = "",
    columns: Any = None,
    limit: int | None = None,
) -> Envelope:
    """Return rows from a table restricted to selected columns."""
    return _with_services(
        connection,
        lambda introspection_service, _: introspection_service.select_columns(
            table=table,
            columns=_normalize_columns(columns),
            schema=schema,
            limit=limit,
        ),
    )


@mcp.tool(annotations=READ_ONLY)
def db_run_select(
    connection: str,
    sql: str = "",
    limit: int | None = None,
    timeout_ms: int | None = None,
    explain: bool = False,
) -> Envelope:
    """Run a guarded read-only SELECT query or return its estimated plan.

    `connection` is a 'project/environment/schema' key from db_list_connections.
    """
    return _with_services(
        connection,
        lambda _, select_service: select_service.run_select(
            sql_query=sql,
            limit=limit,
            timeout_ms=timeout_ms,
            explain=explain,
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

    object_type is one of 'table', 'view', 'procedure', 'function' (table DDL is
    only supported on Oracle; on PostgreSQL/SQL Server use db_list_columns,
    db_list_constraints and db_list_indexes for tables).
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
            object_types=_normalize_columns(object_types) or None,
        ),
    )


if __name__ == "__main__":
    mcp.run(transport="stdio")
