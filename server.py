from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from mcp.server.fastmcp import FastMCP

from src.contracts import Envelope, ErrorEnvelope, error_envelope
from src.errors import AppError
from src.services.connection_registry import ConnectionRegistry
from src.services.introspection_service import IntrospectionService
from src.services.select_service import SelectService

mcp = FastMCP("db-introspection")
connection_registry = ConnectionRegistry()


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


@mcp.tool()
def db_list_connections() -> dict:
    """List all available connection folders under the configured connections root."""
    try:
        return {
            "ok": True,
            "connections": connection_registry.list_connections(),
        }
    except Exception as err:  # noqa: BLE001
        return _error_envelope(err)


@mcp.tool()
def db_list_tables(connection: str, schema: str, include_system: bool = False) -> Envelope:
    """List tables and views visible in the selected schema scope."""
    return _with_services(
        connection,
        lambda introspection_service, _: introspection_service.list_tables(
            schema=schema,
            include_system=include_system,
        ),
    )


@mcp.tool()
def db_list_columns(connection: str, schema: str, table: str = "") -> Envelope:
    """List columns for a table in the allowed schema scope."""
    return _with_services(
        connection,
        lambda introspection_service, _: introspection_service.list_columns(table=table, schema=schema),
    )


@mcp.tool()
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


@mcp.tool()
def db_list_sequences(connection: str, schema: str) -> Envelope:
    """List sequences from schemas allowed by configuration."""
    return _with_services(
        connection,
        lambda introspection_service, _: introspection_service.list_sequences(schema=schema),
    )


@mcp.tool()
def db_list_procedures(connection: str, schema: str) -> Envelope:
    """List stored procedures from schemas allowed by configuration."""
    return _with_services(
        connection,
        lambda introspection_service, _: introspection_service.list_procedures(schema=schema),
    )


@mcp.tool()
def db_list_functions(connection: str, schema: str) -> Envelope:
    """List functions from schemas allowed by configuration."""
    return _with_services(
        connection,
        lambda introspection_service, _: introspection_service.list_functions(schema=schema),
    )


@mcp.tool()
def db_list_jobs(connection: str, schema: str) -> Envelope:
    """List scheduler jobs when supported by the selected database dialect."""
    return _with_services(
        connection,
        lambda introspection_service, _: introspection_service.list_jobs(schema=schema),
    )


@mcp.tool()
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


@mcp.tool()
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


@mcp.tool()
def db_run_select(
    connection: str,
    sql: str = "",
    limit: int | None = None,
    timeout_ms: int | None = None,
    explain: bool = False,
) -> Envelope:
    """Run a guarded read-only SELECT query or return its estimated plan."""
    return _with_services(
        connection,
        lambda _, select_service: select_service.run_select(
            sql_query=sql,
            limit=limit,
            timeout_ms=timeout_ms,
            explain=explain,
        ),
    )


if __name__ == "__main__":
    mcp.run(transport="stdio")
