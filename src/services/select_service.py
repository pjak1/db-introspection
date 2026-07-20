from __future__ import annotations

from src.adapters.base import DatabaseAdapter
from src.config import Settings
from src.services.export import (
    effective_export_limit,
    normalize_export_format,
    normalize_output_format,
    resolve_export_path,
    serialize_rows,
)
from src.services.response import Ok, service_operation
from src.services.query_guard import QueryGuard


class SelectService:
    """Application service for guarded free-form SELECT execution."""

    def __init__(self, adapter: DatabaseAdapter, settings: Settings):
        """Store dependencies and initialize SQL guard using configured limits."""
        self._adapter = adapter
        self._settings = settings
        self._guard = QueryGuard(
            max_select_limit=settings.max_select_limit,
            dialect=self._adapter.dialect,
        )

    @service_operation
    def run_select(
        self,
        sql_query: str,
        limit: int | None,
        timeout_ms: int | None,
        explain: bool = False,
        output_format: str | None = None,
    ) -> Ok:
        """Run a validated read-only query with bounded result size and timeout."""
        fmt = normalize_output_format(output_format)
        applied_timeout = (
            self._settings.statement_timeout_ms
            if timeout_ms is None
            else max(1, int(timeout_ms))
        )

        if explain:
            validated_sql = self._guard.validate_select(sql_query)
            warnings: list[str] = []
            if limit is not None:
                warnings.append(
                    f"Requested limit {limit} was ignored because explain=True plans the original SQL."
                )
            result = self._adapter.explain_select(
                sql_query=validated_sql,
                timeout_ms=applied_timeout,
            )
            return Ok(result, extra_warnings=tuple(warnings))

        guarded = self._guard.prepare_select(sql_query=sql_query, limit=limit)
        result = self._adapter.run_select(
            sql_query=guarded.sql,
            timeout_ms=applied_timeout,
        )
        warnings = list(guarded.warnings)
        if fmt != "rows":
            original = result.data
            row_count = len(original) if isinstance(original, list) else None
            result.data = serialize_rows(original, fmt)
            suffix = f" ({row_count} rows)." if row_count is not None else "."
            warnings.append(f"Rows serialized as {fmt}{suffix}")
        return Ok(result, truncated=guarded.truncated, extra_warnings=warnings)

    @service_operation
    def export_select(
        self,
        sql_query: str,
        filename: str | None,
        output_format: str | None,
        timeout_ms: int | None,
        max_rows: int | None,
    ) -> Ok:
        """Stream a validated read-only query to a file and return a summary.

        Unlike `run_select`, this path bounds rows by the (much higher)
        `max_export_rows` ceiling and never materializes the result set: rows are
        streamed straight to disk. Only a summary crosses the MCP boundary.
        """
        fmt = normalize_export_format(output_format)
        validated_sql = self._guard.validate_select(sql_query)
        applied_timeout = (
            self._settings.statement_timeout_ms
            if timeout_ms is None
            else max(1, int(timeout_ms))
        )
        effective_max, warnings = effective_export_limit(
            max_rows, self._settings.max_export_rows)
        destination = resolve_export_path(filename, fmt, default_stem="query_export")
        result = self._adapter.export_query(
            sql_query=validated_sql,
            destination=destination,
            fmt=fmt,
            timeout_ms=applied_timeout,
            max_rows=effective_max,
        )
        if isinstance(result.data, dict) and result.data.get("truncated"):
            warnings.append(
                f"Export reached the row cap (max_rows={effective_max}); more rows "
                "may exist. Raise max_rows or the connection's max_export_rows."
            )
        return Ok(result, extra_warnings=warnings)
