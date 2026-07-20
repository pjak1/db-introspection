from __future__ import annotations

from src.adapters.base import DatabaseAdapter
from src.config import Settings
from src.services.export import normalize_output_format, serialize_rows
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
