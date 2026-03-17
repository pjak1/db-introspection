from __future__ import annotations

import time

from src.adapters.base import DatabaseAdapter
from src.config import Settings
from src.contracts import Envelope, success_envelope
from src.services.query_guard import QueryGuard
from src.services._response_helpers import elapsed_ms, error_from_exception


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

    def run_select(
        self,
        sql_query: str,
        limit: int | None,
        timeout_ms: int | None,
        explain: bool = False,
    ) -> Envelope:
        """Run a validated read-only query with bounded result size and timeout."""
        started = time.perf_counter()
        try:
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
                return success_envelope(
                    dialect=self._adapter.dialect,
                    data=result.data,
                    duration_ms=elapsed_ms(started),
                    truncated=result.truncated,
                    warnings=warnings + result.warnings,
                    status="explain",
                )

            guarded = self._guard.prepare_select(
                sql_query=sql_query, limit=limit)
            result = self._adapter.run_select(
                sql_query=guarded.sql,
                timeout_ms=applied_timeout,
            )
            return success_envelope(
                dialect=self._adapter.dialect,
                data=result.data,
                duration_ms=elapsed_ms(started),
                truncated=guarded.truncated or result.truncated,
                warnings=guarded.warnings + result.warnings,
                status=result.status,
            )
        except Exception as err:
            return error_from_exception(self._adapter.dialect, started, err)
