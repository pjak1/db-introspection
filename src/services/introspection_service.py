from __future__ import annotations

import re
import time

from src.adapters.base import DatabaseAdapter
from src.config import Settings
from src.contracts import Envelope, success_envelope
from src.errors import DatabaseError, ValidationError
from src.services._response_helpers import elapsed_ms, error_from_exception

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


class IntrospectionService:
    """Application service for metadata tools and table-level data preview."""

    def __init__(self, adapter: DatabaseAdapter, settings: Settings):
        """Store adapter and validated settings for introspection operations."""
        self._adapter = adapter
        self._settings = settings

    def _require_schema(self, schema: str) -> str:
        """Validate and normalize required schema name."""
        allowed = set(self._settings.allowed_schemas)
        normalized_schema = schema.strip() if isinstance(schema, str) else ""
        if not normalized_schema:
            raise ValidationError(
                "missing_schema",
                "Schema is required.",
                details={"allowed_schemas": sorted(allowed)},
            )
        if normalized_schema not in allowed:
            raise ValidationError(
                "invalid_schema",
                f"Schema '{normalized_schema}' is not in allowed schemas.",
                details={"allowed_schemas": sorted(allowed)},
            )
        return normalized_schema

    def _resolve_sample_limit(self, requested: int | None) -> tuple[int, bool, list[str]]:
        """Apply configured sample limit caps and produce truncation warnings."""
        if requested is None:
            return self._settings.default_sample_limit, False, []
        requested_limit = max(1, int(requested))
        applied = min(requested_limit, self._settings.max_sample_limit)
        truncated = requested_limit > self._settings.max_sample_limit
        warnings: list[str] = []
        if truncated:
            warnings.append(
                f"Requested sample limit {requested_limit} was reduced to {self._settings.max_sample_limit}."
            )
        return applied, truncated, warnings

    def list_tables(self, schema: str, include_system: bool) -> Envelope:
        """List tables/views for the effective schema scope."""
        started = time.perf_counter()
        try:
            schema_used = self._require_schema(schema)
            result = self._adapter.list_tables(
                schemas=(schema_used,), include_system=include_system)
            return success_envelope(
                dialect=self._adapter.dialect,
                data=result.data,
                duration_ms=elapsed_ms(started),
                truncated=result.truncated,
                schema_used=schema_used,
                warnings=result.warnings,
                status=result.status,
            )
        except Exception as err:
            return error_from_exception(self._adapter.dialect, started, err)

    def list_columns(self, table: str, schema: str) -> Envelope:
        """List columns for a table constrained to allowed schemas."""
        started = time.perf_counter()
        try:
            if not table.strip():
                raise ValidationError(
                    "invalid_table", "Table name cannot be empty.")
            schema_used = self._require_schema(schema)
            result = self._adapter.list_columns(
                table=table, schemas=(schema_used,))
            return success_envelope(
                dialect=self._adapter.dialect,
                data=result.data,
                duration_ms=elapsed_ms(started),
                schema_used=schema_used,
                warnings=result.warnings,
                status=result.status,
            )
        except Exception as err:
            return error_from_exception(self._adapter.dialect, started, err)

    def list_constraints(
        self,
        schema: str,
        table: str | None,
        constraint_type: str | None,
    ) -> Envelope:
        """List constraints with optional schema/table/type filters."""
        started = time.perf_counter()
        try:
            schema_used = self._require_schema(schema)
            result = self._adapter.list_constraints(
                schemas=(schema_used,),
                table=table,
                constraint_type=constraint_type,
            )
            return success_envelope(
                dialect=self._adapter.dialect,
                data=result.data,
                duration_ms=elapsed_ms(started),
                schema_used=schema_used,
                warnings=result.warnings,
                status=result.status,
            )
        except Exception as err:
            return error_from_exception(self._adapter.dialect, started, err)

    def list_sequences(self, schema: str) -> Envelope:
        """List sequences for the effective schema scope."""
        started = time.perf_counter()
        try:
            schema_used = self._require_schema(schema)
            result = self._adapter.list_sequences(schemas=(schema_used,))
            return success_envelope(
                dialect=self._adapter.dialect,
                data=result.data,
                duration_ms=elapsed_ms(started),
                schema_used=schema_used,
                warnings=result.warnings,
                status=result.status,
            )
        except Exception as err:
            return error_from_exception(self._adapter.dialect, started, err)

    def list_procedures(self, schema: str) -> Envelope:
        """List procedures for the effective schema scope."""
        started = time.perf_counter()
        try:
            schema_used = self._require_schema(schema)
            result = self._adapter.list_procedures(schemas=(schema_used,))
            return success_envelope(
                dialect=self._adapter.dialect,
                data=result.data,
                duration_ms=elapsed_ms(started),
                schema_used=schema_used,
                warnings=result.warnings,
                status=result.status,
            )
        except Exception as err:
            return error_from_exception(self._adapter.dialect, started, err)

    def list_functions(self, schema: str) -> Envelope:
        """List functions for the effective schema scope."""
        started = time.perf_counter()
        try:
            schema_used = self._require_schema(schema)
            result = self._adapter.list_functions(schemas=(schema_used,))
            return success_envelope(
                dialect=self._adapter.dialect,
                data=result.data,
                duration_ms=int((time.perf_counter() - started) * 1000),
                schema_used=schema_used,
                warnings=result.warnings,
                status=result.status,
            )
        except Exception as err:
            return error_from_exception(self._adapter.dialect, started, err)

    def list_jobs(self, schema: str) -> Envelope:
        """List scheduler jobs, translating DB failures to validation-style errors."""
        started = time.perf_counter()
        try:
            schema_used = self._require_schema(schema)
            result = self._adapter.list_jobs()
            return success_envelope(
                dialect=self._adapter.dialect,
                data=result.data,
                duration_ms=elapsed_ms(started),
                schema_used=schema_used,
                warnings=result.warnings,
                status=result.status,
            )
        except Exception as err:
            if isinstance(err, DatabaseError):
                err = ValidationError(err.code, err.message, err.details)
            return error_from_exception(self._adapter.dialect, started, err)

    def sample_table(
        self,
        table: str,
        schema: str,
        limit: int | None,
        order_by: str | None,
    ) -> Envelope:
        """Return a bounded row sample from one table."""
        started = time.perf_counter()
        try:
            if not table.strip():
                raise ValidationError(
                    "invalid_table", "Table name cannot be empty.")

            schema_used = self._require_schema(schema)
            applied_limit, truncated, warnings = self._resolve_sample_limit(
                limit)
            result = self._adapter.sample_table(
                schema=schema_used,
                table=table,
                limit=applied_limit,
                order_by=order_by,
            )
            return success_envelope(
                dialect=self._adapter.dialect,
                data=result.data,
                duration_ms=elapsed_ms(started),
                truncated=truncated or result.truncated,
                schema_used=schema_used,
                warnings=warnings + result.warnings,
                status=result.status,
            )
        except Exception as err:
            return error_from_exception(self._adapter.dialect, started, err)

    def select_columns(
        self,
        table: str,
        columns: list[str],
        schema: str,
        limit: int | None,
    ) -> Envelope:
        """Return a bounded row sample projected to requested columns."""
        started = time.perf_counter()
        try:
            if not table.strip():
                raise ValidationError(
                    "invalid_table", "Table name cannot be empty.")

            if not columns:
                raise ValidationError(
                    "invalid_columns", "At least one column must be provided.")

            normalized_columns: list[str] = []
            seen_columns: set[str] = set()
            for column in columns:
                if not isinstance(column, str):
                    raise ValidationError(
                        "invalid_columns", "Column names must be strings.")
                normalized = column.strip()
                if not normalized:
                    raise ValidationError(
                        "invalid_columns", "Column names cannot be empty.")
                if not _IDENTIFIER_RE.match(normalized):
                    raise ValidationError(
                        "invalid_columns",
                        f"Invalid column identifier: {normalized}",
                    )
                key = normalized.lower()
                if key not in seen_columns:
                    # Keep first occurrence order while removing case-insensitive duplicates.
                    seen_columns.add(key)
                    normalized_columns.append(normalized)

            schema_used = self._require_schema(schema)
            applied_limit, truncated, warnings = self._resolve_sample_limit(
                limit)
            result = self._adapter.select_columns(
                schema=schema_used,
                table=table,
                columns=normalized_columns,
                limit=applied_limit,
            )
            return success_envelope(
                dialect=self._adapter.dialect,
                data=result.data,
                duration_ms=elapsed_ms(started),
                truncated=truncated or result.truncated,
                schema_used=schema_used,
                warnings=warnings + result.warnings,
                status=result.status,
            )
        except Exception as err:
            return error_from_exception(self._adapter.dialect, started, err)
