from __future__ import annotations

import re

from src.adapters.base import AdapterResult, DatabaseAdapter
from src.config import Settings
from src.errors import DatabaseError, ValidationError
from src.services.export import normalize_output_format, serialize_rows
from src.services.response import Ok, service_operation

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
_EMPTY_TABLE_MESSAGE = "Table name cannot be empty."


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

    @service_operation
    def list_tables(self, schema: str, include_system: bool) -> Ok:
        """List tables/views for the effective schema scope."""
        schema_used = self._require_schema(schema)
        result = self._adapter.list_tables(schemas=(schema_used,), include_system=include_system)
        return Ok(result, schema_used=schema_used)

    @service_operation
    def list_columns(self, table: str, schema: str) -> Ok:
        """List columns for a table constrained to allowed schemas."""
        if not table.strip():
            raise ValidationError("invalid_table", _EMPTY_TABLE_MESSAGE)
        schema_used = self._require_schema(schema)
        result = self._adapter.list_columns(table=table, schemas=(schema_used,))
        return Ok(result, schema_used=schema_used)

    @service_operation
    def list_constraints(
        self,
        schema: str,
        table: str | None,
        constraint_type: str | None,
    ) -> Ok:
        """List constraints with optional schema/table/type filters."""
        schema_used = self._require_schema(schema)
        result = self._adapter.list_constraints(
            schemas=(schema_used,),
            table=table,
            constraint_type=constraint_type,
        )
        return Ok(result, schema_used=schema_used)

    @service_operation
    def list_indexes(self, schema: str, table: str | None) -> Ok:
        """List indexes for the effective schema scope, optionally filtered by table."""
        schema_used = self._require_schema(schema)
        normalized_table = table.strip() if isinstance(table, str) and table.strip() else None
        result = self._adapter.list_indexes(schemas=(schema_used,), table=normalized_table)
        return Ok(result, schema_used=schema_used)

    @service_operation
    def get_ddl(self, schema: str, object_name: str, object_type: str) -> Ok:
        """Return the DDL of a database object within the allowed schema scope."""
        schema_used = self._require_schema(schema)
        normalized_name = object_name.strip() if isinstance(object_name, str) else ""
        if not normalized_name:
            raise ValidationError("invalid_object_name", "object_name cannot be empty.")
        normalized_type = object_type.strip().lower() if isinstance(object_type, str) else ""
        allowed_types = self._adapter.ddl_object_types
        if normalized_type not in allowed_types:
            raise ValidationError(
                "invalid_object_type",
                f"object_type must be one of: {', '.join(allowed_types)}.",
                details={"allowed_object_types": list(allowed_types)},
            )
        result = self._adapter.get_ddl(
            schema=schema_used,
            object_name=normalized_name,
            object_type=normalized_type,
        )
        return Ok(result, schema_used=schema_used)

    def _normalize_object_types(self, object_types: list[str] | None) -> tuple[str, ...]:
        """Validate requested search object types against adapter-supported types."""
        supported = self._adapter.searchable_object_types
        if not object_types:
            return tuple(supported)
        requested: list[str] = []
        for item in object_types:
            if not isinstance(item, str):
                raise ValidationError("invalid_object_type", "object_types must be strings.")
            normalized = item.strip().lower()
            if normalized not in supported:
                raise ValidationError(
                    "invalid_object_type",
                    f"Unsupported object_type '{item}'.",
                    details={"supported_object_types": list(supported)},
                )
            if normalized not in requested:
                requested.append(normalized)
        return tuple(requested)

    @service_operation
    def search_objects(
        self,
        schema: str,
        pattern: str,
        object_types: list[str] | None,
    ) -> Ok:
        """Search objects by name substring within the allowed schema scope."""
        schema_used = self._require_schema(schema)
        normalized_pattern = pattern.strip() if isinstance(pattern, str) else ""
        if not normalized_pattern:
            raise ValidationError("invalid_pattern", "Search pattern cannot be empty.")
        effective_types = self._normalize_object_types(object_types)
        result = self._adapter.search_objects(
            schemas=(schema_used,),
            pattern=normalized_pattern,
            object_types=effective_types,
        )
        return Ok(result, schema_used=schema_used)

    @service_operation
    def list_sequences(self, schema: str) -> Ok:
        """List sequences for the effective schema scope."""
        schema_used = self._require_schema(schema)
        result = self._adapter.list_sequences(schemas=(schema_used,))
        return Ok(result, schema_used=schema_used)

    @service_operation
    def list_procedures(self, schema: str) -> Ok:
        """List procedures for the effective schema scope."""
        schema_used = self._require_schema(schema)
        result = self._adapter.list_procedures(schemas=(schema_used,))
        return Ok(result, schema_used=schema_used)

    @service_operation
    def list_functions(self, schema: str) -> Ok:
        """List functions for the effective schema scope."""
        schema_used = self._require_schema(schema)
        result = self._adapter.list_functions(schemas=(schema_used,))
        return Ok(result, schema_used=schema_used)

    @service_operation
    def list_jobs(self, schema: str) -> Ok:
        """List scheduler jobs, translating DB failures to validation-style errors."""
        schema_used = self._require_schema(schema)
        try:
            result = self._adapter.list_jobs()
        except DatabaseError as err:
            raise ValidationError(err.code, err.message, err.details) from err
        return Ok(result, schema_used=schema_used)

    @service_operation
    def table_stats(self, schema: str, table: str) -> Ok:
        """Return row-count estimate and size statistics for one table."""
        if not table.strip():
            raise ValidationError("invalid_table", _EMPTY_TABLE_MESSAGE)
        schema_used = self._require_schema(schema)
        result = self._adapter.table_stats(schema=schema_used, table=table.strip())
        return Ok(result, schema_used=schema_used)

    @service_operation
    def list_foreign_keys(self, schema: str, table: str | None) -> Ok:
        """List foreign-key edges for the effective schema scope, optionally by table."""
        schema_used = self._require_schema(schema)
        normalized_table = table.strip() if isinstance(table, str) and table.strip() else None
        result = self._adapter.list_foreign_keys(schemas=(schema_used,), table=normalized_table)
        return Ok(result, schema_used=schema_used)

    @service_operation
    def top_queries(self, limit: int | None) -> Ok:
        """Return the most resource-intensive queries known to the engine."""
        effective_limit = 20 if limit is None else max(1, min(int(limit), 200))
        result = self._adapter.top_queries(limit=effective_limit)
        return Ok(result)

    @service_operation
    def health_check(self) -> Ok:
        """Return engine health-check rows (each check degrades independently)."""
        return Ok(self._adapter.health_check())

    @staticmethod
    def _apply_output_format(result: AdapterResult, fmt: str, warnings: list[str]) -> None:
        """Serialize result rows to `fmt` in place, recording a warning note."""
        if fmt == "rows":
            return
        original = result.data
        row_count = len(original) if isinstance(original, list) else None
        result.data = serialize_rows(original, fmt)
        suffix = f" ({row_count} rows)." if row_count is not None else "."
        warnings.append(f"Rows serialized as {fmt}{suffix}")

    @staticmethod
    def _normalize_offset(offset: int | None) -> int:
        """Clamp a requested pagination offset to a non-negative integer."""
        return max(0, int(offset)) if offset is not None else 0

    @service_operation
    def sample_table(
        self,
        table: str,
        schema: str,
        limit: int | None,
        order_by: str | None,
        offset: int | None = None,
        output_format: str | None = None,
    ) -> Ok:
        """Return a bounded row sample from one table."""
        if not table.strip():
            raise ValidationError("invalid_table", _EMPTY_TABLE_MESSAGE)
        schema_used = self._require_schema(schema)
        fmt = normalize_output_format(output_format)
        applied_limit, truncated, warnings = self._resolve_sample_limit(limit)
        normalized_offset = self._normalize_offset(offset)
        result = self._adapter.sample_table(
            schema=schema_used,
            table=table,
            limit=applied_limit,
            order_by=order_by,
            offset=normalized_offset,
        )
        if normalized_offset > 0 and not (order_by and order_by.strip()):
            warnings.append(
                "Pagination offset was applied without order_by; row order is not stable across pages."
            )
        self._apply_output_format(result, fmt, warnings)
        return Ok(result, schema_used=schema_used, truncated=truncated, extra_warnings=warnings)

    @service_operation
    def select_columns(
        self,
        table: str,
        columns: list[str],
        schema: str,
        limit: int | None,
        offset: int | None = None,
        output_format: str | None = None,
    ) -> Ok:
        """Return a bounded row sample projected to requested columns."""
        if not table.strip():
            raise ValidationError("invalid_table", _EMPTY_TABLE_MESSAGE)
        if not columns:
            raise ValidationError("invalid_columns", "At least one column must be provided.")

        normalized_columns: list[str] = []
        seen_columns: set[str] = set()
        for column in columns:
            if not isinstance(column, str):
                raise ValidationError("invalid_columns", "Column names must be strings.")
            normalized = column.strip()
            if not normalized:
                raise ValidationError("invalid_columns", "Column names cannot be empty.")
            if not _IDENTIFIER_RE.match(normalized):
                raise ValidationError("invalid_columns", f"Invalid column identifier: {normalized}")
            key = normalized.lower()
            if key not in seen_columns:
                # Keep first occurrence order while removing case-insensitive duplicates.
                seen_columns.add(key)
                normalized_columns.append(normalized)

        schema_used = self._require_schema(schema)
        fmt = normalize_output_format(output_format)
        applied_limit, truncated, warnings = self._resolve_sample_limit(limit)
        normalized_offset = self._normalize_offset(offset)
        result = self._adapter.select_columns(
            schema=schema_used,
            table=table,
            columns=normalized_columns,
            limit=applied_limit,
            offset=normalized_offset,
        )
        if normalized_offset > 0:
            warnings.append(
                "Pagination offset was applied without an ordering; row order is not stable across pages."
            )
        self._apply_output_format(result, fmt, warnings)
        return Ok(result, schema_used=schema_used, truncated=truncated, extra_warnings=warnings)
