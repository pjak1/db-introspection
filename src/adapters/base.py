from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, ClassVar


@dataclass
class AdapterResult:
    """Standardized adapter payload returned to higher-level services."""
    data: Any
    warnings: list[str] = field(default_factory=list)
    truncated: bool = False
    schema_used: str | None = None
    status: str | None = None


class DatabaseAdapter(ABC):
    """Abstract contract for DB-specific metadata and query operations."""
    dialect_name: ClassVar[str | None] = None
    _registry: ClassVar[dict[str, type["DatabaseAdapter"]]] = {}

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Auto-register subclasses by normalized dialect name."""
        super().__init_subclass__(**kwargs)
        dialect_name = getattr(cls, "dialect_name", None)
        if not dialect_name:
            return
        normalized = str(dialect_name).strip().lower()
        if normalized:
            DatabaseAdapter._registry[normalized] = cls

    @classmethod
    def registered_adapters(cls) -> dict[str, type["DatabaseAdapter"]]:
        """Return a copy of all registered adapter classes."""
        return dict(DatabaseAdapter._registry)

    @classmethod
    def adapter_class_for(cls, dialect: str) -> type["DatabaseAdapter"] | None:
        """Return the adapter class for a dialect, if registered."""
        normalized = (dialect or "").strip().lower()
        if not normalized:
            return None
        return DatabaseAdapter._registry.get(normalized)

    @classmethod
    def build_dsn(cls, conn_values: dict[str, str]) -> str:
        """Build a driver-specific DSN from parsed connection-file values.

        Secrets are already resolved into `conn_values` via `${VAR}` expansion
        when the file is read, so no environment access is needed here.
        """
        return ""

    @classmethod
    def default_schema(cls, conn_values: dict[str, str]) -> str:
        """Return the default schema used when none is explicitly requested."""
        return conn_values.get("schema", "public")

    @classmethod
    def wrap_select(cls, query: str, limit: int) -> str:
        """Wrap a SELECT query to enforce a maximum number of returned rows."""
        return f"SELECT * FROM ({query}) AS mcp_subquery LIMIT {int(limit)}"

    # Object types understood by search_objects across all dialects.
    searchable_object_types: ClassVar[tuple[str, ...]] = (
        "table",
        "view",
        "sequence",
        "procedure",
        "function",
    )
    # Object types understood by get_ddl across all dialects.
    ddl_object_types: ClassVar[tuple[str, ...]] = (
        "table",
        "view",
        "procedure",
        "function",
    )

    @abstractmethod
    def list_indexes(self, schemas: tuple[str, ...], table: str | None = None) -> AdapterResult:
        """List indexes for the given schema scope, optionally filtered by table."""
        raise NotImplementedError

    @abstractmethod
    def get_ddl(self, schema: str, object_name: str, object_type: str) -> AdapterResult:
        """Return the DDL/source of a database object."""
        raise NotImplementedError

    @abstractmethod
    def search_objects(
        self,
        schemas: tuple[str, ...],
        pattern: str,
        object_types: tuple[str, ...],
    ) -> AdapterResult:
        """Search objects by name substring within the given schema scope."""
        raise NotImplementedError

    @property
    @abstractmethod
    def dialect(self) -> str:
        """Return the adapter dialect identifier."""
        raise NotImplementedError

    @abstractmethod
    def open_connection(self) -> Any:
        """Open a new read/write-capable DBAPI connection (implemented per dialect)."""
        raise NotImplementedError

    @abstractmethod
    def list_tables(self, schemas: tuple[str, ...], include_system: bool) -> AdapterResult:
        """List tables/views for the given schema scope."""
        raise NotImplementedError

    @abstractmethod
    def list_columns(self, table: str, schemas: tuple[str, ...]) -> AdapterResult:
        """List columns for a table within the given schema scope."""
        raise NotImplementedError

    @abstractmethod
    def list_constraints(
        self,
        schemas: tuple[str, ...],
        table: str | None = None,
        constraint_type: str | None = None,
    ) -> AdapterResult:
        """List constraints optionally filtered by table and constraint type."""
        raise NotImplementedError

    @abstractmethod
    def list_sequences(self, schemas: tuple[str, ...]) -> AdapterResult:
        """List sequences for the given schema scope."""
        raise NotImplementedError

    @abstractmethod
    def list_procedures(self, schemas: tuple[str, ...]) -> AdapterResult:
        """List procedures for the given schema scope."""
        raise NotImplementedError

    @abstractmethod
    def list_functions(self, schemas: tuple[str, ...]) -> AdapterResult:
        """List functions for the given schema scope."""
        raise NotImplementedError

    @abstractmethod
    def list_jobs(self) -> AdapterResult:
        """List scheduler jobs if supported by the target database."""
        raise NotImplementedError

    @abstractmethod
    def sample_table(
        self,
        schema: str,
        table: str,
        limit: int,
        order_by: str | None,
        offset: int = 0,
    ) -> AdapterResult:
        """Return a bounded preview of rows from a single table.

        `offset` skips that many leading rows for pagination (0 = first page).
        """
        raise NotImplementedError

    @abstractmethod
    def select_columns(
        self,
        schema: str,
        table: str,
        columns: list[str],
        limit: int,
        offset: int = 0,
    ) -> AdapterResult:
        """Return a bounded projection of selected columns from a single table.

        `offset` skips that many leading rows for pagination (0 = first page).
        """
        raise NotImplementedError

    @abstractmethod
    def run_select(self, sql_query: str, timeout_ms: int) -> AdapterResult:
        """Execute a read-only SQL query with timeout controls."""
        raise NotImplementedError

    @abstractmethod
    def explain_select(self, sql_query: str, timeout_ms: int) -> AdapterResult:
        """Return an estimated execution plan for a validated read-only SQL query."""
        raise NotImplementedError

    @abstractmethod
    def table_stats(self, schema: str, table: str) -> AdapterResult:
        """Return size/row-count statistics for a single table.

        Row shape: schema, table, row_estimate (from catalog statistics, may be
        stale), table_bytes, index_bytes, total_bytes, column_count,
        last_analyzed. Byte columns may be None when the connection lacks access
        to the size catalogs (degrade with a warning, never fail).
        """
        raise NotImplementedError

    @abstractmethod
    def list_foreign_keys(self, schemas: tuple[str, ...], table: str | None = None) -> AdapterResult:
        """List foreign-key edges within the schema scope.

        Row shape: constraint_name, schema, table, columns, ref_schema,
        ref_table, ref_columns, on_delete, on_update. When `table` is given it
        matches either side (referencing or referenced), so callers can ask both
        "what does X reference?" and "what references X?".
        """
        raise NotImplementedError

    @abstractmethod
    def top_queries(self, limit: int) -> AdapterResult:
        """Return the most resource-intensive queries known to the engine.

        Privilege/extension sensitive (pg_stat_statements / v$sqlstats /
        sys.dm_exec_query_stats); degrade with a warning when unavailable.
        """
        raise NotImplementedError

    @abstractmethod
    def health_check(self) -> AdapterResult:
        """Return a list of health-check rows: check, status, detail.

        Each check degrades independently to status 'unknown' with a detail note
        when the required catalog/DMV access is missing.
        """
        raise NotImplementedError
