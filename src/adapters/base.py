from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar

from src.adapters.session import ConnectionSession, SessionPolicy


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
    # None until `session()` builds it, then shadowed per instance. Declared here
    # so the contract needs no __init__ of its own.
    _connection_session: ConnectionSession | None = None

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
        """List indexes for the given schema scope, optionally filtered by table.

        Row shape: schema, table_name, index_name, is_unique, is_primary,
        index_type, columns, included_columns. `columns` holds the key columns in
        key order and nothing else; `included_columns` holds the INCLUDE/covering
        columns, or NULL on dialects without that concept. Keeping them apart is
        what makes the result usable for covering-index decisions. Dialects add
        their own extras on top (clustering_factor, filter_definition, is_valid…).
        """
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
    def session_policy(self) -> SessionPolicy:
        """Return this dialect's session setup and teardown policy.

        The connection lifecycle itself lives in `ConnectionSession`; this is the
        one dialect-specific part of it, so it is the only part the contract asks
        an adapter for.
        """
        raise NotImplementedError

    def session(self) -> AbstractContextManager[Any]:
        """Hold one connection for one logical operation; see `ConnectionSession`.

        Reentrant, so a method that fans out into several queries pays one login
        instead of one per query.
        """
        if self._connection_session is None:
            # Built on first use rather than in an __init__: the policy may need
            # adapter state that a subclass sets up in its own constructor, and
            # this way no adapter has to remember to call super().__init__().
            # Assigning here shadows the class attribute with an instance one.
            self._connection_session = ConnectionSession(
                connect=lambda: self.open_connection(),
                policy=self.session_policy(),
            )
        return self._connection_session.hold()

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
    def index_usage(
        self,
        schemas: tuple[str, ...],
        table: str | None = None,
        include_fragmentation: bool = False,
    ) -> AdapterResult:
        """Return read/write counters per index for the given schema scope.

        Row shape: schema, table, index_name, is_unique, reads, writes_overhead,
        last_used, size_bytes, never_used, stats_since. `stats_since` is part of
        the answer, not an extra: every engine resets these counters (restart,
        failover, index drop/recreate), so `never_used` only means "not used
        since then" — dropping an index on a window that misses a monthly job is
        exactly the mistake this column prevents.

        The counters live in engine-specific catalogs that a dialect may not
        expose at all; where that is the case, return empty data with
        `status='not_available'` and a warning naming the missing grant rather
        than raising.
        """
        raise NotImplementedError

    @abstractmethod
    def column_stats(
        self,
        schema: str,
        table: str,
        column: str | None = None,
        include_histogram: bool = False,
    ) -> AdapterResult:
        """Return optimizer statistics per column for one table.

        Row shape: schema, table, column, kind, distinct_estimate,
        null_fraction, avg_width, last_analyzed, source. `kind` is 'column' for
        a single column and 'extended' for a multi-column (correlated) statistic
        — the latter is what stops the optimizer from multiplying selectivities
        of related predicates and estimating one row where there are thousands.
        Columns the dialect cannot express are None rather than faked; dialects
        add their own extras.
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

    @abstractmethod
    def export_query(
        self,
        sql_query: str,
        destination: Path,
        fmt: str,
        timeout_ms: int,
        max_rows: int,
    ) -> AdapterResult:
        """Stream a validated read-only SELECT to `destination` in `fmt`.

        Rows are fetched in batches and written straight to disk so the full
        result set never sits in memory. At most `max_rows` rows are written; the
        query is capped at `max_rows + 1` so an extra surviving row marks the
        export as truncated. `data` is a summary dict:
        {path, format, row_count, byte_size, truncated}.
        """
        raise NotImplementedError

    @abstractmethod
    def export_table(
        self,
        schema: str,
        table: str,
        columns: list[str] | None,
        order_by: str | None,
        destination: Path,
        fmt: str,
        timeout_ms: int,
        max_rows: int,
    ) -> AdapterResult:
        """Stream a single table (optionally projected/ordered) to `destination`.

        Builds a dialect-correct SELECT and delegates to `export_query`. Same
        summary payload and truncation semantics.
        """
        raise NotImplementedError
