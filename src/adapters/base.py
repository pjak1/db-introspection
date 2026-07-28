from __future__ import annotations

import threading
from abc import ABC, abstractmethod
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
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

    def __init__(self) -> None:
        """Initialize the per-call connection holder.

        Subclasses that define their own `__init__` must call `super().__init__()`.
        """
        # Thread-local because the registry caches one adapter instance per
        # connection key and shares it across every tool call
        # (`ConnectionRegistry._cache`). Today's dispatch is serial — FastMCP
        # calls sync tool functions inline on the event-loop thread — so this is
        # not strictly required; it is cheap insurance if that ever changes.
        self._session_local = threading.local()

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

    @contextmanager
    def session(self) -> Iterator[Any]:
        """Hold a single connection for the duration of one logical operation.

        Reentrant: a nested `session()` yields the connection already open, so a
        method that fans out into several `_fetch_all` calls pays exactly one
        login instead of one per query. The connection is always closed when the
        outermost block exits — this is deliberately not a pool, so the read-only
        guarantee, the absence of leftover session state and the absence of idle
        connections all keep holding for free.
        """
        borrowed = getattr(self._session_local, "conn", None)
        if borrowed is not None:
            try:
                yield borrowed
            except BaseException:
                # The session outlives this query, so it must be put back into a
                # usable state — otherwise every later query in the same call
                # fails too (PostgreSQL aborts the whole transaction on error).
                self._safe_recover(borrowed)
                raise
            return

        conn = self.open_connection()
        self._session_local.conn = conn
        try:
            self._begin_session(conn)
            yield conn
        finally:
            self._session_local.conn = None
            self._end_session(conn)

    def _safe_recover(self, conn: Any) -> None:
        """Recover the session best-effort; the original error always wins."""
        try:
            self._recover_after_error(conn)
        except Exception:
            pass

    def _begin_session(self, conn: Any) -> None:
        """Apply the dialect's read-only setup, exactly once per session."""

    def _end_session(self, conn: Any) -> None:
        """Tear the session down; must close the connection whatever happens."""
        conn.close()

    def _recover_after_error(self, conn: Any) -> None:
        """Return the session to a state where the next query can still run."""

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
