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
    dsn_env_var: ClassVar[str | None] = None
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
    def build_dsn(cls, conn_values: dict[str, str], env: dict[str, str]) -> str:
        """Build a driver-specific DSN from parsed connection and environment values."""
        return ""

    @classmethod
    def default_schema(cls, conn_values: dict[str, str]) -> str:
        """Return the default schema used when none is explicitly requested."""
        return conn_values.get("schema", "public")

    @classmethod
    def wrap_select(cls, query: str, limit: int) -> str:
        """Wrap a SELECT query to enforce a maximum number of returned rows."""
        return f"SELECT * FROM ({query}) AS mcp_subquery LIMIT {int(limit)}"

    @property
    @abstractmethod
    def dialect(self) -> str:
        """Return the adapter dialect identifier."""
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
    ) -> AdapterResult:
        """Return a bounded preview of rows from a single table."""
        raise NotImplementedError

    @abstractmethod
    def select_columns(
        self,
        schema: str,
        table: str,
        columns: list[str],
        limit: int,
    ) -> AdapterResult:
        """Return a bounded projection of selected columns from a single table."""
        raise NotImplementedError

    @abstractmethod
    def run_select(self, sql_query: str, timeout_ms: int) -> AdapterResult:
        """Execute a read-only SQL query with timeout controls."""
        raise NotImplementedError

    @abstractmethod
    def explain_select(self, sql_query: str, timeout_ms: int) -> AdapterResult:
        """Return an estimated execution plan for a validated read-only SQL query."""
        raise NotImplementedError
