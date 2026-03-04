from __future__ import annotations

from collections.abc import Generator
from uuid import uuid4

import pytest

from src.adapters.base import AdapterResult, DatabaseAdapter
from src.adapters.factory import create_adapter
from src.config import Settings
from src.services.query_guard import QueryGuard


@pytest.fixture
def dynamic_adapter() -> Generator[tuple[str, type[DatabaseAdapter]], None, None]:
    dialect = f"testdb_{uuid4().hex}"

    class DynamicAdapter(DatabaseAdapter):
        dialect_name = None
        dsn_env_var = "TESTDB_DSN"

        def __init__(self, dsn: str):
            self._dsn = dsn

        @property
        def dialect(self) -> str:
            return self.dialect_name

        @classmethod
        def build_dsn(cls, conn_values: dict[str, str], env: dict[str, str]) -> str:
            return f"testdb://{conn_values.get('username', 'user')}"

        @classmethod
        def default_schema(cls, conn_values: dict[str, str]) -> str:
            return "test_schema"

        @classmethod
        def wrap_select(cls, query: str, limit: int) -> str:
            return f"SELECT * FROM ({query}) AS dyn LIMIT {int(limit)}"

        def list_tables(self, schemas: tuple[str, ...], include_system: bool) -> AdapterResult:
            return AdapterResult(data=[])

        def list_columns(self, table: str, schemas: tuple[str, ...]) -> AdapterResult:
            return AdapterResult(data=[])

        def list_constraints(
            self,
            schemas: tuple[str, ...],
            table: str | None = None,
            constraint_type: str | None = None,
        ) -> AdapterResult:
            return AdapterResult(data=[])

        def list_sequences(self, schemas: tuple[str, ...]) -> AdapterResult:
            return AdapterResult(data=[])

        def list_procedures(self, schemas: tuple[str, ...]) -> AdapterResult:
            return AdapterResult(data=[])

        def list_functions(self, schemas: tuple[str, ...]) -> AdapterResult:
            return AdapterResult(data=[])

        def list_jobs(self) -> AdapterResult:
            return AdapterResult(data=[])

        def sample_table(
            self,
            schema: str,
            table: str,
            limit: int,
            order_by: str | None,
        ) -> AdapterResult:
            return AdapterResult(data=[])

        def select_columns(
            self,
            schema: str,
            table: str,
            columns: list[str],
            limit: int,
        ) -> AdapterResult:
            return AdapterResult(data=[])

        def run_select(self, sql_query: str, timeout_ms: int) -> AdapterResult:
            return AdapterResult(data=[{"dsn": self._dsn, "sql_query": sql_query}])

    DynamicAdapter.dialect_name = dialect
    DatabaseAdapter._registry[dialect] = DynamicAdapter

    try:
        yield dialect, DynamicAdapter
    finally:
        DatabaseAdapter._registry.pop(dialect, None)


def test_adapter_class_registers_automatically():
    dialect = "auto_registry_testdb"

    class AutoAdapter(DatabaseAdapter):
        dialect_name = "auto_registry_testdb"

        def __init__(self, dsn: str):
            self._dsn = dsn

        @property
        def dialect(self) -> str:
            return self.dialect_name

        def list_tables(self, schemas: tuple[str, ...], include_system: bool) -> AdapterResult:
            return AdapterResult(data=[])

        def list_columns(self, table: str, schemas: tuple[str, ...]) -> AdapterResult:
            return AdapterResult(data=[])

        def list_constraints(
            self,
            schemas: tuple[str, ...],
            table: str | None = None,
            constraint_type: str | None = None,
        ) -> AdapterResult:
            return AdapterResult(data=[])

        def list_sequences(self, schemas: tuple[str, ...]) -> AdapterResult:
            return AdapterResult(data=[])

        def list_procedures(self, schemas: tuple[str, ...]) -> AdapterResult:
            return AdapterResult(data=[])

        def list_functions(self, schemas: tuple[str, ...]) -> AdapterResult:
            return AdapterResult(data=[])

        def list_jobs(self) -> AdapterResult:
            return AdapterResult(data=[])

        def sample_table(
            self,
            schema: str,
            table: str,
            limit: int,
            order_by: str | None,
        ) -> AdapterResult:
            return AdapterResult(data=[])

        def select_columns(
            self,
            schema: str,
            table: str,
            columns: list[str],
            limit: int,
        ) -> AdapterResult:
            return AdapterResult(data=[])

        def run_select(self, sql_query: str, timeout_ms: int) -> AdapterResult:
            return AdapterResult(data=[])

    try:
        assert DatabaseAdapter.adapter_class_for(dialect) is AutoAdapter
    finally:
        DatabaseAdapter._registry.pop(dialect, None)


def test_settings_resolve_dialect_via_registry(monkeypatch, dynamic_adapter):
    dialect, _ = dynamic_adapter
    monkeypatch.delenv("DB_ALLOWED_SCHEMAS", raising=False)
    settings = Settings.from_connection_values(
        conn_values={
            "dialect": dialect,
            "username": "alice",
        }
    )
    assert settings.db_dialect == dialect
    assert settings.db_dsn == "testdb://alice"
    assert settings.allowed_schemas == ("test_schema",)


def test_query_guard_uses_adapter_wrap_select(dynamic_adapter):
    dialect, _ = dynamic_adapter
    guard = QueryGuard(max_select_limit=200, dialect=dialect)
    result = guard.prepare_select("SELECT 1", limit=7)
    assert result.sql == "SELECT * FROM (SELECT 1) AS dyn LIMIT 7"
    assert result.truncated is False


def test_factory_creates_dynamic_adapter_without_core_changes(dynamic_adapter):
    dialect, adapter_class = dynamic_adapter
    settings = Settings(
        db_dialect=dialect,
        db_dsn="testdb://factory",
        allowed_schemas=("test_schema",),
        default_sample_limit=10,
        max_sample_limit=100,
        max_select_limit=200,
        statement_timeout_ms=5000,
        include_system_schemas=False,
    )
    adapter = create_adapter(settings)
    assert isinstance(adapter, adapter_class)
    assert adapter.dialect == dialect
