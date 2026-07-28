from __future__ import annotations

from pathlib import Path

import pytest

from src.adapters.base import AdapterResult, DatabaseAdapter
from src.config import Settings


class BaseStubAdapter(DatabaseAdapter):
    """A DatabaseAdapter with every abstract method pre-implemented as a no-op.

    Tests subclass this and override only the one or two methods they exercise,
    instead of re-declaring the whole ABC surface. `dialect_name` stays None so
    the stub never registers itself in the adapter registry.
    """

    _dialect = "postgres"

    @property
    def dialect(self) -> str:
        return self._dialect

    def open_connection(self):
        raise NotImplementedError

    def list_tables(self, schemas, include_system) -> AdapterResult:
        return AdapterResult(data=[])

    def list_columns(self, table, schemas) -> AdapterResult:
        return AdapterResult(data=[])

    def list_constraints(self, schemas, table=None, constraint_type=None) -> AdapterResult:
        return AdapterResult(data=[])

    def list_sequences(self, schemas) -> AdapterResult:
        return AdapterResult(data=[])

    def list_procedures(self, schemas) -> AdapterResult:
        return AdapterResult(data=[])

    def list_functions(self, schemas) -> AdapterResult:
        return AdapterResult(data=[])

    def list_jobs(self) -> AdapterResult:
        return AdapterResult(data=[])

    def sample_table(self, schema, table, limit, order_by, offset=0) -> AdapterResult:
        return AdapterResult(data=[])

    def select_columns(self, schema, table, columns, limit, offset=0) -> AdapterResult:
        return AdapterResult(data=[])

    def list_indexes(self, schemas, table=None) -> AdapterResult:
        return AdapterResult(data=[])

    def get_ddl(self, schema, object_name, object_type) -> AdapterResult:
        return AdapterResult(data=[])

    def search_objects(self, schemas, pattern, object_types) -> AdapterResult:
        return AdapterResult(data=[])

    def run_select(self, sql_query, timeout_ms) -> AdapterResult:
        return AdapterResult(data=[])

    def explain_select(self, sql_query, timeout_ms) -> AdapterResult:
        return AdapterResult(data=[])

    def table_stats(self, schema, table) -> AdapterResult:
        return AdapterResult(data=[])

    def list_foreign_keys(self, schemas, table=None) -> AdapterResult:
        return AdapterResult(data=[])

    def top_queries(self, limit) -> AdapterResult:
        return AdapterResult(data=[])

    def health_check(self) -> AdapterResult:
        return AdapterResult(data=[])

    def export_query(self, sql_query, destination, fmt, timeout_ms, max_rows) -> AdapterResult:
        return AdapterResult(data={
            "path": str(destination), "format": fmt,
            "row_count": 0, "byte_size": 0, "truncated": False,
        })

    def export_table(
        self, schema, table, columns, order_by, destination, fmt, timeout_ms, max_rows
    ) -> AdapterResult:
        return AdapterResult(data={
            "path": str(destination), "format": fmt,
            "row_count": 0, "byte_size": 0, "truncated": False,
        }, schema_used=schema)


class RecordingConnection:
    """A connection fake that records what a session did to it.

    Enough surface for `DatabaseAdapter.session()`: the dialect hooks call
    `rollback()`/`close()`, Oracle's `_begin_session` runs a cursor statement and
    Postgres' sets `read_only`. Tests use `executed` to assert how many times the
    read-only setup ran, which is how "one connection per tool call" is verified.
    """

    def __init__(self) -> None:
        self.executed: list[str] = []
        self.read_only: bool | None = None
        self.committed = False
        self.rolled_back = False
        self.closed = False
        self.call_timeout: int | None = None
        self.timeout: int | None = None

    def cursor(self, *args, **kwargs):  # noqa: ANN002, ANN003
        return _RecordingCursor(self)

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True

    def close(self) -> None:
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, *exc_info) -> bool:
        return False


class _RecordingCursor:
    """Minimal cursor that appends every executed statement to its connection."""

    def __init__(self, conn: RecordingConnection) -> None:
        self._conn = conn
        self.description = None

    def execute(self, query, params=None):  # noqa: ANN001
        self._conn.executed.append(str(query))

    def fetchall(self):
        return []

    def __enter__(self):
        return self

    def __exit__(self, *exc_info) -> bool:
        return False


def stub_session(adapter) -> RecordingConnection:  # noqa: ANN001
    """Give `adapter` a fake connection so `session()` never reaches a database.

    Needed by tests that patch `_fetch_all`: methods which fan out into several
    queries now open the session themselves, above that seam. Returns the shared
    connection so a test can assert on it.
    """
    conn = RecordingConnection()
    adapter.open_connection = lambda: conn  # type: ignore[method-assign]
    return conn


_SETTINGS_DEFAULTS = dict(
    db_dialect="postgres",
    db_dsn="postgresql://user:pass@localhost:5432/db",
    allowed_schemas=("public",),
    default_sample_limit=10,
    max_sample_limit=100,
    max_select_limit=200,
    max_export_rows=1_000_000,
    statement_timeout_ms=5000,
    include_system_schemas=False,
)


def make_settings(**overrides) -> Settings:
    """Build a Settings with test defaults; pass keyword overrides per field."""
    return Settings(**{**_SETTINGS_DEFAULTS, **overrides})


@pytest.fixture
def settings() -> Settings:
    """Default single-schema postgres Settings."""
    return make_settings()


def write_conn_file(path: Path, **overrides) -> Path:
    """Write a minimal postgres `db_conn.txt` at `path`; override any field."""
    values = {
        "dialect": "postgres",
        "host": "localhost",
        "db_name": "test_db",
        "port": "5432",
        "username": "test_user",
        "password": "test_pass",
        "schema": "public",
    }
    values.update(overrides)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(f"{k}:{v}" for k, v in values.items()), encoding="utf-8")
    return path
