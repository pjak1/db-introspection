from __future__ import annotations

from src.adapters.mssql import MssqlAdapter
from src.adapters.oracle import OracleAdapter
from src.adapters.postgres import PostgresAdapter


class _FakeCursor:
    """Minimal DBAPI-ish cursor recording executed statements."""

    def __init__(self, log: list[str]):
        self._log = log
        self.description = [("x",)]

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, query, params=None):  # noqa: ANN001
        self._log.append(str(query))

    def fetchall(self):
        return []


class _FakeConn:
    """Fake connection usable both as a context manager and directly."""

    def __init__(self):
        self.executed: list[str] = []
        self.read_only = None
        self.committed = False
        self.rolled_back = False
        self.closed = False
        self.timeout = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def cursor(self, *args, **kwargs):
        return _FakeCursor(self.executed)

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


def test_postgres_read_path_sets_read_only_transaction():
    adapter = PostgresAdapter("postgresql://unused")
    conn = _FakeConn()
    adapter.open_connection = lambda: conn  # type: ignore[method-assign]

    adapter._fetch_all("SELECT 1")

    # The engine-enforced read-only session must be turned on for reads.
    assert conn.read_only is True


def test_oracle_read_path_issues_set_transaction_read_only_first():
    adapter = OracleAdapter("user/pass@db")
    conn = _FakeConn()
    adapter.open_connection = lambda: conn  # type: ignore[method-assign]

    adapter._fetch_all("SELECT 1 FROM dual")

    assert conn.executed[0] == "SET TRANSACTION READ ONLY"
    assert conn.executed[1] == "SELECT 1 FROM dual"


def test_mssql_read_path_rolls_back_and_never_commits():
    adapter = MssqlAdapter("Driver=test")
    conn = _FakeConn()
    adapter.open_connection = lambda: conn  # type: ignore[method-assign]

    adapter._fetch_all("SELECT 1")

    # No engine read-only mode on SQL Server: the read must be rolled back,
    # never committed, so any side effect is discarded.
    assert conn.rolled_back is True
    assert conn.committed is False
    assert conn.closed is True
