"""Lock in the "one connection per tool call" property.

Adapters used to open a connection inside `_fetch_all`, so a tool that fans out
into several queries paid one login per query. These tests assert the fan-out is
gone; nothing else in the suite can tell the difference, because the observable
output never changed.
"""
from __future__ import annotations

import psycopg
import pytest
from conftest import RecordingConnection

from src.adapters.mssql import MssqlAdapter
from src.adapters.oracle import OracleAdapter
from src.adapters.postgres import PostgresAdapter
from src.errors import DatabaseError


class _CountingConnectionFactory:
    """Hand out recording connections and count how many were opened."""

    def __init__(self) -> None:
        self.opened: list[RecordingConnection] = []

    def __call__(self) -> RecordingConnection:
        conn = RecordingConnection()
        self.opened.append(conn)
        return conn

    @property
    def count(self) -> int:
        return len(self.opened)


def _with_counting_connections(adapter):  # noqa: ANN001
    """Replace the adapter's `open_connection` with a counting fake."""
    factory = _CountingConnectionFactory()
    adapter.open_connection = factory  # type: ignore[method-assign]
    return factory


def _adapter(dialect: str):
    """Build the adapter for a dialect with a DSN that is never dialled."""
    return {
        "oracle": lambda: OracleAdapter("user/pass@host:1521/svc"),
        "postgres": lambda: PostgresAdapter("postgresql://unused"),
        "mssql": lambda: MssqlAdapter("DRIVER={unused};SERVER=unused"),
    }[dialect]()


# --------------------------------------------------------------------------
# health_check: 4 (Oracle), 5 (Postgres) and 4 (MSSQL) queries, one login
# --------------------------------------------------------------------------

@pytest.mark.parametrize("dialect", ["oracle", "postgres", "mssql"])
def test_health_check_opens_exactly_one_connection(dialect):
    adapter = _adapter(dialect)
    factory = _with_counting_connections(adapter)

    result = adapter.health_check()

    assert factory.count == 1
    # Every check still ran and reported independently.
    assert len(result.data) >= 4
    assert all(row["status"] == "ok" for row in result.data)


def test_oracle_health_check_issues_read_only_transaction_once():
    """A second SET TRANSACTION READ ONLY in the same transaction is ORA-01453."""
    adapter = _adapter("oracle")
    factory = _with_counting_connections(adapter)

    adapter.health_check()

    conn = factory.opened[0]
    assert conn.executed.count("SET TRANSACTION READ ONLY") == 1
    assert conn.executed[0] == "SET TRANSACTION READ ONLY"


def test_postgres_health_check_sets_read_only_before_first_query():
    """psycopg refuses to set read_only once a transaction is in progress."""
    adapter = _adapter("postgres")
    factory = _with_counting_connections(adapter)

    adapter.health_check()

    assert factory.opened[0].read_only is True


@pytest.mark.parametrize("dialect", ["oracle", "postgres", "mssql"])
def test_health_check_closes_its_connection(dialect):
    """The session is not a pool: the connection dies with the tool call."""
    adapter = _adapter(dialect)
    factory = _with_counting_connections(adapter)

    adapter.health_check()

    conn = factory.opened[0]
    assert conn.closed is True
    assert conn.committed is False


# --------------------------------------------------------------------------
# The degrade-per-check contract must survive the shared session
# --------------------------------------------------------------------------

class _FailFirstQueryConnection(RecordingConnection):
    """Model PostgreSQL's aborted-transaction behaviour after a failed query.

    The first query fails, as a missing grant would. From then on the connection
    rejects *every* statement with "current transaction is aborted" until someone
    rolls back — which is exactly the trap a shared session walks into, and why
    the session policy has a `recover` step.
    """

    def __init__(self) -> None:
        super().__init__()
        self.queries_seen = 0
        self.aborted = False

    def cursor(self, *args, **kwargs):  # noqa: ANN002, ANN003
        return _FailFirstCursor(self)

    def rollback(self) -> None:
        super().rollback()
        self.aborted = False


class _FailFirstCursor:
    def __init__(self, conn: _FailFirstQueryConnection) -> None:
        self._conn = conn
        self.description = None

    def execute(self, query, params=None):  # noqa: ANN001
        self._conn.executed.append(str(query))
        if self._conn.aborted:
            raise psycopg.errors.InFailedSqlTransaction(
                "current transaction is aborted, commands ignored until "
                "end of transaction block"
            )
        self._conn.queries_seen += 1
        if self._conn.queries_seen == 1:
            self._conn.aborted = True
            raise psycopg.errors.InsufficientPrivilege("permission denied")

    def fetchall(self):
        return []

    def __enter__(self):
        return self

    def __exit__(self, *exc_info) -> bool:
        return False


def test_postgres_health_check_degrades_per_check_in_shared_session():
    adapter = _adapter("postgres")
    conn = _FailFirstQueryConnection()
    adapter.open_connection = lambda: conn  # type: ignore[method-assign]

    result = adapter.health_check()

    statuses = [row["status"] for row in result.data]
    assert statuses[0] == "unknown", "the failing check must degrade, not raise"
    assert statuses[1:] == ["ok"] * (len(statuses) - 1), (
        "later checks must still run: the aborted transaction was rolled back"
    )
    assert conn.rolled_back is True


def test_session_recovers_between_queries_only_when_it_outlives_them():
    """A failure in a standalone `_fetch_all` closes the connection outright."""
    adapter = _adapter("postgres")
    conn = _FailFirstQueryConnection()
    adapter.open_connection = lambda: conn  # type: ignore[method-assign]

    with pytest.raises(DatabaseError):
        adapter._fetch_all("SELECT 1")

    assert conn.closed is True


# --------------------------------------------------------------------------
# get_ddl on a table: 3 queries (Postgres) / 5 (MSSQL), one login
# --------------------------------------------------------------------------

@pytest.mark.parametrize("dialect", ["postgres", "mssql"])
def test_get_ddl_table_opens_exactly_one_connection(dialect):
    adapter = _adapter(dialect)
    factory = _with_counting_connections(adapter)

    adapter.get_ddl(schema="public", object_name="users", object_type="table")

    assert factory.count == 1
    assert factory.opened[0].closed is True


def test_oracle_table_stats_opens_exactly_one_connection():
    adapter = _adapter("oracle")
    factory = _with_counting_connections(adapter)

    adapter.table_stats(schema="APP", table="USERS")

    assert factory.count == 1
    assert factory.opened[0].closed is True


# --------------------------------------------------------------------------
# Per-query state must not leak across a shared session
# --------------------------------------------------------------------------

@pytest.mark.parametrize(
    ("dialect", "attribute"),
    [("oracle", "call_timeout"), ("mssql", "timeout")],
)
def test_query_timeout_does_not_leak_to_the_next_query(dialect, attribute):
    """The timeout lives on the connection, which now outlives a single query."""
    adapter = _adapter(dialect)
    factory = _with_counting_connections(adapter)

    with adapter.session() as conn:
        original = getattr(conn, attribute)
        adapter._fetch_all("SELECT 1", timeout_ms=4000)
        assert getattr(conn, attribute) == original, (
            "the timeout must be restored once the query it applies to is done"
        )

    assert factory.count == 1


def test_postgres_query_timeout_is_reset_inside_the_session():
    adapter = _adapter("postgres")
    factory = _with_counting_connections(adapter)

    with adapter.session():
        adapter._fetch_all("SELECT 1", timeout_ms=4000)

    executed = factory.opened[0].executed
    assert "SET LOCAL statement_timeout = 4000" in executed
    assert executed[-1] == "SET LOCAL statement_timeout = DEFAULT"


# --------------------------------------------------------------------------
# Reentrancy
# --------------------------------------------------------------------------

@pytest.mark.parametrize("dialect", ["oracle", "postgres", "mssql"])
def test_nested_sessions_share_one_connection(dialect):
    adapter = _adapter(dialect)
    factory = _with_counting_connections(adapter)

    with adapter.session() as outer:
        with adapter.session() as inner:
            assert inner is outer
        assert outer.closed is False, "the inner block must not close the connection"

    assert factory.count == 1
    assert factory.opened[0].closed is True


@pytest.mark.parametrize("dialect", ["oracle", "postgres", "mssql"])
def test_session_is_released_even_when_the_body_raises(dialect):
    adapter = _adapter(dialect)
    factory = _with_counting_connections(adapter)

    with pytest.raises(RuntimeError):
        with adapter.session():
            raise RuntimeError("boom")

    assert factory.opened[0].closed is True
    # The holder is clear, so the next call opens a fresh connection.
    with adapter.session():
        pass
    assert factory.count == 2
