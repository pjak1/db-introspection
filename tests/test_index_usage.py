"""Cover db_index_usage: query shape, per-dialect degradation and stats_since.

The counters this tool reports are resettable, so `stats_since` has to travel
with every row — a `never_used` verdict without it invites dropping an index
that a monthly job needs. These tests hold that contract in place.
"""
from __future__ import annotations

from conftest import BaseStubAdapter, RecordingConnection, make_settings, stub_session

from src.adapters.base import AdapterResult, DatabaseAdapter
from src.adapters.mssql import MssqlAdapter
from src.adapters.oracle import OracleAdapter
from src.adapters.postgres import PostgresAdapter
from src.errors import DatabaseError
from src.services.introspection_service import IntrospectionService


def _capture_by_query(adapter, responses):  # noqa: ANN001
    """Route `_fetch_all` by substring so multi-query methods can be stubbed.

    `responses` maps a substring of the query to either rows or an exception to
    raise. Every executed query is recorded in the returned list.
    """
    seen: list[str] = []

    def fake_fetch(query, params=None, timeout_ms=None):  # noqa: ANN001
        seen.append(query)
        for needle, outcome in responses.items():
            if needle in query:
                if isinstance(outcome, Exception):
                    raise outcome
                return outcome
        return []

    adapter._fetch_all = fake_fetch  # type: ignore[method-assign]
    stub_session(adapter)
    return seen


# --------------------------------------------------------------------------
# SQL Server
# --------------------------------------------------------------------------

def test_mssql_index_usage_left_joins_so_unused_indexes_survive():
    """An inner join would hide exactly the indexes with no recorded reads."""
    adapter = MssqlAdapter("DRIVER={unused}")
    seen = _capture_by_query(adapter, {})
    adapter.index_usage(schemas=("dbo",))

    counters = next(q for q in seen if "dm_db_index_usage_stats" in q)
    assert "LEFT JOIN sys.dm_db_index_usage_stats" in counters
    # Usage stats are per database; without this the same object_id from another
    # database could be matched.
    assert "ius.database_id = DB_ID()" in counters
    assert "AS never_used" in counters
    assert "AS writes_overhead" in counters
    # Size comes from the catalog, so it needs no DMV grant.
    assert "sys.allocation_units" in counters
    assert "sys.dm_db_index_physical_stats" not in counters


def test_mssql_index_usage_adds_fragmentation_only_when_requested():
    adapter = MssqlAdapter("DRIVER={unused}")
    seen = _capture_by_query(adapter, {})
    adapter.index_usage(schemas=("dbo",), include_fragmentation=True)

    counters = next(q for q in seen if "dm_db_index_usage_stats" in q)
    assert "sys.dm_db_index_physical_stats" in counters
    assert "'LIMITED'" in counters
    assert "AS fragmentation_pct" in counters


def test_mssql_index_usage_stamps_every_row_with_stats_since():
    adapter = MssqlAdapter("DRIVER={unused}")
    _capture_by_query(adapter, {
        "dm_db_index_usage_stats": [
            {"index_name": "ix_a", "reads": 0},
            {"index_name": "ix_b", "reads": 7},
        ],
        "dm_os_sys_info": [{"stats_since": "2026-07-01T08:00:00"}],
    })

    result = adapter.index_usage(schemas=("dbo",))

    assert [row["stats_since"] for row in result.data] == [
        "2026-07-01T08:00:00", "2026-07-01T08:00:00"]
    assert result.status == "available"


def test_mssql_index_usage_degrades_when_counters_are_denied():
    adapter = MssqlAdapter("DRIVER={unused}")
    _capture_by_query(adapter, {
        "dm_db_index_usage_stats": DatabaseError(
            "database_error", "MSSQL query failed.",
            details="permission was denied on object 'dm_db_index_usage_stats'"),
    })

    result = adapter.index_usage(schemas=("dbo",))

    assert result.data == []
    assert result.status == "not_available"
    assert "VIEW DATABASE STATE" in result.warnings[0]


def test_mssql_index_usage_keeps_rows_when_only_stats_since_is_denied():
    """The reset time needs a stricter grant than the counters, so it degrades alone."""
    adapter = MssqlAdapter("DRIVER={unused}")
    _capture_by_query(adapter, {
        "dm_db_index_usage_stats": [{"index_name": "ix_a", "reads": 0}],
        "dm_os_sys_info": DatabaseError(
            "database_error", "MSSQL query failed.",
            details="VIEW SERVER STATE permission was denied"),
    })

    result = adapter.index_usage(schemas=("dbo",))

    assert len(result.data) == 1
    assert result.data[0]["stats_since"] is None
    assert result.status == "available"
    assert any("VIEW SERVER STATE" in warning for warning in result.warnings)


def test_mssql_index_usage_reraises_unrelated_errors():
    adapter = MssqlAdapter("DRIVER={unused}")
    _capture_by_query(adapter, {
        "dm_db_index_usage_stats": DatabaseError(
            "database_error", "MSSQL query failed.", details="syntax error near 'FROM'"),
    })

    try:
        adapter.index_usage(schemas=("dbo",))
    except DatabaseError as exc:
        assert "syntax error" in str(exc.details)
    else:  # pragma: no cover - the raise is the assertion
        raise AssertionError("expected a non-permission error to propagate")


# --------------------------------------------------------------------------
# PostgreSQL
# --------------------------------------------------------------------------

def test_postgres_index_usage_query_shape_and_table_filter():
    adapter = PostgresAdapter("postgresql://unused")
    seen = _capture_by_query(adapter, {})
    adapter.index_usage(schemas=("public",), table="users")

    counters = next(q for q in seen if "pg_stat_all_indexes" in q)
    assert "LEFT JOIN pg_catalog.pg_stat_all_indexes" in counters
    assert "COALESCE(s.idx_scan, 0) = 0 AS never_used" in counters
    assert "pg_relation_size(ix.indexrelid) AS size_bytes" in counters
    assert "AND t.relname = %s" in counters


def test_postgres_index_usage_warns_when_track_counts_is_off():
    """With track_counts off nothing is counted, so every index looks unused."""
    adapter = PostgresAdapter("postgresql://unused")
    _capture_by_query(adapter, {
        "pg_stat_all_indexes": [{"index_name": "ix_a", "reads": 0}],
        "pg_stat_database": [{"stats_since": None, "track_counts": "off"}],
    })

    result = adapter.index_usage(schemas=("public",))

    assert any("track_counts is off" in warning for warning in result.warnings)


def test_postgres_index_usage_is_quiet_when_track_counts_is_on():
    adapter = PostgresAdapter("postgresql://unused")
    _capture_by_query(adapter, {
        "pg_stat_all_indexes": [{"index_name": "ix_a", "reads": 3}],
        "pg_stat_database": [
            {"stats_since": "2026-06-01T00:00:00", "track_counts": "on"}],
    })

    result = adapter.index_usage(schemas=("public",))

    assert result.warnings == []
    assert result.data[0]["stats_since"] == "2026-06-01T00:00:00"


def test_postgres_index_usage_warns_that_fragmentation_is_unavailable():
    adapter = PostgresAdapter("postgresql://unused")
    _capture_by_query(adapter, {"pg_stat_all_indexes": [{"index_name": "ix_a"}]})

    result = adapter.index_usage(schemas=("public",), include_fragmentation=True)

    assert any("pgstattuple" in warning for warning in result.warnings)


# --------------------------------------------------------------------------
# Oracle and the base default
# --------------------------------------------------------------------------

def test_oracle_index_usage_degrades_without_touching_the_database():
    adapter = OracleAdapter("user/pass@host:1521/svc")

    def explode():  # pragma: no cover - must never be called
        raise AssertionError("Oracle index_usage must not open a connection")

    adapter.open_connection = explode  # type: ignore[method-assign]

    result = adapter.index_usage(schemas=("RPP_REZA",))

    assert result.data == []
    assert result.status == "not_available"
    assert "SELECT_CATALOG_ROLE" in result.warnings[0]


def test_index_usage_is_part_of_the_abstract_contract():
    """The base class stays purely abstract, so no dialect can silently inherit
    a default: each one has to answer, even if the answer is 'not_available'."""
    assert "index_usage" in DatabaseAdapter.__abstractmethods__

    class Incomplete(DatabaseAdapter):
        pass

    try:
        Incomplete()  # type: ignore[abstract]
    except TypeError as exc:
        assert "index_usage" in str(exc)
    else:  # pragma: no cover - the raise is the assertion
        raise AssertionError("an adapter without index_usage must not instantiate")


# --------------------------------------------------------------------------
# One connection per tool call
# --------------------------------------------------------------------------

def _counting(adapter):  # noqa: ANN001
    opened: list[RecordingConnection] = []

    def factory() -> RecordingConnection:
        conn = RecordingConnection()
        opened.append(conn)
        return conn

    adapter.open_connection = factory  # type: ignore[method-assign]
    return opened


def test_mssql_index_usage_opens_exactly_one_connection():
    adapter = MssqlAdapter("DRIVER={unused}")
    opened = _counting(adapter)
    adapter._fetch_all = lambda *a, **k: []  # type: ignore[method-assign]

    adapter.index_usage(schemas=("dbo",))

    assert len(opened) == 1


def test_postgres_index_usage_opens_exactly_one_connection():
    adapter = PostgresAdapter("postgresql://unused")
    opened = _counting(adapter)
    adapter._fetch_all = lambda *a, **k: []  # type: ignore[method-assign]

    adapter.index_usage(schemas=("public",))

    assert len(opened) == 1


# --------------------------------------------------------------------------
# Service layer
# --------------------------------------------------------------------------

class _RecordingAdapter(BaseStubAdapter):
    def __init__(self) -> None:
        super().__init__()
        self.calls: dict = {}

    def index_usage(self, schemas, table=None, include_fragmentation=False) -> AdapterResult:
        self.calls["index_usage"] = {
            "schemas": schemas, "table": table,
            "include_fragmentation": include_fragmentation,
        }
        return AdapterResult(data=[{"index_name": "ix_a"}])


def _service(adapter) -> IntrospectionService:  # noqa: ANN001
    return IntrospectionService(adapter=adapter, settings=make_settings(
        allowed_schemas=("public",)))


def test_service_index_usage_passes_schema_and_flag():
    adapter = _RecordingAdapter()
    result = _service(adapter).index_usage(
        schema="public", table="users", include_fragmentation=True)

    assert result["ok"] is True
    assert result["meta"]["schema_used"] == "public"
    assert adapter.calls["index_usage"] == {
        "schemas": ("public",), "table": "users", "include_fragmentation": True}


def test_service_index_usage_normalizes_blank_table_to_none():
    adapter = _RecordingAdapter()
    _service(adapter).index_usage(schema="public", table="   ")
    assert adapter.calls["index_usage"]["table"] is None


def test_service_index_usage_rejects_schema_outside_allowlist():
    result = _service(_RecordingAdapter()).index_usage(schema="secret", table=None)
    assert result["ok"] is False
    assert result["error"]["code"] == "invalid_schema"
