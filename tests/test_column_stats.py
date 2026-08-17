"""Cover db_column_stats: query shape, extended statistics and degradation.

Two things here are easy to get quietly wrong and both mislead rather than fail:
PostgreSQL encodes distinct values as a negative fraction, and SQL Server has no
distinct/null count in its catalog at all. Both must be visible in the answer.
"""
from __future__ import annotations

from conftest import BaseStubAdapter, RecordingConnection, make_settings, stub_session

from src.adapters.base import AdapterResult, DatabaseAdapter
from src.adapters.mssql import MssqlAdapter
from src.adapters.oracle import OracleAdapter
from src.adapters.postgres import PostgresAdapter
from src.errors import DatabaseError
from src.services.introspection_service import IntrospectionService


def _route(adapter, responses):  # noqa: ANN001
    """Route `_fetch_all` by query substring; values are rows or an exception."""
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
# Oracle
# --------------------------------------------------------------------------

def test_oracle_column_stats_derives_null_fraction_and_asks_for_column_groups():
    adapter = OracleAdapter("user/pass@host:1521/svc")
    seen = _route(adapter, {"FROM all_tab_col_statistics c": [{"column": "ID"}]})
    adapter.column_stats(schema="rpp", table="osoba")

    columns_query = next(q for q in seen if "FROM all_tab_col_statistics c" in q)
    # num_nulls is a count, so the fraction needs the table's row estimate.
    assert "c.num_nulls / t.num_rows END AS null_fraction" in columns_query
    assert "'column' AS kind" in columns_query
    # Extended (column group) statistics are a second, independent query.
    assert any("all_stat_extensions" in q for q in seen)


def test_oracle_column_stats_binds_upper_cased_identifiers():
    adapter = OracleAdapter("user/pass@host:1521/svc")
    captured: dict = {}

    def fake_fetch(query, params=None, timeout_ms=None):  # noqa: ANN001
        if "FROM all_tab_col_statistics c" in query:
            captured["params"] = params
        return []

    adapter._fetch_all = fake_fetch  # type: ignore[method-assign]
    stub_session(adapter)
    adapter.column_stats(schema="rpp", table="osoba", column="rodne_cislo")

    assert captured["params"] == {"o": "RPP", "t": "OSOBA", "c": "RODNE_CISLO"}


def test_oracle_column_stats_degrades_extended_without_losing_columns():
    """ALL_STAT_EXTENSIONS is 11g+; an older or restricted database must not fail."""
    adapter = OracleAdapter("user/pass@host:1521/svc")
    _route(adapter, {
        "FROM all_tab_col_statistics c": [{"column": "ID", "kind": "column"}],
        "FROM all_stat_extensions": DatabaseError(
            "database_error", "Oracle query failed.",
            details="ORA-00942: table or view does not exist"),
    })

    result = adapter.column_stats(schema="rpp", table="osoba")

    assert len(result.data) == 1
    assert result.status == "available"
    assert any("ALL_STAT_EXTENSIONS" in warning for warning in result.warnings)


def test_oracle_column_stats_says_include_histogram_is_a_noop():
    adapter = OracleAdapter("user/pass@host:1521/svc")
    _route(adapter, {"FROM all_tab_col_statistics c": [{"column": "ID"}]})

    result = adapter.column_stats(schema="rpp", table="osoba", include_histogram=True)

    assert any("no effect on Oracle" in warning for warning in result.warnings)


def test_oracle_column_stats_reports_not_found_for_unknown_table():
    adapter = OracleAdapter("user/pass@host:1521/svc")
    _route(adapter, {})

    result = adapter.column_stats(schema="rpp", table="nope")

    assert result.data == []
    assert result.status == "not_found"


# --------------------------------------------------------------------------
# PostgreSQL
# --------------------------------------------------------------------------

def test_postgres_column_stats_omits_histogram_columns_by_default():
    adapter = PostgresAdapter("postgresql://unused")
    seen = _route(adapter, {"pg_catalog.pg_stats s": [{"column": "id"}]})
    adapter.column_stats(schema="public", table="users")

    columns_query = next(q for q in seen if "pg_catalog.pg_stats s" in q)
    assert "s.n_distinct AS distinct_estimate" in columns_query
    assert "most_common_vals" not in columns_query
    assert "histogram_bounds" not in columns_query
    assert any("pg_stats_ext" in q for q in seen)


def test_postgres_column_stats_adds_histogram_columns_when_requested():
    adapter = PostgresAdapter("postgresql://unused")
    seen = _route(adapter, {"pg_catalog.pg_stats s": [{"column": "id"}]})
    adapter.column_stats(schema="public", table="users", include_histogram=True)

    columns_query = next(q for q in seen if "pg_catalog.pg_stats s" in q)
    assert "s.most_common_vals::text AS most_common_vals" in columns_query
    assert "s.histogram_bounds::text AS histogram_bounds" in columns_query


def test_postgres_column_stats_explains_negative_distinct_estimate():
    """A negative n_distinct is a fraction of rows; unexplained it reads as garbage."""
    adapter = PostgresAdapter("postgresql://unused")
    _route(adapter, {
        "pg_catalog.pg_stats s": [
            {"column": "id", "distinct_estimate": -1.0, "distinct_is_fraction": True},
        ],
    })

    result = adapter.column_stats(schema="public", table="users")

    assert any("fraction of the row count" in w for w in result.warnings)


def test_postgres_column_stats_is_quiet_when_distinct_is_a_count():
    adapter = PostgresAdapter("postgresql://unused")
    _route(adapter, {
        "pg_catalog.pg_stats s": [
            {"column": "id", "distinct_estimate": 42.0, "distinct_is_fraction": False},
        ],
    })

    result = adapter.column_stats(schema="public", table="users")

    assert result.warnings == []


def test_postgres_column_stats_degrades_extended_on_old_servers():
    adapter = PostgresAdapter("postgresql://unused")
    _route(adapter, {
        "pg_catalog.pg_stats s": [{"column": "id"}],
        "pg_stats_ext": DatabaseError(
            "database_error", "Database query failed.",
            details='relation "pg_stats_ext" does not exist'),
    })

    result = adapter.column_stats(schema="public", table="users")

    assert len(result.data) == 1
    assert any("pg_stats_ext does not exist" in w for w in result.warnings)


def test_postgres_column_stats_binds_optional_column_filter():
    adapter = PostgresAdapter("postgresql://unused")
    captured: dict = {}

    def fake_fetch(query, params=None, timeout_ms=None):  # noqa: ANN001
        if "pg_catalog.pg_stats s" in query:
            captured["query"] = query
            captured["params"] = params
        return []

    adapter._fetch_all = fake_fetch  # type: ignore[method-assign]
    stub_session(adapter)
    adapter.column_stats(schema="public", table="users", column="email")

    assert "AND s.attname = %s" in captured["query"]
    assert captured["params"] == ("public", "users", "email")


# --------------------------------------------------------------------------
# SQL Server
# --------------------------------------------------------------------------

def test_mssql_column_stats_marks_multi_column_statistics_as_extended():
    adapter = MssqlAdapter("DRIVER={unused}")
    seen = _route(adapter, {"sys.stats s": [{"stats_name": "s1"}]})
    adapter.column_stats(schema="dbo", table="Termin")

    query = seen[0]
    assert "'extended' ELSE 'column' END AS kind" in query
    # modification_counter is the staleness signal the catalog does expose.
    assert "sp.modification_counter" in query
    assert "sys.dm_db_stats_properties(s.object_id, s.stats_id)" in query
    # Without the flag the histogram DMF must not be touched.
    assert "dm_db_stats_histogram" not in query


def test_mssql_column_stats_derives_distinct_from_histogram_when_requested():
    adapter = MssqlAdapter("DRIVER={unused}")
    seen = _route(adapter, {"sys.stats s": [{"stats_name": "s1"}]})
    adapter.column_stats(schema="dbo", table="Termin", include_histogram=True)

    query = seen[0]
    assert "sys.dm_db_stats_histogram(s.object_id, s.stats_id)" in query
    assert "SUM(h.distinct_range_rows) + COUNT(*) AS distinct_estimate" in query
    assert "AS null_rows" in query


def test_mssql_column_stats_warns_that_distinct_is_absent_by_default():
    adapter = MssqlAdapter("DRIVER={unused}")
    _route(adapter, {"sys.stats s": [{"stats_name": "s1"}]})

    result = adapter.column_stats(schema="dbo", table="Termin")

    assert any("include_histogram" in w for w in result.warnings)


def test_mssql_column_stats_has_no_such_warning_with_the_flag():
    adapter = MssqlAdapter("DRIVER={unused}")
    _route(adapter, {"sys.stats s": [{"stats_name": "s1"}]})

    result = adapter.column_stats(schema="dbo", table="Termin", include_histogram=True)

    assert result.warnings == []


def test_mssql_column_stats_binds_table_and_column_twice():
    adapter = MssqlAdapter("DRIVER={unused}")
    captured: dict = {}

    def fake_fetch(query, params=None, timeout_ms=None):  # noqa: ANN001
        captured["params"] = params
        return []

    adapter._fetch_all = fake_fetch  # type: ignore[method-assign]
    stub_session(adapter)
    adapter.column_stats(schema="dbo", table="Termin", column="JazykID")

    assert captured["params"] == ("dbo", "Termin", "JazykID", "JazykID")


def test_mssql_column_stats_degrades_on_permission_error():
    adapter = MssqlAdapter("DRIVER={unused}")
    _route(adapter, {
        "sys.stats s": DatabaseError(
            "database_error", "MSSQL query failed.",
            details="VIEW DATABASE STATE permission was denied"),
    })

    result = adapter.column_stats(schema="dbo", table="Termin")

    assert result.data == []
    assert result.status == "not_available"


# --------------------------------------------------------------------------
# Base default and session count
# --------------------------------------------------------------------------

def test_column_stats_is_part_of_the_abstract_contract():
    """Every dialect implements it explicitly; the base declares, never answers."""
    assert "column_stats" in DatabaseAdapter.__abstractmethods__


def _counting(adapter):  # noqa: ANN001
    opened: list[RecordingConnection] = []

    def factory() -> RecordingConnection:
        conn = RecordingConnection()
        opened.append(conn)
        return conn

    adapter.open_connection = factory  # type: ignore[method-assign]
    return opened


def test_column_stats_opens_exactly_one_connection_per_dialect():
    for adapter in (
        OracleAdapter("user/pass@host:1521/svc"),
        PostgresAdapter("postgresql://unused"),
        MssqlAdapter("DRIVER={unused}"),
    ):
        opened = _counting(adapter)
        adapter._fetch_all = lambda *a, **k: []  # type: ignore[method-assign]

        adapter.column_stats(schema="s", table="t")

        assert len(opened) == 1, f"{adapter.dialect} opened {len(opened)} connections"


# --------------------------------------------------------------------------
# Service layer
# --------------------------------------------------------------------------

class _RecordingAdapter(BaseStubAdapter):
    def __init__(self) -> None:
        super().__init__()
        self.calls: dict = {}

    def column_stats(self, schema, table, column=None, include_histogram=False):
        self.calls["column_stats"] = {
            "schema": schema, "table": table, "column": column,
            "include_histogram": include_histogram,
        }
        return AdapterResult(data=[{"column": "id"}])


def _service(adapter) -> IntrospectionService:  # noqa: ANN001
    return IntrospectionService(adapter=adapter, settings=make_settings(
        allowed_schemas=("public",)))


def test_service_column_stats_passes_arguments():
    adapter = _RecordingAdapter()
    result = _service(adapter).column_stats(
        schema="public", table="users", column="email", include_histogram=True)

    assert result["ok"] is True
    assert adapter.calls["column_stats"] == {
        "schema": "public", "table": "users", "column": "email",
        "include_histogram": True}


def test_service_column_stats_normalizes_blank_column_to_none():
    adapter = _RecordingAdapter()
    _service(adapter).column_stats(schema="public", table="users", column="  ")
    assert adapter.calls["column_stats"]["column"] is None


def test_service_column_stats_rejects_empty_table():
    result = _service(_RecordingAdapter()).column_stats(schema="public", table=" ")
    assert result["ok"] is False
    assert result["error"]["code"] == "invalid_table"


def test_service_column_stats_rejects_injection_shaped_column():
    result = _service(_RecordingAdapter()).column_stats(
        schema="public", table="users", column="id; DROP TABLE users")
    assert result["ok"] is False
    assert result["error"]["code"] == "invalid_columns"


def test_service_column_stats_rejects_schema_outside_allowlist():
    result = _service(_RecordingAdapter()).column_stats(schema="secret", table="users")
    assert result["ok"] is False
    assert result["error"]["code"] == "invalid_schema"
