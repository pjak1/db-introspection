from __future__ import annotations

import pytest
from conftest import BaseStubAdapter, make_settings

from src.adapters.base import AdapterResult
from src.adapters.mssql import MssqlAdapter
from src.adapters.oracle import OracleAdapter
from src.adapters.postgres import PostgresAdapter
from src.services.introspection_service import IntrospectionService


def _capture(adapter):
    """Patch an adapter's `_fetch_all` to capture the query and bind params."""
    captured: dict = {}

    def fake_fetch(query, params=None, timeout_ms=None):  # noqa: ANN001
        captured["query"] = query
        captured["params"] = params
        return []

    adapter._fetch_all = fake_fetch  # type: ignore[method-assign]
    return captured


# --------------------------------------------------------------------------
# list_indexes adapter SQL
# --------------------------------------------------------------------------

def test_postgres_list_indexes_filters_by_table():
    adapter = PostgresAdapter("postgresql://unused")
    captured = _capture(adapter)
    adapter.list_indexes(schemas=("public",), table="users")

    query = captured["query"]
    assert "FROM pg_catalog.pg_index ix" in query
    assert "ix.indisprimary AS is_primary" in query
    assert "AND t.relname = %s" in query
    assert captured["params"] == (["public"], "users")


def test_postgres_list_indexes_without_table_has_no_table_filter():
    adapter = PostgresAdapter("postgresql://unused")
    captured = _capture(adapter)
    adapter.list_indexes(schemas=("public",))

    assert "AND t.relname = %s" not in captured["query"]
    assert captured["params"] == (["public"],)


def test_oracle_list_indexes_uses_listagg_and_constraint_join():
    adapter = OracleAdapter("user/pass@db")
    captured = _capture(adapter)
    adapter.list_indexes(schemas=("sample_schema",), table="users")

    query = captured["query"]
    assert "FROM all_indexes i" in query
    assert "LISTAGG(col.column_name" in query
    assert captured["params"]["table_name"] == "USERS"


def test_mssql_list_indexes_skips_heaps_and_binds_table():
    adapter = MssqlAdapter("Driver=test")
    captured = _capture(adapter)
    adapter.list_indexes(schemas=("dbo",), table="users")

    query = captured["query"]
    assert "FROM sys.indexes ix" in query
    assert "ix.type > 0" in query
    # schema params first, then table bound twice for the NULL-or-equals filter.
    assert captured["params"] == ("dbo", "users", "users")


# --------------------------------------------------------------------------
# get_ddl adapter SQL
# --------------------------------------------------------------------------

def test_postgres_get_ddl_view_uses_pg_get_viewdef():
    adapter = PostgresAdapter("postgresql://unused")
    captured = _capture(adapter)
    adapter.get_ddl(schema="public", object_name="v_users", object_type="view")

    assert "pg_get_viewdef(c.oid, true)" in captured["query"]
    assert captured["params"] == ("public", "v_users")


def test_postgres_get_ddl_function_passes_prokind():
    adapter = PostgresAdapter("postgresql://unused")
    captured = _capture(adapter)
    adapter.get_ddl(schema="public", object_name="f", object_type="function")

    assert "pg_get_functiondef(p.oid)" in captured["query"]
    assert captured["params"] == ("function", "public", "f", "f")


def test_postgres_get_ddl_table_is_not_supported():
    adapter = PostgresAdapter("postgresql://unused")
    _capture(adapter)
    result = adapter.get_ddl(schema="public", object_name="users", object_type="table")

    assert result.status == "not_supported"
    assert result.data == []


def test_mssql_get_ddl_uses_object_definition():
    adapter = MssqlAdapter("Driver=test")
    captured = _capture(adapter)
    # _capture returns [] -> treated as not found, but query is still recorded.
    adapter.get_ddl(schema="dbo", object_name="usp_x", object_type="procedure")

    assert "OBJECT_DEFINITION(OBJECT_ID(" in captured["query"]
    assert captured["params"] == ("procedure", "dbo", "usp_x", "dbo", "usp_x")


def test_oracle_get_ddl_uses_dbms_metadata():
    adapter = OracleAdapter("user/pass@db")
    captured = _capture(adapter)
    adapter.get_ddl(schema="sample_schema", object_name="t", object_type="table")

    assert "DBMS_METADATA.GET_DDL(:otype" in captured["query"]
    assert captured["params"]["otype"] == "TABLE"
    assert captured["params"]["oowner2"] == "SAMPLE_SCHEMA"


# --------------------------------------------------------------------------
# search_objects adapter SQL
# --------------------------------------------------------------------------

def test_postgres_search_objects_builds_like_and_type_filter():
    adapter = PostgresAdapter("postgresql://unused")
    captured = _capture(adapter)
    adapter.search_objects(
        schemas=("public",), pattern="usr", object_types=("table", "view"))

    query = captured["query"]
    assert "o.object_name ILIKE %s" in query
    assert "o.object_type = ANY(%s)" in query
    *_schema_lists, like, types = captured["params"]
    assert like == "%usr%"
    assert types == ["table", "view"]


def test_mssql_search_objects_lowercases_pattern():
    adapter = MssqlAdapter("Driver=test")
    captured = _capture(adapter)
    adapter.search_objects(
        schemas=("dbo",), pattern="USR", object_types=("table",))

    assert "LOWER(o.object_name) LIKE LOWER(?)" in captured["query"]
    assert "%USR%" in captured["params"]


def test_oracle_search_objects_maps_types_to_uppercase():
    adapter = OracleAdapter("user/pass@db")
    captured = _capture(adapter)
    adapter.search_objects(
        schemas=("sample_schema",), pattern="usr", object_types=("table", "function"))

    params = captured["params"]
    assert params["pattern"] == "%USR%"
    assert params["t0"] == "TABLE"
    assert params["t1"] == "FUNCTION"


# --------------------------------------------------------------------------
# Comment columns are included in list_columns / list_tables SQL
# --------------------------------------------------------------------------

def test_postgres_list_columns_includes_comment():
    adapter = PostgresAdapter("postgresql://unused")
    captured = _capture(adapter)
    adapter.list_columns(table="users", schemas=("public",))
    assert "col_description(cls.oid, attr.attnum) AS comment" in captured["query"]
    # params must stay unchanged so the existing contract holds.
    assert captured["params"] == ("users", ["public"])


def test_postgres_list_tables_includes_comment():
    adapter = PostgresAdapter("postgresql://unused")
    captured = _capture(adapter)
    adapter.list_tables(schemas=("public",), include_system=False)
    assert "AS table_comment" in captured["query"]


def test_oracle_list_columns_quotes_reserved_comment_alias():
    # COMMENT is a reserved word in Oracle; an unquoted alias raises ORA-00923.
    adapter = OracleAdapter("user/pass@db")
    captured = _capture(adapter)
    adapter.list_columns(table="users", schemas=("sample_schema",))
    assert 'AS "comment"' in captured["query"]
    assert "AS comment," not in captured["query"]


# --------------------------------------------------------------------------
# Service-level validation and not_supported degradation
# --------------------------------------------------------------------------

class _StubAdapter(BaseStubAdapter):
    """Minimal adapter recording calls; inherits base not_supported defaults."""

    def __init__(self):
        self.calls: dict = {}

    def list_indexes(self, schemas, table=None):
        self.calls["list_indexes"] = {"schemas": schemas, "table": table}
        return AdapterResult(data=[{"index_name": "ix"}], status="available")

    def get_ddl(self, schema, object_name, object_type):
        self.calls["get_ddl"] = {
            "schema": schema, "object_name": object_name, "object_type": object_type}
        return AdapterResult(data=[], status="not_supported")

    def search_objects(self, schemas, pattern, object_types):
        self.calls["search_objects"] = {
            "schemas": schemas, "pattern": pattern, "object_types": object_types}
        return AdapterResult(data=[], status="available")


def _service(adapter=None) -> tuple[IntrospectionService, _StubAdapter]:
    adapter = adapter or _StubAdapter()
    return IntrospectionService(adapter=adapter, settings=make_settings()), adapter


def test_service_list_indexes_normalizes_blank_table_to_none():
    service, adapter = _service()
    result = service.list_indexes(schema="public", table="   ")
    assert result["ok"] is True
    assert adapter.calls["list_indexes"]["table"] is None


def test_service_list_indexes_rejects_disallowed_schema():
    service, _ = _service()
    result = service.list_indexes(schema="secret", table=None)
    assert result["ok"] is False
    assert result["error"]["code"] == "invalid_schema"


def test_service_get_ddl_rejects_unknown_object_type():
    service, _ = _service()
    result = service.get_ddl(schema="public", object_name="x", object_type="trigger")
    assert result["ok"] is False
    assert result["error"]["code"] == "invalid_object_type"


def test_service_get_ddl_rejects_empty_object_name():
    service, _ = _service()
    result = service.get_ddl(schema="public", object_name="  ", object_type="view")
    assert result["ok"] is False
    assert result["error"]["code"] == "invalid_object_name"


def test_service_get_ddl_degrades_when_adapter_not_supported():
    # _StubAdapter.get_ddl returns a not_supported result; the service surfaces it
    # as a successful envelope carrying meta.status == "not_supported".
    service, _ = _service()
    result = service.get_ddl(schema="public", object_name="x", object_type="table")
    assert result["ok"] is True
    assert result["meta"]["status"] == "not_supported"


def test_service_search_objects_defaults_to_all_types():
    service, adapter = _service()
    result = service.search_objects(schema="public", pattern="a", object_types=None)
    assert result["ok"] is True
    assert adapter.calls["search_objects"]["object_types"] == (
        "table", "view", "sequence", "procedure", "function")


def test_service_search_objects_rejects_empty_pattern():
    service, _ = _service()
    result = service.search_objects(schema="public", pattern="  ", object_types=None)
    assert result["ok"] is False
    assert result["error"]["code"] == "invalid_pattern"


def test_service_search_objects_rejects_unknown_object_type():
    service, _ = _service()
    result = service.search_objects(
        schema="public", pattern="a", object_types=["table", "trigger"])
    assert result["ok"] is False
    assert result["error"]["code"] == "invalid_object_type"


def test_service_search_objects_dedupes_requested_types():
    service, adapter = _service()
    service.search_objects(
        schema="public", pattern="a", object_types=["table", "Table", "TABLE"])
    assert adapter.calls["search_objects"]["object_types"] == ("table",)


# --------------------------------------------------------------------------
# #1 additive output-shape consistency
# --------------------------------------------------------------------------

def test_oracle_list_sequences_exposes_start_value_key():
    adapter = OracleAdapter(dsn="u/p@db")
    captured = _capture(adapter)
    adapter.list_sequences(schemas=("SCHEMA",))
    assert "start_value" in captured["query"].lower()


def test_oracle_list_constraints_returns_real_check_clause():
    adapter = OracleAdapter(dsn="u/p@db")
    captured = _capture(adapter)
    adapter.list_constraints(schemas=("SCHEMA",))
    assert "search_condition_vc as check_clause" in captured["query"].lower()


def test_ddl_object_types_are_dialect_accurate():
    # The advertised DDL object types must match what each dialect really supports:
    # only Oracle reconstructs table DDL.
    assert "table" in OracleAdapter.ddl_object_types
    assert "table" not in PostgresAdapter.ddl_object_types
    assert "table" not in MssqlAdapter.ddl_object_types


def test_get_ddl_table_rejected_when_dialect_excludes_it():
    class _PgStub(BaseStubAdapter):
        ddl_object_types = ("view", "procedure", "function")

    service = IntrospectionService(adapter=_PgStub(), settings=make_settings())
    result = service.get_ddl(schema="public", object_name="t", object_type="table")
    assert result["ok"] is False
    assert result["error"]["code"] == "invalid_object_type"
