from __future__ import annotations

from conftest import BaseStubAdapter, make_settings

from src.adapters.base import AdapterResult
from src.services.introspection_service import IntrospectionService


class RecordingAdapter(BaseStubAdapter):
    """Stub adapter that records the arguments it was called with."""

    def __init__(self) -> None:
        self.calls: dict[str, dict] = {}

    def table_stats(self, schema, table) -> AdapterResult:
        self.calls["table_stats"] = {"schema": schema, "table": table}
        return AdapterResult(data=[{"schema": schema, "table": table, "row_estimate": 42}])

    def list_foreign_keys(self, schemas, table=None) -> AdapterResult:
        self.calls["list_foreign_keys"] = {"schemas": schemas, "table": table}
        return AdapterResult(data=[{"constraint_name": "fk_x", "table": "child", "ref_table": "parent"}])

    def top_queries(self, limit) -> AdapterResult:
        self.calls["top_queries"] = {"limit": limit}
        return AdapterResult(data=[{"query_id": "1", "query": "SELECT 1"}])

    def health_check(self) -> AdapterResult:
        self.calls["health_check"] = {}
        return AdapterResult(data=[{"check": "x", "status": "ok"}])


def _service(adapter: RecordingAdapter) -> IntrospectionService:
    return IntrospectionService(adapter=adapter, settings=make_settings(allowed_schemas=("s",)))


def test_table_stats_delegates_and_validates():
    adapter = RecordingAdapter()
    result = _service(adapter).table_stats(schema="s", table="t")

    assert result["ok"] is True
    assert result["meta"]["schema_used"] == "s"
    assert adapter.calls["table_stats"] == {"schema": "s", "table": "t"}


def test_table_stats_rejects_empty_table():
    result = _service(RecordingAdapter()).table_stats(schema="s", table="  ")
    assert result["ok"] is False
    assert result["error"]["code"] == "invalid_table"


def test_table_stats_rejects_schema_outside_allowlist():
    result = _service(RecordingAdapter()).table_stats(schema="other", table="t")
    assert result["ok"] is False
    assert result["error"]["code"] == "invalid_schema"


def test_list_foreign_keys_passes_schema_and_optional_table():
    adapter = RecordingAdapter()
    result = _service(adapter).list_foreign_keys(schema="s", table="child")

    assert result["ok"] is True
    assert adapter.calls["list_foreign_keys"] == {"schemas": ("s",), "table": "child"}


def test_list_foreign_keys_normalizes_blank_table_to_none():
    adapter = RecordingAdapter()
    _service(adapter).list_foreign_keys(schema="s", table="   ")
    assert adapter.calls["list_foreign_keys"]["table"] is None


def test_top_queries_defaults_and_clamps_limit():
    adapter = RecordingAdapter()
    _service(adapter).top_queries(limit=None)
    assert adapter.calls["top_queries"]["limit"] == 20

    _service(adapter).top_queries(limit=5000)
    assert adapter.calls["top_queries"]["limit"] == 200

    _service(adapter).top_queries(limit=0)
    assert adapter.calls["top_queries"]["limit"] == 1


def test_health_check_delegates():
    adapter = RecordingAdapter()
    result = _service(adapter).health_check()
    assert result["ok"] is True
    assert "health_check" in adapter.calls
    assert result["data"] == [{"check": "x", "status": "ok"}]
