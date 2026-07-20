from conftest import BaseStubAdapter, make_settings

from src.adapters.base import AdapterResult
from src.services.introspection_service import IntrospectionService


class DummyAdapter(BaseStubAdapter):
    def select_columns(self, schema, table, columns, limit, offset=0) -> AdapterResult:
        return AdapterResult(
            data=[{column: f"v_{column}" for column in columns}],
            schema_used=schema,
        )


def _single_schema_settings():
    return make_settings(allowed_schemas=("sample_schema_a",))


def _multi_schema_settings():
    return make_settings(allowed_schemas=("sample_schema_a", "sample_schema_b"))


def test_select_columns_rejects_empty_columns():
    service = IntrospectionService(adapter=DummyAdapter(), settings=_single_schema_settings())
    result = service.select_columns(table="sample_table", columns=[], schema="sample_schema_a", limit=10)
    assert result["ok"] is False
    assert result["error"]["code"] == "invalid_columns"


def test_select_columns_rejects_invalid_identifier():
    service = IntrospectionService(adapter=DummyAdapter(), settings=_single_schema_settings())
    result = service.select_columns(
        table="sample_table",
        columns=["id", "bad-column"],
        schema="sample_schema_a",
        limit=10,
    )
    assert result["ok"] is False
    assert result["error"]["code"] == "invalid_columns"


def test_select_columns_valid_input_returns_ok():
    service = IntrospectionService(adapter=DummyAdapter(), settings=_single_schema_settings())
    result = service.select_columns(
        table="sample_table",
        columns=["id", "nazev"],
        schema="sample_schema_a",
        limit=10,
    )
    assert result["ok"] is True
    assert result["data"] == [{"id": "v_id", "nazev": "v_nazev"}]


def test_select_columns_requires_schema_even_for_single_allowed():
    service = IntrospectionService(adapter=DummyAdapter(), settings=_single_schema_settings())
    result = service.select_columns(
        table="sample_table",
        columns=["id"],
        schema="",
        limit=10,
    )
    assert result["ok"] is False
    assert result["error"]["code"] == "missing_schema"


def test_select_columns_requires_schema_when_multiple_allowed():
    service = IntrospectionService(adapter=DummyAdapter(), settings=_multi_schema_settings())
    result = service.select_columns(
        table="sample_table",
        columns=["id"],
        schema="",
        limit=10,
    )
    assert result["ok"] is False
    assert result["error"]["code"] == "missing_schema"


def test_select_columns_limit_is_truncated_to_max():
    settings = make_settings(allowed_schemas=("sample_schema_a",), max_sample_limit=5)
    service = IntrospectionService(adapter=DummyAdapter(), settings=settings)
    result = service.select_columns(
        table="sample_table",
        columns=["id"],
        schema="sample_schema_a",
        limit=999,
    )
    assert result["ok"] is True
    assert result["meta"]["truncated"] is True
    assert result["meta"]["warnings"]
