from __future__ import annotations

import json

import pytest
from conftest import BaseStubAdapter, make_settings

from src.adapters.base import AdapterResult
from src.errors import ValidationError
from src.services.export import normalize_output_format, serialize_rows
from src.services.introspection_service import IntrospectionService

_ROWS = [{"id": 1, "name": "a"}, {"id": 2, "name": None}]


def test_normalize_output_format_accepts_known_and_defaults():
    assert normalize_output_format(None) == "rows"
    assert normalize_output_format("CSV") == "csv"
    with pytest.raises(ValidationError) as exc:
        normalize_output_format("xml")
    assert exc.value.code == "invalid_format"


def test_serialize_rows_json_roundtrips():
    out = serialize_rows(_ROWS, "json")
    assert isinstance(out, str)
    assert json.loads(out) == _ROWS


def test_serialize_rows_csv_has_header_and_blank_for_none():
    out = serialize_rows(_ROWS, "csv")
    lines = out.splitlines()
    assert lines[0] == "id,name"
    assert lines[1] == "1,a"
    assert lines[2] == "2,"  # None rendered as empty field


def test_serialize_rows_passthrough_for_non_tabular():
    assert serialize_rows("already-a-string", "csv") == "already-a-string"
    assert serialize_rows([], "csv") == []


class PagingAdapter(BaseStubAdapter):
    """Records offset and returns fixed rows for sample/select."""

    def __init__(self) -> None:
        self.offset_seen: int | None = None

    def sample_table(self, schema, table, limit, order_by, offset=0) -> AdapterResult:
        self.offset_seen = offset
        return AdapterResult(data=list(_ROWS), schema_used=schema)

    def select_columns(self, schema, table, columns, limit, offset=0) -> AdapterResult:
        self.offset_seen = offset
        return AdapterResult(data=list(_ROWS), schema_used=schema)


def _service(adapter: PagingAdapter) -> IntrospectionService:
    return IntrospectionService(adapter=adapter, settings=make_settings(allowed_schemas=("s",)))


def test_sample_table_passes_offset_and_warns_without_order():
    adapter = PagingAdapter()
    result = _service(adapter).sample_table(
        table="t", schema="s", limit=10, order_by=None, offset=20
    )
    assert adapter.offset_seen == 20
    assert any("offset" in w.lower() for w in result["meta"]["warnings"])


def test_sample_table_offset_with_order_by_has_no_stability_warning():
    adapter = PagingAdapter()
    result = _service(adapter).sample_table(
        table="t", schema="s", limit=10, order_by="id", offset=20
    )
    assert adapter.offset_seen == 20
    assert not any("not stable" in w.lower() for w in result["meta"]["warnings"])


def test_sample_table_json_format_serializes_and_warns():
    adapter = PagingAdapter()
    result = _service(adapter).sample_table(
        table="t", schema="s", limit=10, order_by=None, output_format="json"
    )
    assert isinstance(result["data"], str)
    assert json.loads(result["data"]) == _ROWS
    assert any("serialized as json" in w.lower() for w in result["meta"]["warnings"])


def test_select_columns_offset_passthrough():
    adapter = PagingAdapter()
    _service(adapter).select_columns(
        table="t", columns=["id", "name"], schema="s", limit=10, offset=5
    )
    assert adapter.offset_seen == 5


def test_invalid_format_surfaces_as_error_envelope():
    result = _service(PagingAdapter()).sample_table(
        table="t", schema="s", limit=10, order_by=None, output_format="xml"
    )
    assert result["ok"] is False
    assert result["error"]["code"] == "invalid_format"
