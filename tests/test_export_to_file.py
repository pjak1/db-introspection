from __future__ import annotations

import json
from pathlib import Path

import pytest
from conftest import BaseStubAdapter, make_settings

from src.adapters._sql_helpers import stream_cursor_to_file
from src.adapters.base import AdapterResult
from src.errors import ValidationError
from src.services.export import (
    effective_export_limit,
    normalize_export_format,
    resolve_export_path,
)
from src.services.introspection_service import IntrospectionService
from src.services.select_service import SelectService


class FakeCursor:
    """Minimal DBAPI-ish cursor for exercising the streaming writer."""

    def __init__(self, columns, rows):
        self.description = [(name,) for name in columns]
        self._rows = list(rows)
        self.arraysize = 1

    def fetchmany(self, size):
        batch = self._rows[:size]
        self._rows = self._rows[size:]
        return batch


# --- streaming writer ------------------------------------------------------

def test_stream_cursor_csv_writes_header_and_blank_for_none(tmp_path):
    cur = FakeCursor(["id", "name"], [(1, "a"), (2, None)])
    dest = tmp_path / "out.csv"
    result = stream_cursor_to_file(cur, dest, "csv", max_rows=100)
    assert result.data["row_count"] == 2
    assert result.data["truncated"] is False
    assert result.data["byte_size"] > 0
    lines = dest.read_text(encoding="utf-8").splitlines()
    assert lines == ["id,name", "1,a", "2,"]


def test_stream_cursor_json_roundtrips(tmp_path):
    cur = FakeCursor(["id", "name"], [(1, "a"), (2, None)])
    dest = tmp_path / "out.json"
    stream_cursor_to_file(cur, dest, "json", max_rows=100)
    assert json.loads(dest.read_text(encoding="utf-8")) == [
        {"id": 1, "name": "a"},
        {"id": 2, "name": None},
    ]


def test_stream_cursor_truncates_at_cap(tmp_path):
    # Adapter caps the query at max_rows + 1, so a surviving extra row = truncated.
    cur = FakeCursor(["id"], [(1,), (2,)])
    result = stream_cursor_to_file(cur, tmp_path / "o.csv", "csv", max_rows=1)
    assert result.data["row_count"] == 1
    assert result.data["truncated"] is True


def test_stream_cursor_empty_result(tmp_path):
    cur = FakeCursor(["id"], [])
    result = stream_cursor_to_file(cur, tmp_path / "o.csv", "csv", max_rows=100)
    assert result.data["row_count"] == 0
    assert result.data["truncated"] is False
    # Header only (read_text normalizes the CSV's \r\n line ending to \n).
    assert Path(result.data["path"]).read_text(encoding="utf-8").splitlines() == ["id"]


# --- format / path / limit helpers ----------------------------------------

def test_normalize_export_format_defaults_and_rejects_rows():
    assert normalize_export_format(None) == "csv"
    assert normalize_export_format("JSON") == "json"
    with pytest.raises(ValidationError) as exc:
        normalize_export_format("rows")
    assert exc.value.code == "invalid_format"


@pytest.mark.parametrize("bad", ["../x.csv", "a/b.csv", "a\\b.csv", "..", "sub/../x.csv"])
def test_resolve_export_path_rejects_unsafe_names(monkeypatch, tmp_path, bad):
    monkeypatch.setenv("DB_INTROSPECTION_EXPORT_DIR", str(tmp_path))
    with pytest.raises(ValidationError) as exc:
        resolve_export_path(bad, "csv", "default")
    assert exc.value.code == "invalid_filename"


def test_resolve_export_path_appends_extension_and_stays_inside(monkeypatch, tmp_path):
    monkeypatch.setenv("DB_INTROSPECTION_EXPORT_DIR", str(tmp_path))
    dest = resolve_export_path("myfile", "csv", "default")
    assert dest.name == "myfile.csv"
    assert dest.parent == tmp_path.resolve()


def test_resolve_export_path_sanitizes_default_stem(monkeypatch, tmp_path):
    monkeypatch.setenv("DB_INTROSPECTION_EXPORT_DIR", str(tmp_path))
    dest = resolve_export_path(None, "json", "public.my table")
    assert dest.name == "public.my_table.json"


def test_effective_export_limit_clamps_to_ceiling():
    assert effective_export_limit(None, 1000) == (1000, [])
    assert effective_export_limit(50, 1000) == (50, [])
    value, warnings = effective_export_limit(5000, 1000)
    assert value == 1000
    assert warnings and "reduced" in warnings[0]


# --- service wiring --------------------------------------------------------

class RecordingExportAdapter(BaseStubAdapter):
    """Captures export calls and writes a marker file to prove the path wiring."""

    def __init__(self):
        self.calls: list[tuple] = []

    def export_query(self, sql_query, destination, fmt, timeout_ms, max_rows) -> AdapterResult:
        self.calls.append(("query", sql_query, Path(destination), fmt, timeout_ms, max_rows))
        Path(destination).write_text("marker", encoding="utf-8")
        return AdapterResult(data={
            "path": str(destination), "format": fmt,
            "row_count": 3, "byte_size": 6, "truncated": max_rows < 10,
        })

    def export_table(
        self, schema, table, columns, order_by, destination, fmt, timeout_ms, max_rows
    ) -> AdapterResult:
        self.calls.append(("table", schema, table, columns, order_by, Path(destination), fmt, max_rows))
        return AdapterResult(data={
            "path": str(destination), "format": fmt,
            "row_count": 3, "byte_size": 6, "truncated": False,
        }, schema_used=schema)


def _intro(adapter, **overrides) -> IntrospectionService:
    settings = make_settings(allowed_schemas=("public",), **overrides)
    return IntrospectionService(adapter=adapter, settings=settings)


def test_service_export_table_passes_through(monkeypatch, tmp_path):
    monkeypatch.setenv("DB_INTROSPECTION_EXPORT_DIR", str(tmp_path))
    adapter = RecordingExportAdapter()
    env = _intro(adapter).export_table(
        table="t", schema="public", columns=["a", "b"], order_by="a DESC",
        filename="out", output_format="csv", max_rows=None,
    )
    assert env["ok"] is True
    assert env["data"]["row_count"] == 3
    kind, schema, table, columns, order_by, dest, fmt, max_rows = adapter.calls[0]
    assert kind == "table"
    assert (schema, table, columns, order_by, fmt) == ("public", "t", ["a", "b"], "a DESC", "csv")
    assert max_rows == 1_000_000  # default ceiling
    assert dest.name == "out.csv"


def test_service_export_table_rejects_bad_schema(monkeypatch, tmp_path):
    monkeypatch.setenv("DB_INTROSPECTION_EXPORT_DIR", str(tmp_path))
    env = _intro(RecordingExportAdapter()).export_table(
        table="t", schema="nope", columns=None, order_by=None,
        filename=None, output_format="csv", max_rows=None,
    )
    assert env["ok"] is False
    assert env["error"]["code"] == "invalid_schema"


def test_service_export_table_rejects_bad_column(monkeypatch, tmp_path):
    monkeypatch.setenv("DB_INTROSPECTION_EXPORT_DIR", str(tmp_path))
    env = _intro(RecordingExportAdapter()).export_table(
        table="t", schema="public", columns=["ok", "bad; drop"], order_by=None,
        filename=None, output_format="csv", max_rows=None,
    )
    assert env["ok"] is False
    assert env["error"]["code"] == "invalid_columns"


def test_service_export_select_streams_and_warns_on_truncation(monkeypatch, tmp_path):
    monkeypatch.setenv("DB_INTROSPECTION_EXPORT_DIR", str(tmp_path))
    adapter = RecordingExportAdapter()
    service = SelectService(adapter=adapter, settings=make_settings())
    env = service.export_select(
        sql_query="SELECT 1", filename="q", output_format="json",
        timeout_ms=None, max_rows=5,
    )
    assert env["ok"] is True
    kind, sql, dest, fmt, timeout_ms, max_rows = adapter.calls[0]
    assert kind == "query"
    assert (fmt, max_rows) == ("json", 5)
    assert dest.name == "q.json"
    assert dest.read_text(encoding="utf-8") == "marker"
    # RecordingExportAdapter reports truncated when max_rows < 10.
    assert any("row cap" in w for w in env["meta"]["warnings"])


def test_service_export_select_reduces_over_ceiling(monkeypatch, tmp_path):
    monkeypatch.setenv("DB_INTROSPECTION_EXPORT_DIR", str(tmp_path))
    adapter = RecordingExportAdapter()
    service = SelectService(adapter=adapter, settings=make_settings(max_export_rows=100))
    env = service.export_select(
        sql_query="SELECT 1", filename=None, output_format="csv",
        timeout_ms=None, max_rows=5000,
    )
    assert env["ok"] is True
    assert adapter.calls[0][-1] == 100  # clamped to ceiling
    assert any("reduced" in w for w in env["meta"]["warnings"])


def test_service_export_rejects_rows_format(monkeypatch, tmp_path):
    monkeypatch.setenv("DB_INTROSPECTION_EXPORT_DIR", str(tmp_path))
    env = _intro(RecordingExportAdapter()).export_table(
        table="t", schema="public", columns=None, order_by=None,
        filename=None, output_format="rows", max_rows=None,
    )
    assert env["ok"] is False
    assert env["error"]["code"] == "invalid_format"
