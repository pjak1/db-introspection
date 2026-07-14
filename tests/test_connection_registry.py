from __future__ import annotations

from pathlib import Path

import pytest
from conftest import BaseStubAdapter, write_conn_file

import src.services.connection_registry as registry_module
from src.errors import ValidationError
from src.services.connection_registry import ConnectionRegistry


class DummyAdapter(BaseStubAdapter):
    pass


def _set_project_root(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(ConnectionRegistry, "resolve_project_root", lambda self: tmp_path)


def test_list_connections_reads_hierarchical_db_conns_directory(monkeypatch, tmp_path: Path):
    write_conn_file(tmp_path / "DB_conns" / "A" / "DEV" / "dbo" / "db_conn.txt")
    write_conn_file(tmp_path / "DB_conns" / "A" / "INT" / "dbo" / "db_conn.txt")
    write_conn_file(tmp_path / "DB_conns" / "B" / "db_conn.txt")
    write_conn_file(tmp_path / "DB_conns" / "C" / "X" / "Y" / "Z" / "db_conn.txt")

    _set_project_root(monkeypatch, tmp_path)
    registry = ConnectionRegistry()

    assert registry.list_connections() == ["A/DEV/dbo", "A/INT/dbo"]


def test_list_connections_returns_empty_when_db_conns_missing(monkeypatch, tmp_path: Path):
    _set_project_root(monkeypatch, tmp_path)
    registry = ConnectionRegistry()

    assert registry.list_connections() == []


@pytest.mark.parametrize(
    "raw_key, expected",
    [
        ("A/DEV/dbo", "A/DEV/dbo"),
        ("A\\DEV\\dbo", "A/DEV/dbo"),
        ("A//DEV///dbo", "A/DEV/dbo"),
    ],
)
def test_normalize_connection_canonicalizes_separators(raw_key: str, expected: str):
    assert ConnectionRegistry._normalize_connection(raw_key) == expected


@pytest.mark.parametrize(
    "raw_key",
    [
        "",
        "A/DEV",
        "A/DEV/dbo/extra",
        "A//dbo",
        "A/./dbo",
        "A/../dbo",
    ],
)
def test_normalize_connection_rejects_invalid_paths(raw_key: str):
    with pytest.raises(ValidationError):
        ConnectionRegistry._normalize_connection(raw_key)


def test_get_services_rebuilds_cache_when_conn_file_changes(monkeypatch, tmp_path: Path):
    conn_file = tmp_path / "DB_conns" / "PROJECT_X" / "DEV" / "schema_x" / "db_conn.txt"
    write_conn_file(conn_file, schema="first_schema")

    _set_project_root(monkeypatch, tmp_path)
    monkeypatch.setattr(registry_module, "create_adapter", lambda settings: DummyAdapter())
    registry = ConnectionRegistry()

    first_introspection, first_select = registry.get_services("PROJECT_X/DEV/schema_x")
    assert first_introspection._settings.allowed_schemas == ("first_schema",)

    write_conn_file(conn_file, schema="second_schema")
    second_introspection, second_select = registry.get_services("PROJECT_X\\DEV\\schema_x")

    assert second_introspection._settings.allowed_schemas == ("second_schema",)
    assert second_introspection is not first_introspection
    assert second_select is not first_select
