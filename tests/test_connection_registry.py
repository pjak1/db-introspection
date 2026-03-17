from __future__ import annotations

from pathlib import Path

import pytest

import src.services.connection_registry as registry_module
from src.adapters.base import AdapterResult, DatabaseAdapter
from src.errors import ValidationError
from src.services.connection_registry import ConnectionRegistry


class DummyAdapter(DatabaseAdapter):
    @property
    def dialect(self) -> str:
        return "postgres"

    def list_tables(self, schemas: tuple[str, ...], include_system: bool) -> AdapterResult:
        return AdapterResult(data=[])

    def list_columns(self, table: str, schemas: tuple[str, ...]) -> AdapterResult:
        return AdapterResult(data=[])

    def list_constraints(
        self,
        schemas: tuple[str, ...],
        table: str | None = None,
        constraint_type: str | None = None,
    ) -> AdapterResult:
        return AdapterResult(data=[])

    def list_sequences(self, schemas: tuple[str, ...]) -> AdapterResult:
        return AdapterResult(data=[])

    def list_procedures(self, schemas: tuple[str, ...]) -> AdapterResult:
        return AdapterResult(data=[])

    def list_functions(self, schemas: tuple[str, ...]) -> AdapterResult:
        return AdapterResult(data=[])

    def list_jobs(self) -> AdapterResult:
        return AdapterResult(data=[])

    def sample_table(
        self,
        schema: str,
        table: str,
        limit: int,
        order_by: str | None,
    ) -> AdapterResult:
        return AdapterResult(data=[])

    def select_columns(
        self,
        schema: str,
        table: str,
        columns: list[str],
        limit: int,
    ) -> AdapterResult:
        return AdapterResult(data=[])

    def run_select(self, sql_query: str, timeout_ms: int) -> AdapterResult:
        return AdapterResult(data=[])

    def explain_select(self, sql_query: str, timeout_ms: int) -> AdapterResult:
        return AdapterResult(data=[])


def _write_conn(path: Path, *, schema: str = "public") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(
            [
                "dialect:postgres",
                "host:localhost",
                "db_name:test_db",
                "port:5432",
                "username:test_user",
                "password:test_pass",
                f"schema:{schema}",
            ]
        ),
        encoding="utf-8",
    )


def _set_project_root(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(ConnectionRegistry,
                        "resolve_project_root", lambda self: tmp_path)


def test_list_connections_reads_hierarchical_db_conns_directory(monkeypatch, tmp_path: Path):
    _write_conn(tmp_path / "DB_conns" / "A" / "DEV" / "dbo" / "db_conn.txt")
    _write_conn(tmp_path / "DB_conns" / "A" / "INT" / "dbo" / "db_conn.txt")
    _write_conn(tmp_path / "DB_conns" / "B" / "db_conn.txt")
    _write_conn(tmp_path / "DB_conns" / "C" / "X" / "Y" / "Z" / "db_conn.txt")

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
    _write_conn(conn_file, schema="first_schema")

    _set_project_root(monkeypatch, tmp_path)
    monkeypatch.setattr(registry_module, "create_adapter",
                        lambda settings: DummyAdapter())
    registry = ConnectionRegistry()

    first_introspection, first_select = registry.get_services(
        "PROJECT_X/DEV/schema_x")
    assert first_introspection._settings.allowed_schemas == ("first_schema",)

    _write_conn(conn_file, schema="second_schema")
    second_introspection, second_select = registry.get_services(
        "PROJECT_X\\DEV\\schema_x")

    assert second_introspection._settings.allowed_schemas == ("second_schema",)
    assert second_introspection is not first_introspection
    assert second_select is not first_select
