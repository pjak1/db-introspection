from pathlib import Path

import pytest

from src.adapters.mssql import MssqlAdapter
from src.adapters.oracle import OracleAdapter
from src.adapters.postgres import PostgresAdapter
from src.errors import ConfigError
from src.services.connection_registry import ConnectionRegistry


def test_postgres_open_connection_returns_driver_connection(monkeypatch):
    sentinel = object()
    monkeypatch.setattr("src.adapters.postgres.psycopg.connect", lambda *a, **k: sentinel)

    assert PostgresAdapter(dsn="postgresql://u:p@h:5432/db").open_connection() is sentinel


def test_oracle_open_connection_returns_driver_connection(monkeypatch):
    sentinel = object()
    monkeypatch.setattr("oracledb.connect", lambda *a, **k: sentinel)

    assert OracleAdapter(dsn="u/p@h:1521/svc").open_connection() is sentinel


def test_mssql_open_connection_returns_driver_connection(monkeypatch):
    sentinel = object()
    monkeypatch.setattr("pyodbc.connect", lambda *a, **k: sentinel)

    assert MssqlAdapter(dsn="Driver=test").open_connection() is sentinel


def test_build_settings_returns_settings_for_connection(monkeypatch, tmp_path: Path):
    conn_file = tmp_path / "db_conn.txt"
    conn_file.write_text(
        "\n".join(
            [
                "dialect:postgres",
                "host:example-host",
                "db_name:example_db",
                "port:5432",
                "username:example_user",
                "password:example_pass",
                "schema:example_schema",
            ]
        ),
        encoding="utf-8",
    )
    registry = ConnectionRegistry()
    monkeypatch.setattr(registry, "resolve_conn_file", lambda connection: conn_file)
    monkeypatch.delenv("POSTGRES_DSN", raising=False)
    monkeypatch.delenv("DB_ALLOWED_SCHEMAS", raising=False)

    settings = registry.build_settings("PROJECT_A/DEV/example_schema")

    assert settings.db_dialect == "postgres"
    assert settings.db_dsn == "postgresql://example_user:example_pass@example-host:5432/example_db"


def test_build_settings_empty_file_raises_config_error(monkeypatch, tmp_path: Path):
    conn_file = tmp_path / "db_conn.txt"
    conn_file.write_text("", encoding="utf-8")
    registry = ConnectionRegistry()
    monkeypatch.setattr(registry, "resolve_conn_file", lambda connection: conn_file)

    with pytest.raises(ConfigError) as exc:
        registry.build_settings("PROJECT_A/DEV/example_schema")

    assert exc.value.code == "invalid_config"
