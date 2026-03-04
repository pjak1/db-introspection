from pathlib import Path

import pytest

from src.adapters.factory import create_adapter
from src.config import Settings
from src.errors import ConfigError


def _base_env(monkeypatch, tmp_path: Path, *, dialect: str):
    conn_file = tmp_path / "db_conn.txt"
    conn_file.write_text(f"dialect:{dialect}\n", encoding="utf-8")
    monkeypatch.setattr("src.config._default_conn_file_path", lambda: conn_file)
    monkeypatch.delenv("POSTGRES_DSN", raising=False)
    monkeypatch.delenv("ORACLE_DSN", raising=False)
    monkeypatch.delenv("MSSQL_DSN", raising=False)
    monkeypatch.delenv("DB_DIALECT", raising=False)
    monkeypatch.setenv("DB_ALLOWED_SCHEMAS", "public")


def test_settings_oracle_from_env(monkeypatch, tmp_path: Path):
    _base_env(monkeypatch, tmp_path, dialect="oracle")
    monkeypatch.setenv("ORACLE_DSN", "user/pass@localhost:1521/XEPDB1")
    settings = Settings.from_env()
    assert settings.db_dialect == "oracle"
    assert settings.db_dsn == "user/pass@localhost:1521/XEPDB1"


def test_settings_mssql_from_env(monkeypatch, tmp_path: Path):
    _base_env(monkeypatch, tmp_path, dialect="mssql")
    monkeypatch.setenv(
        "MSSQL_DSN",
        "DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost,1433;DATABASE=db;UID=u;PWD=p;",
    )
    settings = Settings.from_env()
    assert settings.db_dialect == "mssql"
    assert "SERVER=localhost,1433" in settings.db_dsn


@pytest.mark.parametrize("alias", ["sqlserver", "sql_server", "sql-server"])
def test_settings_mssql_aliases_are_rejected(monkeypatch, tmp_path: Path, alias: str):
    _base_env(monkeypatch, tmp_path, dialect=alias)
    monkeypatch.setenv(
        "MSSQL_DSN",
        "DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost,1433;DATABASE=db;UID=u;PWD=p;",
    )

    with pytest.raises(ConfigError) as exc:
        Settings.from_env()

    assert exc.value.code == "invalid_config"
    assert exc.value.message == f"Unsupported DB_DIALECT: {alias}"


def test_factory_creates_oracle_adapter():
    settings = Settings(
        db_dialect="oracle",
        db_dsn="user/pass@localhost:1521/XEPDB1",
        allowed_schemas=("HR",),
        default_sample_limit=10,
        max_sample_limit=100,
        max_select_limit=200,
        statement_timeout_ms=5000,
        include_system_schemas=False,
    )
    adapter = create_adapter(settings)
    assert adapter.dialect == "oracle"


def test_factory_creates_mssql_adapter():
    settings = Settings(
        db_dialect="mssql",
        db_dsn="DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost,1433;DATABASE=db;",
        allowed_schemas=("dbo",),
        default_sample_limit=10,
        max_sample_limit=100,
        max_select_limit=200,
        statement_timeout_ms=5000,
        include_system_schemas=False,
    )
    adapter = create_adapter(settings)
    assert adapter.dialect == "mssql"
