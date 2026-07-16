from pathlib import Path

import pytest

from src.adapters.factory import create_adapter
from src.config import Settings
from src.errors import ConfigError


def _write_conn_file(monkeypatch, tmp_path: Path, lines: list[str]) -> None:
    conn_file = tmp_path / "db_conn.txt"
    conn_file.write_text("\n".join(lines) + "\n", encoding="utf-8")
    monkeypatch.setattr("src.config._default_conn_file_path", lambda: conn_file)


def test_settings_oracle_from_conn_file(monkeypatch, tmp_path: Path):
    _write_conn_file(
        monkeypatch,
        tmp_path,
        [
            "dialect:oracle",
            "host:localhost",
            "port:1521",
            "service_name:XEPDB1",
            "username:user",
            "password:pass",
            "schema:HR",
        ],
    )
    settings = Settings.from_env()
    assert settings.db_dialect == "oracle"
    assert settings.db_dsn == "user/pass@localhost:1521/XEPDB1"


def test_settings_mssql_from_conn_file(monkeypatch, tmp_path: Path):
    _write_conn_file(
        monkeypatch,
        tmp_path,
        [
            "dialect:mssql",
            "host:localhost",
            "port:1433",
            "db_name:db",
            "username:u",
            "password:p",
            "schema:dbo",
        ],
    )
    settings = Settings.from_env()
    assert settings.db_dialect == "mssql"
    assert "SERVER=localhost,1433" in settings.db_dsn


@pytest.mark.parametrize("alias", ["sqlserver", "sql_server", "sql-server"])
def test_settings_mssql_aliases_are_rejected(monkeypatch, tmp_path: Path, alias: str):
    _write_conn_file(
        monkeypatch,
        tmp_path,
        [
            f"dialect:{alias}",
            "host:localhost",
            "port:1433",
            "db_name:db",
            "username:u",
            "password:p",
            "schema:dbo",
        ],
    )

    with pytest.raises(ConfigError) as exc:
        Settings.from_env()

    assert exc.value.code == "invalid_config"
    assert exc.value.message == f"Unsupported dialect: {alias}"


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
