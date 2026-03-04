from pathlib import Path

import pytest

from src.config import Settings
from src.errors import ConfigError


def test_settings_loads_dsn_and_schema_from_db_conn_file(monkeypatch, tmp_path: Path):
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
    monkeypatch.setattr("src.config._default_conn_file_path", lambda: conn_file)
    monkeypatch.delenv("POSTGRES_DSN", raising=False)
    monkeypatch.delenv("DB_ALLOWED_SCHEMAS", raising=False)

    settings = Settings.from_env()
    assert settings.db_dsn == "postgresql://example_user:example_pass@example-host:5432/example_db"
    assert settings.allowed_schemas == ("example_schema",)


def test_env_schema_overrides_conn_file_schema(monkeypatch, tmp_path: Path):
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
                "schema:from_file",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr("src.config._default_conn_file_path", lambda: conn_file)
    monkeypatch.delenv("POSTGRES_DSN", raising=False)
    monkeypatch.setenv("DB_ALLOWED_SCHEMAS", "from_env")

    settings = Settings.from_env()
    assert settings.allowed_schemas == ("from_env",)


def test_from_env_ignores_db_conn_file_env(monkeypatch, tmp_path: Path):
    valid_conn = tmp_path / "db_conn.txt"
    valid_conn.write_text(
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
    monkeypatch.setattr("src.config._default_conn_file_path", lambda: valid_conn)
    monkeypatch.setenv("DB_CONN_FILE", str(tmp_path / "wrong_conn.txt"))
    monkeypatch.delenv("POSTGRES_DSN", raising=False)
    monkeypatch.delenv("DB_ALLOWED_SCHEMAS", raising=False)

    settings = Settings.from_env()
    assert settings.db_dsn == "postgresql://example_user:example_pass@example-host:5432/example_db"


def test_from_env_missing_default_conn_file_returns_path_in_error(monkeypatch, tmp_path: Path):
    missing = tmp_path / "db_conn.txt"
    monkeypatch.setattr("src.config._default_conn_file_path", lambda: missing)
    monkeypatch.delenv("POSTGRES_DSN", raising=False)
    monkeypatch.delenv("DB_ALLOWED_SCHEMAS", raising=False)

    with pytest.raises(ConfigError) as exc:
        Settings.from_env()

    assert exc.value.code == "invalid_config"
    assert str(missing) in exc.value.message


def test_from_env_missing_dialect_fails_fast(monkeypatch, tmp_path: Path):
    conn_file = tmp_path / "db_conn.txt"
    conn_file.write_text(
        "\n".join(
            [
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
    monkeypatch.setattr("src.config._default_conn_file_path", lambda: conn_file)
    monkeypatch.setenv("DB_DIALECT", "oracle")
    monkeypatch.delenv("POSTGRES_DSN", raising=False)
    monkeypatch.delenv("ORACLE_DSN", raising=False)
    monkeypatch.delenv("MSSQL_DSN", raising=False)

    with pytest.raises(ConfigError) as exc:
        Settings.from_env()

    assert exc.value.code == "invalid_config"
    assert "Missing required 'dialect' in db_conn.txt" in exc.value.message


def test_from_env_invalid_dialect_fails_fast(monkeypatch, tmp_path: Path):
    conn_file = tmp_path / "db_conn.txt"
    conn_file.write_text(
        "\n".join(
            [
                "dialect:unknown",
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
    monkeypatch.setattr("src.config._default_conn_file_path", lambda: conn_file)
    monkeypatch.delenv("POSTGRES_DSN", raising=False)
    monkeypatch.delenv("ORACLE_DSN", raising=False)
    monkeypatch.delenv("MSSQL_DSN", raising=False)

    with pytest.raises(ConfigError) as exc:
        Settings.from_env()

    assert exc.value.code == "invalid_config"
    assert exc.value.message == "Unsupported DB_DIALECT: unknown"


def test_from_env_mssql_dialect_is_supported(monkeypatch, tmp_path: Path):
    conn_file = tmp_path / "db_conn.txt"
    conn_file.write_text(
        "\n".join(
            [
                "dialect:mssql",
                "host:example-host",
                "db_name:example_db",
                "port:1433",
                "username:example_user",
                "password:example_pass",
                "schema:dbo",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr("src.config._default_conn_file_path", lambda: conn_file)
    monkeypatch.delenv("MSSQL_DSN", raising=False)
    monkeypatch.delenv("DB_ALLOWED_SCHEMAS", raising=False)

    settings = Settings.from_env()

    assert settings.db_dialect == "mssql"
    assert "SERVER=example-host,1433" in settings.db_dsn


@pytest.mark.parametrize("alias", ["sqlserver", "sql_server", "sql-server"])
def test_from_env_mssql_aliases_are_rejected(monkeypatch, tmp_path: Path, alias: str):
    conn_file = tmp_path / "db_conn.txt"
    conn_file.write_text(
        "\n".join(
            [
                f"dialect:{alias}",
                "host:example-host",
                "db_name:example_db",
                "port:1433",
                "username:example_user",
                "password:example_pass",
                "schema:dbo",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr("src.config._default_conn_file_path", lambda: conn_file)
    monkeypatch.delenv("POSTGRES_DSN", raising=False)
    monkeypatch.delenv("ORACLE_DSN", raising=False)
    monkeypatch.delenv("MSSQL_DSN", raising=False)

    with pytest.raises(ConfigError) as exc:
        Settings.from_env()

    assert exc.value.code == "invalid_config"
    assert exc.value.message == f"Unsupported DB_DIALECT: {alias}"
