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


def test_schema_csv_in_conn_file_drives_allowed_schemas(monkeypatch, tmp_path: Path):
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
                "schema:schema_a,schema_b,schema_c",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr("src.config._default_conn_file_path", lambda: conn_file)
    # A global env value must NOT influence a per-connection setting anymore.
    monkeypatch.setenv("DB_ALLOWED_SCHEMAS", "from_env")

    settings = Settings.from_env()
    assert settings.allowed_schemas == ("schema_a", "schema_b", "schema_c")


def test_per_connection_limits_read_from_conn_file(monkeypatch, tmp_path: Path):
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
                "default_sample_limit:5",
                "max_sample_limit:50",
                "max_select_limit:500",
                "statement_timeout_ms:10000",
                "include_system_schemas:true",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr("src.config._default_conn_file_path", lambda: conn_file)
    # These global env vars must be ignored entirely.
    monkeypatch.setenv("DB_MAX_SELECT_LIMIT", "1")
    monkeypatch.setenv("DB_STATEMENT_TIMEOUT_MS", "1")
    monkeypatch.setenv("DB_INCLUDE_SYSTEM_SCHEMAS", "false")

    settings = Settings.from_env()
    assert settings.default_sample_limit == 5
    assert settings.max_sample_limit == 50
    assert settings.max_select_limit == 500
    assert settings.statement_timeout_ms == 10000
    assert settings.include_system_schemas is True


def test_per_connection_limits_default_when_absent(monkeypatch, tmp_path: Path):
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

    settings = Settings.from_env()
    assert settings.default_sample_limit == 10
    assert settings.max_sample_limit == 100
    assert settings.max_select_limit == 200
    assert settings.statement_timeout_ms == 5000
    assert settings.include_system_schemas is False


def test_non_integer_limit_in_conn_file_fails_fast(monkeypatch, tmp_path: Path):
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
                "max_select_limit:not_a_number",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr("src.config._default_conn_file_path", lambda: conn_file)

    with pytest.raises(ConfigError) as exc:
        Settings.from_env()

    assert exc.value.code == "invalid_config"
    assert "max_select_limit" in exc.value.message


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
    assert exc.value.message == "Unsupported dialect: unknown"


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
    assert exc.value.message == f"Unsupported dialect: {alias}"


def test_credential_ref_password_flows_into_dsn(monkeypatch, tmp_path: Path):
    # A `credential://` password is resolved from the OS keychain and used just
    # like a plaintext/`${VAR}` password when building the DSN.
    from src.secret_store import SecretStore

    class _Keyring:
        def get_password(self, service, name):
            return "kc_pass" if name == "PW" else None

    monkeypatch.setattr(SecretStore, "_keyring", lambda self: _Keyring())

    conn_file = tmp_path / "db_conn.txt"
    conn_file.write_text(
        "\n".join(
            [
                "dialect:postgres",
                "host:example-host",
                "db_name:example_db",
                "port:5432",
                "username:example_user",
                "password:credential://PW",
                "schema:example_schema",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr("src.config._default_conn_file_path", lambda: conn_file)

    settings = Settings.from_env()
    assert settings.db_dsn == "postgresql://example_user:kc_pass@example-host:5432/example_db"


def test_plugin_namespaced_keys_are_ignored_by_core():
    # Plugin-owned `plugin.*` keys must never disturb core Settings, so a
    # dropped-in plugin can add per-connection config without a core change.
    settings = Settings.from_connection_values(
        conn_values={
            "dialect": "postgres",
            "host": "example-host",
            "db_name": "example_db",
            "port": "5432",
            "username": "example_user",
            "password": "example_pass",
            "schema": "example_schema",
            "plugin.write.mode": "dry_run",
        }
    )

    assert settings.db_dsn == "postgresql://example_user:example_pass@example-host:5432/example_db"
    assert settings.allowed_schemas == ("example_schema",)
