from pathlib import Path

import pytest

from src.config import Settings, read_connection_file
from src.errors import ConfigError


def test_env_ref_expands_into_connection_values(monkeypatch, tmp_path: Path):
    conn_file = tmp_path / "db_conn.txt"
    conn_file.write_text(
        "\n".join(
            [
                "dialect:postgres",
                "username:${SECRET_USER}",
                "password:${SECRET_PASS}",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("SECRET_USER", "resolved_user")
    monkeypatch.setenv("SECRET_PASS", "resolved_pass")

    values = read_connection_file(conn_file)

    assert values["username"] == "resolved_user"
    assert values["password"] == "resolved_pass"


def test_env_ref_expands_into_dsn(monkeypatch, tmp_path: Path):
    conn_file = tmp_path / "db_conn.txt"
    conn_file.write_text(
        "\n".join(
            [
                "dialect:postgres",
                "host:example-host",
                "db_name:example_db",
                "port:5432",
                "username:${SECRET_USER}",
                "password:${SECRET_PASS}",
                "schema:example_schema",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr("src.config._default_conn_file_path", lambda: conn_file)
    monkeypatch.setenv("SECRET_USER", "env_user")
    monkeypatch.setenv("SECRET_PASS", "env_pass")
    monkeypatch.delenv("POSTGRES_DSN", raising=False)
    monkeypatch.delenv("DB_ALLOWED_SCHEMAS", raising=False)

    settings = Settings.from_env()

    assert settings.db_dsn == "postgresql://env_user:env_pass@example-host:5432/example_db"


def test_missing_env_ref_fails_with_variable_name_not_value(monkeypatch, tmp_path: Path):
    conn_file = tmp_path / "db_conn.txt"
    conn_file.write_text(
        "\n".join(
            [
                "dialect:postgres",
                "password:${MISSING_SECRET}",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.delenv("MISSING_SECRET", raising=False)

    with pytest.raises(ConfigError) as exc:
        read_connection_file(conn_file)

    assert exc.value.code == "invalid_config"
    assert "MISSING_SECRET" in exc.value.message
    assert "password" in exc.value.message


def test_literal_values_without_ref_are_unchanged(monkeypatch, tmp_path: Path):
    conn_file = tmp_path / "db_conn.txt"
    conn_file.write_text(
        "\n".join(
            [
                "dialect:postgres",
                "username:literal_user",
                "password:p@ss$word",
            ]
        ),
        encoding="utf-8",
    )

    values = read_connection_file(conn_file)

    assert values["username"] == "literal_user"
    # A bare '$' without ${...} stays literal (backward compatible).
    assert values["password"] == "p@ss$word"


def test_escaped_ref_yields_literal_without_expanding(monkeypatch, tmp_path: Path):
    conn_file = tmp_path / "db_conn.txt"
    conn_file.write_text(
        "\n".join(
            [
                "dialect:postgres",
                # '$${NOT_A_VAR}' must become the literal '${NOT_A_VAR}' and must
                # NOT require NOT_A_VAR to exist in the environment.
                "password:$${NOT_A_VAR}",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.delenv("NOT_A_VAR", raising=False)

    values = read_connection_file(conn_file)

    assert values["password"] == "${NOT_A_VAR}"


def test_double_dollar_not_before_brace_stays_literal(tmp_path: Path):
    conn_file = tmp_path / "db_conn.txt"
    conn_file.write_text(
        "\n".join(
            [
                "dialect:postgres",
                # '$$' only escapes when it precedes '{'; elsewhere it is literal.
                "password:pa$$word",
            ]
        ),
        encoding="utf-8",
    )

    values = read_connection_file(conn_file)

    assert values["password"] == "pa$$word"
