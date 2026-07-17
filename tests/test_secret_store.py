from __future__ import annotations

from pathlib import Path

import keyring.errors
import pytest

import src.config as config
from src.errors import ConfigError
from src.secret_store import SecretStore
from src import secrets as secrets_cli


class FakeKeyring:
    """In-memory stand-in for the `keyring` module used by SecretStore."""

    def __init__(self) -> None:
        self.store: dict[tuple[str, str], str] = {}

    def get_password(self, service: str, name: str) -> str | None:
        return self.store.get((service, name))

    def set_password(self, service: str, name: str, value: str) -> None:
        self.store[(service, name)] = value

    def delete_password(self, service: str, name: str) -> None:
        if (service, name) not in self.store:
            raise keyring.errors.PasswordDeleteError("not found")
        del self.store[(service, name)]


@pytest.fixture
def fake_keyring(monkeypatch) -> FakeKeyring:
    """Patch SecretStore so every instance talks to one in-memory backend."""
    fake = FakeKeyring()
    monkeypatch.setattr(SecretStore, "_keyring", lambda self: fake)
    return fake


@pytest.fixture
def store(fake_keyring: FakeKeyring) -> SecretStore:
    return SecretStore()


def test_is_secret_ref_detects_scheme():
    assert SecretStore.is_secret_ref("credential://X")
    assert not SecretStore.is_secret_ref("${X}")
    assert not SecretStore.is_secret_ref("plain_value")


def test_set_get_delete_round_trip(store: SecretStore):
    store.set("DB_PW", "s3cret")
    assert store.get("DB_PW") == "s3cret"

    store.delete("DB_PW")
    assert store.get("DB_PW") is None


def test_delete_is_idempotent(store: SecretStore):
    store.delete("NEVER_SET")  # must not raise


def test_list_names_tracks_set_and_delete(store: SecretStore):
    store.set("A", "1")
    store.set("B", "2")
    assert store.list_names() == ["A", "B"]

    store.delete("A")
    assert store.list_names() == ["B"]


def test_index_key_is_not_listed_or_writable(store: SecretStore):
    store.set("A", "1")
    assert SecretStore._INDEX_KEY not in store.list_names()

    with pytest.raises(ConfigError):
        store.set(SecretStore._INDEX_KEY, "x")


def test_resolve_returns_stored_secret(store: SecretStore):
    store.set("REZA_DEV_RPP_REZA_PASSWORD", "pw")
    assert store.resolve("password", "credential://REZA_DEV_RPP_REZA_PASSWORD") == "pw"


def test_resolve_missing_secret_raises_naming_field(store: SecretStore):
    with pytest.raises(ConfigError) as exc:
        store.resolve("password", "credential://NOT_SET")

    assert exc.value.code == "invalid_config"
    assert "password" in exc.value.message
    assert "NOT_SET" in exc.value.message


def test_resolve_empty_reference_raises(store: SecretStore):
    with pytest.raises(ConfigError) as exc:
        store.resolve("password", "credential://")

    assert exc.value.code == "invalid_config"


def test_backend_error_surfaces_as_config_error(monkeypatch):
    class BrokenKeyring:
        def get_password(self, service, name):
            raise RuntimeError("keychain locked")

    monkeypatch.setattr(SecretStore, "_keyring", lambda self: BrokenKeyring())
    with pytest.raises(ConfigError) as exc:
        SecretStore().get("ANY")

    assert exc.value.code == "invalid_config"
    assert "keychain" in exc.value.message.lower()


def test_read_connection_file_resolves_credential_and_env(
    monkeypatch, tmp_path: Path, fake_keyring: FakeKeyring
):
    # config uses a module-level SecretStore; the fake_keyring fixture patches
    # the class, so config's instance talks to the same in-memory backend.
    SecretStore().set("PW_FROM_KEYCHAIN", "kc_pw")
    monkeypatch.setenv("USER_FROM_ENV", "env_user")

    conn_file = tmp_path / "db_conn.txt"
    conn_file.write_text(
        "\n".join(
            [
                "dialect:postgres",
                "host:example-host",
                "db_name:example_db",
                "port:5432",
                "username:${USER_FROM_ENV}",
                "password:credential://PW_FROM_KEYCHAIN",
                "schema:example_schema",
            ]
        ),
        encoding="utf-8",
    )

    values = config.read_connection_file(conn_file)
    assert values["username"] == "env_user"  # ${VAR} path unchanged
    assert values["password"] == "kc_pw"  # credential:// resolved from keychain


# --- CLI ------------------------------------------------------------------


def test_cli_set_get_delete_list(fake_keyring: FakeKeyring, capsys):
    assert secrets_cli.main(["set", "PW", "--value", "abc"]) == 0
    assert secrets_cli.main(["get", "PW"]) == 0
    assert capsys.readouterr().out.splitlines()[-1] == "abc"

    assert secrets_cli.main(["list"]) == 0
    assert "PW" in capsys.readouterr().out

    assert secrets_cli.main(["delete", "PW"]) == 0
    assert secrets_cli.main(["get", "PW"]) == 1  # gone -> non-zero exit


def test_cli_import_env_stores_and_skips_non_secrets(
    fake_keyring: FakeKeyring, tmp_path: Path, capsys
):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "DB_INTROSPECTION_ENABLE_WRITE_PLUGINS=0",  # non-secret, skipped
                "PROJ_DEV_S_USERNAME=u",
                "PROJ_DEV_S_PASSWORD=p",
            ]
        ),
        encoding="utf-8",
    )

    assert secrets_cli.main(["import-env", "--file", str(env_file)]) == 0

    store = SecretStore()
    assert store.get("PROJ_DEV_S_USERNAME") == "u"
    assert store.get("PROJ_DEV_S_PASSWORD") == "p"
    assert store.get("DB_INTROSPECTION_ENABLE_WRITE_PLUGINS") is None

    out = capsys.readouterr().out
    assert "credential://PROJ_DEV_S_PASSWORD" in out
