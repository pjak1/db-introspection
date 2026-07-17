from __future__ import annotations

import json
from typing import TYPE_CHECKING

from src.errors import ConfigError

if TYPE_CHECKING:  # pragma: no cover - typing only
    from types import ModuleType

# Connection-file values may reference a secret held in the OS keychain (Windows
# Credential Manager / macOS Keychain / Linux Secret Service) instead of a
# plaintext `${VAR}`/.env value, e.g. `password:credential://MY_DB_PASSWORD`.
# All keychain access is centralized in this class so `config` and the secrets
# CLI share one implementation; `keyring` is imported lazily so connections that
# never use `credential://` need nothing installed or configured.


class SecretStore:
    """Read/write DB secrets in the OS keychain via the `keyring` library.

    The store is addressed by a fixed service name; individual secrets are keyed
    by a caller-chosen name. A small JSON index (stored under a reserved key)
    tracks the set names, because keyring cannot portably enumerate them.
    """

    SECRET_SCHEME = "credential://"
    DEFAULT_SERVICE = "db-introspection-mcp"
    _INDEX_KEY = "__secret_index__"

    def __init__(self, service: str = DEFAULT_SERVICE) -> None:
        self.service = service

    @classmethod
    def is_secret_ref(cls, value: str) -> bool:
        """True when a connection-file value points at a keychain secret."""
        return value.startswith(cls.SECRET_SCHEME)

    def resolve(self, field: str, value: str) -> str:
        """Resolve a `credential://<name>` value to the stored secret.

        Fail-closed: raises ConfigError (naming `field`) when the reference is
        empty, the secret is not set, or the keychain backend is unavailable —
        mirroring the unset-`${VAR}` error so a typo surfaces immediately.
        """
        name = value[len(self.SECRET_SCHEME):].strip()
        if not name:
            raise ConfigError(
                "invalid_config",
                f"Connection field '{field}' has an empty "
                f"'{self.SECRET_SCHEME}' reference.",
            )
        secret = self.get(name)
        if secret is None:
            raise ConfigError(
                "invalid_config",
                f"Connection field '{field}' references keychain secret "
                f"'{name}', which is not set. Store it with: "
                f"python -m src.secrets set {name}",
            )
        return secret

    def get(self, name: str) -> str | None:
        """Return the stored secret for `name`, or None when it is not set."""
        keyring = self._keyring()
        try:
            return keyring.get_password(self.service, name)
        except Exception as exc:  # backend unavailable/locked
            raise ConfigError(
                "invalid_config",
                f"OS keychain is unavailable: {exc}",
            ) from exc

    def set(self, name: str, value: str) -> None:
        """Store `value` under `name` and record it in the index."""
        self._reject_reserved(name)
        keyring = self._keyring()
        keyring.set_password(self.service, name, value)
        self._index_update(add=name)

    def delete(self, name: str) -> None:
        """Remove `name` from the keychain (best-effort) and the index."""
        self._reject_reserved(name)
        keyring = self._keyring()
        try:
            keyring.delete_password(self.service, name)
        except self._keyring_errors().PasswordDeleteError:
            pass  # already absent — deletion is idempotent
        self._index_update(remove=name)

    def list_names(self) -> list[str]:
        """Return the sorted names of secrets stored by this store."""
        return self._load_index()

    # -- internals ---------------------------------------------------------

    def _reject_reserved(self, name: str) -> None:
        if name == self._INDEX_KEY:
            raise ConfigError(
                "invalid_config",
                f"'{name}' is a reserved secret name.",
            )

    def _load_index(self) -> list[str]:
        raw = self.get(self._INDEX_KEY)
        if not raw:
            return []
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return []
        return sorted(str(item) for item in data) if isinstance(data, list) else []

    def _index_update(self, *, add: str | None = None, remove: str | None = None) -> None:
        names = set(self._load_index())
        if add is not None:
            names.add(add)
        if remove is not None:
            names.discard(remove)
        self._keyring().set_password(
            self.service, self._INDEX_KEY, json.dumps(sorted(names))
        )

    def _keyring(self) -> "ModuleType":
        try:
            import keyring
        except ImportError as exc:
            raise ConfigError(
                "invalid_config",
                "The 'keyring' package is required to use credential:// "
                "secrets. Install it with: pip install keyring",
            ) from exc
        return keyring

    def _keyring_errors(self) -> "ModuleType":
        import keyring.errors

        return keyring.errors
