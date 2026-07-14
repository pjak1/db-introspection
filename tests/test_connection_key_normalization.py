from src.plugins.api import PluginContext
from src.services.connection_registry import (
    ConnectionRegistry,
    normalize_connection_key,
)


def test_normalize_connection_key_canonicalizes_separators():
    assert normalize_connection_key("A\\DEV\\s") == "A/DEV/s"
    assert normalize_connection_key("A//DEV///s") == "A/DEV/s"
    assert normalize_connection_key("  A/DEV/s  ") == "A/DEV/s"
    assert normalize_connection_key(None) == ""


def test_registry_strict_normalization_agrees_with_shared_key():
    # The registry adds strict 3-part validation on top of the same canonical key,
    # so for a valid input both must produce the identical string.
    registry = ConnectionRegistry()
    for raw in ("A\\DEV\\s", "A//DEV//s", " A/DEV/s "):
        assert registry._normalize_connection(raw) == normalize_connection_key(raw)


def test_write_allowlist_uses_same_canonical_key(monkeypatch):
    # Allowlist entry and the incoming connection differ only in separators/whitespace;
    # both must canonicalize identically so the allowlist check matches.
    monkeypatch.setenv("DB_WRITABLE_CONNECTIONS", " PROJECT_A/DEV/schema_a ")
    context = PluginContext(mcp=None, connection_registry=ConnectionRegistry())

    assert context.is_write_allowed("PROJECT_A\\DEV\\schema_a") is True
    assert context.is_write_allowed("PROJECT_A//DEV//schema_a") is True
    assert context.is_write_allowed("PROJECT_A/DEV/other") is False
