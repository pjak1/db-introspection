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
