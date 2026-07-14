import asyncio
from pathlib import Path

import pytest
from mcp.server.fastmcp import FastMCP

from src.errors import ValidationError
from src.plugins.api import MUTATING, PluginContext
from src.plugins.loader import load_plugins
from src.services.connection_registry import ConnectionRegistry


class _FakeMcp:
    """Records add_tool calls so tests can assert what a plugin registered."""

    def __init__(self) -> None:
        self.added: list[tuple[str, object]] = []

    def add_tool(self, fn, name=None, annotations=None) -> None:
        self.added.append((name or fn.__name__, annotations))


def _context(mcp) -> PluginContext:
    return PluginContext(mcp=mcp, connection_registry=ConnectionRegistry())


def _write_plugin(directory: Path, name: str, body: str) -> Path:
    path = directory / name
    path.write_text(body, encoding="utf-8")
    return path


_REGISTER_ONE = (
    "def register(context):\n"
    "    def db_execute_write(connection, sql):\n"
    "        return {}\n"
    "    context.mcp.add_tool(db_execute_write, name='db_execute_write')\n"
)


def test_disabled_by_default_registers_nothing(monkeypatch, tmp_path: Path):
    monkeypatch.delenv("DB_ENABLE_WRITE_PLUGINS", raising=False)
    _write_plugin(tmp_path, "write_tools.py", _REGISTER_ONE)
    mcp = _FakeMcp()

    result = load_plugins(_context(mcp), plugins_dir=tmp_path)

    assert result == []
    assert mcp.added == []


def test_enabled_loads_plugin_and_registers_tool(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("DB_ENABLE_WRITE_PLUGINS", "1")
    _write_plugin(tmp_path, "write_tools.py", _REGISTER_ONE)
    mcp = _FakeMcp()

    result = load_plugins(_context(mcp), plugins_dir=tmp_path)

    assert result == ["write_tools"]
    assert [name for name, _ in mcp.added] == ["db_execute_write"]


def test_plugin_without_register_is_skipped(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("DB_ENABLE_WRITE_PLUGINS", "1")
    _write_plugin(tmp_path, "bad.py", "x = 1\n")
    mcp = _FakeMcp()

    result = load_plugins(_context(mcp), plugins_dir=tmp_path)

    assert result == []
    assert mcp.added == []


def test_broken_plugin_does_not_crash_and_others_still_load(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("DB_ENABLE_WRITE_PLUGINS", "1")
    _write_plugin(tmp_path, "boom.py", "raise RuntimeError('boom')\n")
    _write_plugin(tmp_path, "good.py", _REGISTER_ONE)
    mcp = _FakeMcp()

    result = load_plugins(_context(mcp), plugins_dir=tmp_path)

    assert result == ["good"]
    assert [name for name, _ in mcp.added] == ["db_execute_write"]


def test_private_and_dunder_modules_are_skipped(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("DB_ENABLE_WRITE_PLUGINS", "1")
    _write_plugin(tmp_path, "_helper.py", _REGISTER_ONE)
    _write_plugin(tmp_path, "__init__.py", _REGISTER_ONE)
    mcp = _FakeMcp()

    result = load_plugins(_context(mcp), plugins_dir=tmp_path)

    assert result == []
    assert mcp.added == []


def test_missing_plugins_dir_is_noop(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("DB_ENABLE_WRITE_PLUGINS", "1")
    mcp = _FakeMcp()

    result = load_plugins(_context(mcp), plugins_dir=tmp_path / "does_not_exist")

    assert result == []


def test_require_writable_allows_listed_connection(monkeypatch):
    monkeypatch.setenv("DB_WRITABLE_CONNECTIONS", "PROJECT_A/DEV/schema_a, PROJECT_B/INT/s")
    ctx = _context(_FakeMcp())

    assert ctx.is_write_allowed("PROJECT_A/DEV/schema_a")
    # Backslashes and duplicate slashes normalize to the canonical key.
    ctx.require_writable("PROJECT_A\\DEV\\schema_a")  # must not raise


def test_require_writable_rejects_unlisted_connection(monkeypatch):
    monkeypatch.setenv("DB_WRITABLE_CONNECTIONS", "PROJECT_A/DEV/schema_a")
    ctx = _context(_FakeMcp())

    assert not ctx.is_write_allowed("PROJECT_A/PROD/schema_a")
    with pytest.raises(ValidationError) as exc:
        ctx.require_writable("PROJECT_A/PROD/schema_a")

    assert exc.value.code == "write_not_allowed"


def test_empty_allowlist_rejects_all(monkeypatch):
    monkeypatch.delenv("DB_WRITABLE_CONNECTIONS", raising=False)
    ctx = _context(_FakeMcp())

    with pytest.raises(ValidationError) as exc:
        ctx.require_writable("PROJECT_A/DEV/schema_a")

    assert exc.value.code == "write_not_allowed"


def test_settings_for_resolves_dsn(monkeypatch, tmp_path: Path):
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
    ctx = PluginContext(mcp=_FakeMcp(), connection_registry=registry)

    settings = ctx.settings_for("PROJECT_A/DEV/example_schema")

    assert settings.db_dialect == "postgres"
    assert settings.db_dsn == "postgresql://example_user:example_pass@example-host:5432/example_db"


def test_mutating_annotations_register_on_real_fastmcp():
    mcp = FastMCP("test-mutating")

    def db_execute_write(connection: str, sql: str) -> dict:
        """Execute a write statement."""
        return {}

    mcp.add_tool(db_execute_write, name="db_execute_write", annotations=MUTATING)

    tools = asyncio.run(mcp.list_tools())
    assert "db_execute_write" in [tool.name for tool in tools]
