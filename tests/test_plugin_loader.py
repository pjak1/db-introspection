import asyncio
from pathlib import Path

import pytest
from mcp.server.fastmcp import FastMCP

from src.errors import ConfigError, ValidationError
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
    monkeypatch.delenv("DB_INTROSPECTION_ENABLE_WRITE_PLUGINS", raising=False)
    _write_plugin(tmp_path, "write_tools.py", _REGISTER_ONE)
    mcp = _FakeMcp()

    result = load_plugins(_context(mcp), plugins_dir=tmp_path)

    assert result == []
    assert mcp.added == []


def test_enabled_loads_plugin_and_registers_tool(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("DB_INTROSPECTION_ENABLE_WRITE_PLUGINS", "1")
    _write_plugin(tmp_path, "write_tools.py", _REGISTER_ONE)
    mcp = _FakeMcp()

    result = load_plugins(_context(mcp), plugins_dir=tmp_path)

    assert result == ["write_tools"]
    assert [name for name, _ in mcp.added] == ["db_execute_write"]


def test_plugin_without_register_is_skipped(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("DB_INTROSPECTION_ENABLE_WRITE_PLUGINS", "1")
    _write_plugin(tmp_path, "bad.py", "x = 1\n")
    mcp = _FakeMcp()

    result = load_plugins(_context(mcp), plugins_dir=tmp_path)

    assert result == []
    assert mcp.added == []


def test_broken_plugin_does_not_crash_and_others_still_load(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("DB_INTROSPECTION_ENABLE_WRITE_PLUGINS", "1")
    _write_plugin(tmp_path, "boom.py", "raise RuntimeError('boom')\n")
    _write_plugin(tmp_path, "good.py", _REGISTER_ONE)
    mcp = _FakeMcp()

    result = load_plugins(_context(mcp), plugins_dir=tmp_path)

    assert result == ["good"]
    assert [name for name, _ in mcp.added] == ["db_execute_write"]


def test_private_and_dunder_modules_are_skipped(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("DB_INTROSPECTION_ENABLE_WRITE_PLUGINS", "1")
    _write_plugin(tmp_path, "_helper.py", _REGISTER_ONE)
    _write_plugin(tmp_path, "__init__.py", _REGISTER_ONE)
    mcp = _FakeMcp()

    result = load_plugins(_context(mcp), plugins_dir=tmp_path)

    assert result == []
    assert mcp.added == []


def test_missing_plugins_dir_is_noop(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("DB_INTROSPECTION_ENABLE_WRITE_PLUGINS", "1")
    mcp = _FakeMcp()

    result = load_plugins(_context(mcp), plugins_dir=tmp_path / "does_not_exist")

    assert result == []


def _conn_file(tmp_path: Path, writable: str | None, *extra: str) -> Path:
    """Write a minimal db_conn.txt, optionally with `writable` and extra lines."""
    lines = [
        "dialect:postgres",
        "host:example-host",
        "db_name:example_db",
        "port:5432",
        "username:u",
        "password:p",
        "schema:s",
    ]
    if writable is not None:
        lines.append(f"writable:{writable}")
    lines.extend(extra)
    path = tmp_path / "db_conn.txt"
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def _ctx_for_conn_file(monkeypatch, conn_file: Path | None) -> PluginContext:
    """Build a context whose registry resolves every key to `conn_file`.

    Pass conn_file=None to simulate an unknown connection (resolve raises).
    """
    registry = ConnectionRegistry()

    def resolve(connection: str) -> Path:
        if conn_file is None:
            raise ConfigError("invalid_config", "connection not found")
        return conn_file

    monkeypatch.setattr(registry, "resolve_conn_file", resolve)
    return PluginContext(mcp=_FakeMcp(), connection_registry=registry)


def test_require_writable_allows_connection_marked_writable(monkeypatch, tmp_path: Path):
    ctx = _ctx_for_conn_file(monkeypatch, _conn_file(tmp_path, "true"))

    assert ctx.is_write_allowed("PROJECT_A/DEV/schema_a")
    ctx.require_writable("PROJECT_A/DEV/schema_a")  # must not raise


def test_require_writable_rejects_connection_without_writable_flag(monkeypatch, tmp_path: Path):
    ctx = _ctx_for_conn_file(monkeypatch, _conn_file(tmp_path, None))

    assert not ctx.is_write_allowed("PROJECT_A/DEV/schema_a")
    with pytest.raises(ValidationError) as exc:
        ctx.require_writable("PROJECT_A/DEV/schema_a")

    assert exc.value.code == "write_not_allowed"


def test_require_writable_rejects_when_writable_is_false(monkeypatch, tmp_path: Path):
    ctx = _ctx_for_conn_file(monkeypatch, _conn_file(tmp_path, "false"))

    assert not ctx.is_write_allowed("PROJECT_A/DEV/schema_a")
    with pytest.raises(ValidationError):
        ctx.require_writable("PROJECT_A/DEV/schema_a")


def test_unknown_connection_is_not_writable(monkeypatch):
    # Fail-closed: if the connection file cannot even be resolved, it is not writable.
    ctx = _ctx_for_conn_file(monkeypatch, None)

    assert not ctx.is_write_allowed("PROJECT_A/DEV/schema_a")
    with pytest.raises(ValidationError) as exc:
        ctx.require_writable("PROJECT_A/DEV/schema_a")

    assert exc.value.code == "write_not_allowed"


def test_connection_config_returns_all_keys(monkeypatch, tmp_path: Path):
    conn_file = _conn_file(tmp_path, "true", "plugin.write.mode:dry_run")
    ctx = _ctx_for_conn_file(monkeypatch, conn_file)

    values = ctx.connection_config("PROJECT_A/DEV/schema_a")

    assert values["writable"] == "true"
    assert values["plugin.write.mode"] == "dry_run"
    assert values["dialect"] == "postgres"


def test_connection_config_raises_for_unknown_connection(monkeypatch):
    ctx = _ctx_for_conn_file(monkeypatch, None)

    with pytest.raises(ConfigError):
        ctx.connection_config("PROJECT_A/DEV/schema_a")


def test_plugin_config_filters_and_strips_prefix(monkeypatch, tmp_path: Path):
    conn_file = _conn_file(
        tmp_path,
        "true",
        "plugin.write.mode:dry_run",
        "plugin.write.max_rows:100",
        "plugin.audit.sink:stderr",
    )
    ctx = _ctx_for_conn_file(monkeypatch, conn_file)

    assert ctx.plugin_config("PROJECT_A/DEV/schema_a", "write") == {
        "mode": "dry_run",
        "max_rows": "100",
    }


def test_plugin_config_empty_when_none(monkeypatch, tmp_path: Path):
    ctx = _ctx_for_conn_file(monkeypatch, _conn_file(tmp_path, "true"))

    assert ctx.plugin_config("PROJECT_A/DEV/schema_a", "write") == {}


def test_plugin_config_name_is_case_insensitive(monkeypatch, tmp_path: Path):
    conn_file = _conn_file(tmp_path, "true", "plugin.write.mode:dry_run")
    ctx = _ctx_for_conn_file(monkeypatch, conn_file)

    assert ctx.plugin_config("PROJECT_A/DEV/schema_a", "WRITE") == {"mode": "dry_run"}


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
