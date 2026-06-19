from __future__ import annotations

import asyncio

import server


class FakeIntrospectionService:
    def list_indexes(self, schema: str, table: str | None) -> dict:
        return {"ok": True, "called": "list_indexes", "schema": schema, "table": table}

    def get_ddl(self, schema: str, object_name: str, object_type: str) -> dict:
        return {
            "ok": True,
            "called": "get_ddl",
            "schema": schema,
            "object_name": object_name,
            "object_type": object_type,
        }

    def search_objects(self, schema: str, pattern: str, object_types) -> dict:
        return {
            "ok": True,
            "called": "search_objects",
            "schema": schema,
            "pattern": pattern,
            "object_types": object_types,
        }


class FakeRegistry:
    def get_services(self, connection: str):
        return FakeIntrospectionService(), object()


def test_db_list_indexes_routes(monkeypatch):
    monkeypatch.setattr(server, "connection_registry", FakeRegistry())
    result = server.db_list_indexes(connection="A/DEV/public", schema="public", table="users")
    assert result["called"] == "list_indexes"
    assert result["table"] == "users"


def test_db_get_ddl_routes(monkeypatch):
    monkeypatch.setattr(server, "connection_registry", FakeRegistry())
    result = server.db_get_ddl(
        connection="A/DEV/public", schema="public", object_name="v", object_type="view")
    assert result["called"] == "get_ddl"
    assert result["object_type"] == "view"


def test_db_search_objects_accepts_csv_object_types(monkeypatch):
    monkeypatch.setattr(server, "connection_registry", FakeRegistry())
    result = server.db_search_objects(
        connection="A/DEV/public", schema="public", pattern="usr",
        object_types="table, view")
    assert result["called"] == "search_objects"
    assert result["object_types"] == ["table", "view"]


def test_db_search_objects_none_object_types_passes_none(monkeypatch):
    monkeypatch.setattr(server, "connection_registry", FakeRegistry())
    result = server.db_search_objects(
        connection="A/DEV/public", schema="public", pattern="usr", object_types=None)
    assert result["object_types"] is None


def test_all_tools_carry_read_only_annotation():
    tools = asyncio.run(server.mcp.list_tools())
    assert tools, "expected registered tools"
    for tool in tools:
        assert tool.annotations is not None, f"{tool.name} missing annotations"
        assert tool.annotations.readOnlyHint is True, f"{tool.name} not read-only"
        assert tool.annotations.destructiveHint is False, f"{tool.name} marked destructive"
