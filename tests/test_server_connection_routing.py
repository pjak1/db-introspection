from __future__ import annotations

import inspect

import server
from src.errors import ConfigError, ValidationError


class FakeIntrospectionService:
    def list_tables(self, schema: str, include_system: bool) -> dict:
        return {"ok": True, "called": "list_tables", "schema": schema, "include_system": include_system}

    def list_columns(self, table: str, schema: str) -> dict:
        return {"ok": True, "called": "list_columns", "table": table, "schema": schema}

    def list_constraints(
        self,
        schema: str,
        table: str | None,
        constraint_type: str | None,
    ) -> dict:
        return {
            "ok": True,
            "called": "list_constraints",
            "schema": schema,
            "table": table,
            "constraint_type": constraint_type,
        }

    def list_sequences(self, schema: str) -> dict:
        return {"ok": True, "called": "list_sequences", "schema": schema}

    def list_procedures(self, schema: str) -> dict:
        return {"ok": True, "called": "list_procedures", "schema": schema}

    def list_functions(self, schema: str) -> dict:
        return {"ok": True, "called": "list_functions", "schema": schema}

    def list_jobs(self, schema: str) -> dict:
        return {"ok": True, "called": "list_jobs", "schema": schema}

    def sample_table(
        self,
        table: str,
        schema: str,
        limit: int | None,
        order_by: str | None,
    ) -> dict:
        return {
            "ok": True,
            "called": "sample_table",
            "table": table,
            "schema": schema,
            "limit": limit,
            "order_by": order_by,
        }

    def select_columns(
        self,
        table: str,
        columns: list[str],
        schema: str,
        limit: int | None,
    ) -> dict:
        return {
            "ok": True,
            "called": "select_columns",
            "table": table,
            "columns": columns,
            "schema": schema,
            "limit": limit,
        }


class FakeSelectService:
    def run_select(
        self,
        sql_query: str,
        limit: int | None,
        timeout_ms: int | None,
        explain: bool = False,
    ) -> dict:
        return {
            "ok": True,
            "called": "run_select",
            "sql_query": sql_query,
            "limit": limit,
            "timeout_ms": timeout_ms,
            "explain": explain,
        }


class FakeRegistry:
    def list_connections(self) -> list[str]:
        return ["A/DEV/dbo", "B/INT/public"]

    def get_services(self, connection: str):
        if not connection.strip():
            raise ValidationError("missing_connection_schema", "connection is required.")
        if connection == "missing":
            raise ConfigError("invalid_config", "connection not found")
        return FakeIntrospectionService(), FakeSelectService()


def test_tools_require_connection_parameter():
    assert inspect.signature(server.db_list_tables).parameters["connection"].default is inspect._empty
    assert inspect.signature(server.db_run_select).parameters["connection"].default is inspect._empty


def test_db_list_connections_returns_connection_names(monkeypatch):
    monkeypatch.setattr(server, "connection_registry", FakeRegistry())

    result = server.db_list_connections()

    assert result == {"ok": True, "connections": ["A/DEV/dbo", "B/INT/public"]}


def test_db_tool_routes_to_selected_connection(monkeypatch):
    monkeypatch.setattr(server, "connection_registry", FakeRegistry())

    result = server.db_list_tables(connection="A/DEV/dbo", schema="public", include_system=False)

    assert result["ok"] is True
    assert result["called"] == "list_tables"
    assert result["schema"] == "public"


def test_db_select_columns_accepts_csv_columns(monkeypatch):
    monkeypatch.setattr(server, "connection_registry", FakeRegistry())

    result = server.db_select_columns(
        connection="A/DEV/dbo",
        schema="public",
        table="users",
        columns="id, name , email",
        limit=3,
    )

    assert result["ok"] is True
    assert result["called"] == "select_columns"
    assert result["columns"] == ["id", "name", "email"]


def test_db_run_select_routes_explain_flag(monkeypatch):
    monkeypatch.setattr(server, "connection_registry", FakeRegistry())

    result = server.db_run_select(
        connection="A/DEV/dbo",
        sql="SELECT 1",
        explain=True,
    )

    assert result["ok"] is True
    assert result["called"] == "run_select"
    assert result["explain"] is True


def test_db_tool_returns_validation_error_for_empty_connection(monkeypatch):
    monkeypatch.setattr(server, "connection_registry", FakeRegistry())

    result = server.db_list_tables(connection="", schema="public", include_system=False)

    assert result["ok"] is False
    assert result["data"] is None
    assert result["dialect"] == "unknown"
    assert result["meta"]["row_count"] == 0
    assert result["meta"]["truncated"] is False
    assert result["meta"]["warnings"] == []
    assert result["meta"]["status"] is None
    assert result["error"]["code"] == "missing_connection_schema"
    assert "message" in result["error"]
    assert "details" in result["error"]


def test_db_tool_returns_config_error_for_missing_connection(monkeypatch):
    monkeypatch.setattr(server, "connection_registry", FakeRegistry())

    result = server.db_run_select(connection="missing", sql="SELECT 1")

    assert result["ok"] is False
    assert result["data"] is None
    assert result["dialect"] == "unknown"
    assert result["meta"]["row_count"] == 0
    assert result["meta"]["truncated"] is False
    assert result["meta"]["warnings"] == []
    assert result["meta"]["status"] is None
    assert result["error"]["code"] == "invalid_config"
    assert "message" in result["error"]
    assert "details" in result["error"]
