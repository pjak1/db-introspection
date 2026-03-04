import pytest

from src.errors import ValidationError
from src.services.query_guard import QueryGuard


def test_prepare_select_postgres_wrapper():
    guard = QueryGuard(max_select_limit=200, dialect="postgres")
    result = guard.prepare_select("SELECT 1", limit=10)
    assert result.sql == "SELECT * FROM (SELECT 1) AS mcp_subquery LIMIT 10"
    assert result.truncated is False


def test_prepare_select_oracle_wrapper():
    guard = QueryGuard(max_select_limit=200, dialect="oracle")
    result = guard.prepare_select("SELECT 1 FROM dual", limit=10)
    assert result.sql.endswith("FETCH FIRST 10 ROWS ONLY")
    assert " LIMIT " not in result.sql
    assert ") AS mcp_subquery" not in result.sql


def test_prepare_select_mssql_wrapper():
    guard = QueryGuard(max_select_limit=200, dialect="mssql")
    result = guard.prepare_select("SELECT 1", limit=10)
    assert result.sql.startswith("SELECT TOP (10) * FROM (SELECT 1) mcp_subquery")


def test_prepare_select_allows_cte():
    guard = QueryGuard(max_select_limit=200, dialect="oracle")
    result = guard.prepare_select("WITH x AS (SELECT 1) SELECT * FROM x", limit=15)
    assert result.sql.endswith("FETCH FIRST 15 ROWS ONLY")


def test_prepare_select_rejects_ddl_dml():
    guard = QueryGuard(max_select_limit=200, dialect="postgres")
    with pytest.raises(ValidationError):
        guard.prepare_select("DELETE FROM users", limit=10)


def test_prepare_select_rejects_multi_statement():
    guard = QueryGuard(max_select_limit=200, dialect="postgres")
    with pytest.raises(ValidationError):
        guard.prepare_select("SELECT 1; SELECT 2", limit=10)


def test_prepare_select_truncates_limit():
    guard = QueryGuard(max_select_limit=200, dialect="postgres")
    result = guard.prepare_select("SELECT * FROM users", limit=999)
    assert "LIMIT 200" in result.sql
    assert result.truncated is True
    assert result.warnings


def test_prepare_select_rejects_unsupported_dialect():
    with pytest.raises(ValidationError):
        QueryGuard(max_select_limit=200, dialect="sqlite")

