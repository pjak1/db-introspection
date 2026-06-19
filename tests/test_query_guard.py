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


def test_prepare_select_allows_recursive_cte():
    guard = QueryGuard(max_select_limit=200, dialect="postgres")
    sql = (
        "WITH RECURSIVE t(n) AS ("
        "SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 5"
        ") SELECT * FROM t"
    )
    result = guard.prepare_select(sql, limit=10)
    assert "LIMIT 10" in result.sql


@pytest.mark.parametrize(
    "dialect, must_contain",
    [
        ("postgres", "LIMIT 10"),
        ("oracle", "FETCH FIRST 10 ROWS ONLY"),
        ("mssql", "FETCH NEXT 10 ROWS ONLY"),
    ],
)
def test_prepare_select_cte_is_supported_on_every_dialect(dialect, must_contain):
    guard = QueryGuard(max_select_limit=100, dialect=dialect)
    result = guard.prepare_select(
        "WITH x AS (SELECT 1 AS n) SELECT * FROM x", limit=10)
    assert "WITH x AS" in result.sql
    assert must_contain in result.sql


def test_prepare_select_mssql_cte_never_nests_with_in_derived_table():
    # PostgreSQL/Oracle accept `FROM (WITH ...)`, but it is invalid T-SQL.
    guard = QueryGuard(max_select_limit=100, dialect="mssql")
    result = guard.prepare_select(
        "WITH x AS (SELECT 1 AS n) SELECT * FROM x", limit=10)
    assert "FROM (WITH" not in result.sql.upper()


def test_prepare_select_mssql_cte_uses_offset_fetch():
    guard = QueryGuard(max_select_limit=100, dialect="mssql")
    result = guard.prepare_select(
        "WITH x AS (SELECT 1 AS n) SELECT * FROM x", limit=10)
    assert result.sql.startswith("WITH x AS")
    assert result.sql.endswith("OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY")


def test_prepare_select_mssql_cte_keeps_outer_order_by():
    guard = QueryGuard(max_select_limit=100, dialect="mssql")
    result = guard.prepare_select(
        "WITH x AS (SELECT 1 AS n) SELECT * FROM x ORDER BY n", limit=10)
    assert result.sql.endswith("ORDER BY n OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY")


def test_prepare_select_mssql_cte_ignores_order_by_inside_body():
    # ORDER BY only appears inside the CTE body, so a no-op ordering must be added
    # to the outer query to make OFFSET-FETCH valid.
    guard = QueryGuard(max_select_limit=100, dialect="mssql")
    result = guard.prepare_select(
        "WITH x AS (SELECT 1 n ORDER BY 1) SELECT * FROM x", limit=10)
    assert result.sql.endswith(
        "ORDER BY (SELECT NULL) OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY")


def test_prepare_select_mssql_outer_order_by_uses_offset_fetch():
    guard = QueryGuard(max_select_limit=100, dialect="mssql")
    result = guard.prepare_select("SELECT * FROM t ORDER BY a", limit=10)
    assert result.sql == "SELECT * FROM t ORDER BY a OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY"


def test_prepare_select_mssql_outer_offset_is_overridden():
    guard = QueryGuard(max_select_limit=100, dialect="mssql")
    result = guard.prepare_select(
        "SELECT * FROM t ORDER BY a OFFSET 20 ROWS FETCH NEXT 5 ROWS ONLY", limit=10)
    assert result.sql == "SELECT * FROM t ORDER BY a OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY"


def test_prepare_select_mssql_order_by_inside_subquery_is_wrapped():
    # ORDER BY lives only in a subquery (depth > 0), so the outer query can be
    # safely wrapped in a derived table instead of receiving an invalid OFFSET-FETCH.
    guard = QueryGuard(max_select_limit=100, dialect="mssql")
    result = guard.prepare_select(
        "SELECT a FROM t WHERE x IN (SELECT TOP 3 id FROM s ORDER BY id)", limit=10)
    assert result.sql == (
        "SELECT TOP (10) * FROM "
        "(SELECT a FROM t WHERE x IN (SELECT TOP 3 id FROM s ORDER BY id)) mcp_subquery"
    )


def test_prepare_select_rejects_merge():
    guard = QueryGuard(max_select_limit=200, dialect="postgres")
    with pytest.raises(ValidationError):
        guard.prepare_select(
            "MERGE INTO t USING s ON (t.id = s.id) "
            "WHEN NOT MATCHED THEN INSERT VALUES (s.id)",
            limit=10,
        )


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


def test_validate_select_returns_original_sql_without_extra_whitespace():
    guard = QueryGuard(max_select_limit=200, dialect="postgres")
    result = guard.validate_select("  SELECT * FROM users  ")
    assert result == "SELECT * FROM users"


def test_validate_select_rejects_ddl_for_explain_path():
    guard = QueryGuard(max_select_limit=200, dialect="postgres")
    with pytest.raises(ValidationError):
        guard.validate_select("DROP TABLE users")


def test_validate_select_rejects_multi_statement_for_explain_path():
    guard = QueryGuard(max_select_limit=200, dialect="postgres")
    with pytest.raises(ValidationError):
        guard.validate_select("SELECT 1; SELECT 2")


def test_prepare_select_rejects_unsupported_dialect():
    with pytest.raises(ValidationError):
        QueryGuard(max_select_limit=200, dialect="sqlite")

