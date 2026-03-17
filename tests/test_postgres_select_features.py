from src.adapters.postgres import PostgresAdapter


def test_postgres_list_columns_query_uses_format_type():
    adapter = PostgresAdapter("postgresql://unused")
    captured: dict = {}

    def fake_fetch(query, params=None, timeout_ms=None):  # noqa: ANN001
        captured["query"] = query
        captured["params"] = params
        return []

    adapter._fetch_all = fake_fetch  # type: ignore[method-assign]
    adapter.list_columns(table="users", schemas=("public",))

    query = captured["query"]
    assert "pg_catalog.format_type(attr.atttypid, attr.atttypmod) AS full_data_type" in query
    assert "JOIN pg_catalog.pg_attribute attr" in query
    assert captured["params"] == ("users", ["public"])


def test_postgres_explain_select_uses_explain_format_text():
    adapter = PostgresAdapter("postgresql://unused")
    captured: dict = {}

    def fake_fetch(query, params=None, timeout_ms=None):  # noqa: ANN001
        captured["query"] = query
        captured["timeout_ms"] = timeout_ms
        return [{"QUERY PLAN": "Seq Scan on users"}]

    adapter._fetch_all = fake_fetch  # type: ignore[method-assign]
    result = adapter.explain_select("SELECT * FROM users", timeout_ms=1200)

    assert captured["query"] == "EXPLAIN (FORMAT TEXT) SELECT * FROM users"
    assert captured["timeout_ms"] == 1200
    assert result.status == "explain"
    assert result.data == [{"plan_text": "Seq Scan on users"}]
