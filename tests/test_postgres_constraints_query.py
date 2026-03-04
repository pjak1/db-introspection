from src.adapters.postgres import PostgresAdapter


def test_list_constraints_query_without_optional_filters():
    adapter = PostgresAdapter("postgresql://unused")
    captured: dict = {}

    def fake_fetch(query, params=None, timeout_ms=None):  # noqa: ANN001
        captured["query"] = query
        captured["params"] = params
        return []

    adapter._fetch_all = fake_fetch  # type: ignore[method-assign]
    adapter.list_constraints(schemas=("sample_schema",))

    query = captured["query"]
    params = captured["params"]
    assert "(%s IS NULL OR" not in query
    assert "tc.constraint_schema = ANY(%s)" in query
    assert "tc.table_name = %s" not in query
    assert "tc.constraint_type = %s" not in query
    assert params == (["sample_schema"],)


def test_list_constraints_query_with_optional_filters():
    adapter = PostgresAdapter("postgresql://unused")
    captured: dict = {}

    def fake_fetch(query, params=None, timeout_ms=None):  # noqa: ANN001
        captured["query"] = query
        captured["params"] = params
        return []

    adapter._fetch_all = fake_fetch  # type: ignore[method-assign]
    adapter.list_constraints(
        schemas=("sample_schema",),
        table="sample_table",
        constraint_type="primary key",
    )

    query = captured["query"]
    params = captured["params"]
    assert "(%s IS NULL OR" not in query
    assert "tc.table_name = %s" in query
    assert "tc.constraint_type = %s" in query
    assert params == (["sample_schema"], "sample_table", "PRIMARY KEY")
