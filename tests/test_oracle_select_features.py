from src.adapters.oracle import OracleAdapter


class _OracleCursor:
    def __init__(self):
        self.executed: list[str] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params=None):
        self.executed.append(query)
        return self

    def fetchall(self):
        return [
            ("Plan hash value: 12345",),
            ("TABLE ACCESS FULL USERS",),
        ]


class _OracleConnection:
    def __init__(self):
        self.call_timeout: int | None = None
        self.cursor_instance = _OracleCursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return self.cursor_instance


def test_oracle_list_columns_adds_full_data_type(monkeypatch):
    captured: dict[str, str] = {}

    def fake_fetch_all(self, query, params=None, timeout_ms=None):
        captured["query"] = query
        return [
            {
                "schema": "SAMPLE_SCHEMA",
                "table_name": "USERS",
                "column_name": "NAME",
                "ordinal_position": 1,
                "data_type": "VARCHAR2",
                "udt_name": "VARCHAR2",
                "is_nullable": 1,
                "column_default": None,
                "helper_char_used": "C",
                "helper_char_length": 20,
                "helper_data_length": 80,
                "helper_data_precision": None,
                "helper_data_scale": None,
            }
        ]

    monkeypatch.setattr(OracleAdapter, "_fetch_all", fake_fetch_all)
    adapter = OracleAdapter(dsn="user/pass@db")

    result = adapter.list_columns(table="users", schemas=("sample_schema",))

    assert "char_used AS helper_char_used" in captured["query"]
    assert result.data == [
        {
            "schema": "SAMPLE_SCHEMA",
            "table_name": "USERS",
            "column_name": "NAME",
            "ordinal_position": 1,
            "data_type": "VARCHAR2",
            "udt_name": "VARCHAR2",
            "is_nullable": 1,
            "column_default": None,
            "full_data_type": "VARCHAR2(20 CHAR)",
        }
    ]


def test_oracle_explain_select_uses_dbms_xplan_display(monkeypatch):
    conn = _OracleConnection()
    monkeypatch.setattr(OracleAdapter, "_connect", lambda self: conn)
    adapter = OracleAdapter(dsn="user/pass@db")

    result = adapter.explain_select("SELECT * FROM users", timeout_ms=1400)

    assert conn.call_timeout == 1400
    assert conn.cursor_instance.executed == [
        "EXPLAIN PLAN FOR SELECT * FROM users",
        "SELECT plan_table_output FROM TABLE(DBMS_XPLAN.DISPLAY())",
    ]
    assert result.status == "explain"
    assert result.data == [
        {"plan_text": "Plan hash value: 12345"},
        {"plan_text": "TABLE ACCESS FULL USERS"},
    ]
