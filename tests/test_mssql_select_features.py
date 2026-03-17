from src.adapters.mssql import MssqlAdapter


class _MssqlCursor:
    def __init__(self):
        self.executed: list[str] = []
        self.description = None
        self._result_sets = [
            {
                "description": [("StmtText",), ("NodeId",)],
                "rows": [("  |--Clustered Index Scan(OBJECT:([dbo].[Users]))", 1)],
            }
        ]
        self._result_index = -1

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params=None):
        self.executed.append(query)
        if query == "SET SHOWPLAN_TEXT ON":
            self.description = None
            self._result_index = -1
            return self
        if query == "SET SHOWPLAN_TEXT OFF":
            self.description = None
            return self
        self._result_index = 0
        self.description = self._result_sets[0]["description"]
        return self

    def fetchall(self):
        if self._result_index < 0:
            return []
        return self._result_sets[self._result_index]["rows"]

    def nextset(self):
        self._result_index += 1
        if self._result_index >= len(self._result_sets):
            self.description = None
            return False
        self.description = self._result_sets[self._result_index]["description"]
        return True


class _MssqlConnection:
    def __init__(self):
        self.timeout: int | None = None
        self.cursor_instance = _MssqlCursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return self.cursor_instance


def test_mssql_list_columns_adds_full_data_type(monkeypatch):
    captured: dict[str, str] = {}

    def fake_fetch_all(self, query, params=None, timeout_ms=None):
        captured["query"] = query
        return [
            {
                "schema": "dbo",
                "table_name": "users",
                "column_name": "payload",
                "ordinal_position": 1,
                "data_type": "varchar",
                "udt_name": "varchar",
                "is_nullable": 1,
                "column_default": None,
                "_character_maximum_length": -1,
                "_numeric_precision": None,
                "_numeric_scale": None,
                "_datetime_precision": None,
            }
        ]

    monkeypatch.setattr(MssqlAdapter, "_fetch_all", fake_fetch_all)
    adapter = MssqlAdapter(dsn="Driver=test")

    result = adapter.list_columns(table="users", schemas=("dbo",))

    assert "CHARACTER_MAXIMUM_LENGTH AS _character_maximum_length" in captured["query"]
    assert result.data == [
        {
            "schema": "dbo",
            "table_name": "users",
            "column_name": "payload",
            "ordinal_position": 1,
            "data_type": "varchar",
            "udt_name": "varchar",
            "is_nullable": 1,
            "column_default": None,
            "full_data_type": "varchar(max)",
        }
    ]


def test_mssql_explain_select_uses_showplan_text(monkeypatch):
    conn = _MssqlConnection()
    monkeypatch.setattr(MssqlAdapter, "_connect", lambda self: conn)
    adapter = MssqlAdapter(dsn="Driver=test")

    result = adapter.explain_select("SELECT * FROM dbo.Users", timeout_ms=1200)

    assert conn.timeout == 1
    assert conn.cursor_instance.executed == [
        "SET SHOWPLAN_TEXT ON",
        "SELECT * FROM dbo.Users",
        "SET SHOWPLAN_TEXT OFF",
    ]
    assert result.status == "explain"
    assert result.data == [
        {"plan_text": "  |--Clustered Index Scan(OBJECT:([dbo].[Users]))"}
    ]
