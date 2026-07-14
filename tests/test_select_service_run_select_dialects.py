from conftest import BaseStubAdapter, make_settings

from src.adapters.base import AdapterResult
from src.services.select_service import SelectService


class StubAdapter(BaseStubAdapter):
    def __init__(self, dialect: str):
        self._dialect = dialect
        self.captured_run_sql: str | None = None
        self.captured_explain_sql: str | None = None
        self.captured_timeout: int | None = None
        self.captured_method: str | None = None

    def run_select(self, sql_query: str, timeout_ms: int) -> AdapterResult:
        self.captured_method = "run_select"
        self.captured_run_sql = sql_query
        self.captured_timeout = timeout_ms
        return AdapterResult(data=[{"ok": True}])

    def explain_select(self, sql_query: str, timeout_ms: int) -> AdapterResult:
        self.captured_method = "explain_select"
        self.captured_explain_sql = sql_query
        self.captured_timeout = timeout_ms
        return AdapterResult(data=[{"plan_text": "Seq Scan on users"}], status="explain")


def _settings():
    return make_settings(db_dsn="postgresql://localhost/db")


def test_select_service_wraps_postgres_query():
    adapter = StubAdapter(dialect="postgres")
    service = SelectService(adapter=adapter, settings=_settings())

    envelope = service.run_select(sql_query="SELECT 1", limit=10, timeout_ms=None)

    assert envelope["ok"] is True
    assert adapter.captured_method == "run_select"
    assert adapter.captured_run_sql == "SELECT * FROM (SELECT 1) AS mcp_subquery LIMIT 10"
    assert adapter.captured_timeout == 5000


def test_select_service_wraps_oracle_query():
    adapter = StubAdapter(dialect="oracle")
    service = SelectService(adapter=adapter, settings=_settings())

    envelope = service.run_select(sql_query="SELECT 1 FROM dual", limit=10, timeout_ms=1200)

    assert envelope["ok"] is True
    assert adapter.captured_method == "run_select"
    assert adapter.captured_run_sql == "SELECT * FROM (SELECT 1 FROM dual) mcp_subquery FETCH FIRST 10 ROWS ONLY"
    assert adapter.captured_timeout == 1200


def test_select_service_wraps_mssql_query():
    adapter = StubAdapter(dialect="mssql")
    service = SelectService(adapter=adapter, settings=_settings())

    envelope = service.run_select(sql_query="SELECT 1", limit=10, timeout_ms=None)

    assert envelope["ok"] is True
    assert adapter.captured_method == "run_select"
    assert adapter.captured_run_sql == "SELECT TOP (10) * FROM (SELECT 1) mcp_subquery"
    assert adapter.captured_timeout == 5000


def test_select_service_explain_uses_original_sql_and_ignores_limit():
    adapter = StubAdapter(dialect="postgres")
    service = SelectService(adapter=adapter, settings=_settings())

    envelope = service.run_select(
        sql_query="SELECT * FROM users",
        limit=10,
        timeout_ms=None,
        explain=True,
    )

    assert envelope["ok"] is True
    assert adapter.captured_method == "explain_select"
    assert adapter.captured_explain_sql == "SELECT * FROM users"
    assert adapter.captured_timeout == 5000
    assert envelope["data"] == [{"plan_text": "Seq Scan on users"}]
    assert envelope["meta"]["status"] == "explain"
    assert envelope["meta"]["truncated"] is False
    assert envelope["meta"]["warnings"] == [
        "Requested limit 10 was ignored because explain=True plans the original SQL."
    ]
