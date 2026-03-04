
from src.adapters.base import AdapterResult, DatabaseAdapter
from src.config import Settings
from src.services.select_service import SelectService


class StubAdapter(DatabaseAdapter):
    def __init__(self, dialect: str):
        self._dialect = dialect
        self.captured_sql: str | None = None
        self.captured_timeout: int | None = None

    @property
    def dialect(self) -> str:
        return self._dialect

    def list_tables(self, schemas: tuple[str, ...], include_system: bool) -> AdapterResult:
        raise NotImplementedError

    def list_columns(self, table: str, schemas: tuple[str, ...]) -> AdapterResult:
        raise NotImplementedError

    def list_constraints(
        self,
        schemas: tuple[str, ...],
        table: str | None = None,
        constraint_type: str | None = None,
    ) -> AdapterResult:
        raise NotImplementedError

    def list_sequences(self, schemas: tuple[str, ...]) -> AdapterResult:
        raise NotImplementedError

    def list_procedures(self, schemas: tuple[str, ...]) -> AdapterResult:
        raise NotImplementedError

    def list_functions(self, schemas: tuple[str, ...]) -> AdapterResult:
        raise NotImplementedError

    def list_jobs(self) -> AdapterResult:
        raise NotImplementedError

    def sample_table(
        self,
        schema: str,
        table: str,
        limit: int,
        order_by: str | None,
    ) -> AdapterResult:
        raise NotImplementedError

    def select_columns(
        self,
        schema: str,
        table: str,
        columns: list[str],
        limit: int,
    ) -> AdapterResult:
        raise NotImplementedError

    def run_select(self, sql_query: str, timeout_ms: int) -> AdapterResult:
        self.captured_sql = sql_query
        self.captured_timeout = timeout_ms
        return AdapterResult(data=[{"ok": True}])


def _settings() -> Settings:
    return Settings(
        db_dialect="postgres",
        db_dsn="postgresql://localhost/db",
        allowed_schemas=("public",),
        default_sample_limit=10,
        max_sample_limit=100,
        max_select_limit=200,
        statement_timeout_ms=5000,
        include_system_schemas=False,
    )


def test_select_service_wraps_postgres_query():
    adapter = StubAdapter(dialect="postgres")
    service = SelectService(adapter=adapter, settings=_settings())

    envelope = service.run_select(sql_query="SELECT 1", limit=10, timeout_ms=None)

    assert envelope["ok"] is True
    assert adapter.captured_sql == "SELECT * FROM (SELECT 1) AS mcp_subquery LIMIT 10"
    assert adapter.captured_timeout == 5000


def test_select_service_wraps_oracle_query():
    adapter = StubAdapter(dialect="oracle")
    service = SelectService(adapter=adapter, settings=_settings())

    envelope = service.run_select(sql_query="SELECT 1 FROM dual", limit=10, timeout_ms=1200)

    assert envelope["ok"] is True
    assert adapter.captured_sql == "SELECT * FROM (SELECT 1 FROM dual) mcp_subquery FETCH FIRST 10 ROWS ONLY"
    assert adapter.captured_timeout == 1200


def test_select_service_wraps_mssql_query():
    adapter = StubAdapter(dialect="mssql")
    service = SelectService(adapter=adapter, settings=_settings())

    envelope = service.run_select(sql_query="SELECT 1", limit=10, timeout_ms=None)

    assert envelope["ok"] is True
    assert adapter.captured_sql == "SELECT TOP (10) * FROM (SELECT 1) mcp_subquery"
    assert adapter.captured_timeout == 5000

