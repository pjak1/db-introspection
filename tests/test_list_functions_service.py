from src.adapters.base import AdapterResult, DatabaseAdapter
from src.config import Settings
from src.errors import ValidationError
from src.services.introspection_service import IntrospectionService


class DummyAdapter(DatabaseAdapter):
    @property
    def dialect(self) -> str:
        return "postgres"

    def list_tables(self, schemas: tuple[str, ...], include_system: bool) -> AdapterResult:
        return AdapterResult(data=[])

    def list_columns(self, table: str, schemas: tuple[str, ...]) -> AdapterResult:
        return AdapterResult(data=[])

    def list_constraints(
        self,
        schemas: tuple[str, ...],
        table: str | None = None,
        constraint_type: str | None = None,
    ) -> AdapterResult:
        return AdapterResult(data=[])

    def list_sequences(self, schemas: tuple[str, ...]) -> AdapterResult:
        return AdapterResult(data=[])

    def list_procedures(self, schemas: tuple[str, ...]) -> AdapterResult:
        return AdapterResult(data=[])

    def list_functions(self, schemas: tuple[str, ...]) -> AdapterResult:
        raise ValidationError("invalid_functions_query",
                              "Simulated adapter failure.")

    def list_jobs(self) -> AdapterResult:
        return AdapterResult(data=[])

    def sample_table(
        self,
        schema: str,
        table: str,
        limit: int,
        order_by: str | None,
    ) -> AdapterResult:
        return AdapterResult(data=[])

    def select_columns(
        self,
        schema: str,
        table: str,
        columns: list[str],
        limit: int,
    ) -> AdapterResult:
        return AdapterResult(data=[])

    def run_select(self, sql_query: str, timeout_ms: int) -> AdapterResult:
        return AdapterResult(data=[])

    def explain_select(self, sql_query: str, timeout_ms: int) -> AdapterResult:
        return AdapterResult(data=[])


def _settings() -> Settings:
    return Settings(
        db_dialect="postgres",
        db_dsn="postgresql://user:pass@localhost:5432/db",
        allowed_schemas=("public",),
        default_sample_limit=10,
        max_sample_limit=100,
        max_select_limit=200,
        statement_timeout_ms=5000,
        include_system_schemas=False,
    )


def test_list_functions_returns_error_envelope_when_adapter_raises():
    service = IntrospectionService(
        adapter=DummyAdapter(), settings=_settings())

    result = service.list_functions(schema="public")

    assert result["ok"] is False
    assert result["data"] is None
    assert result["dialect"] == "postgres"
    assert result["error"]["code"] == "invalid_functions_query"
    assert result["meta"]["row_count"] == 0
    assert result["meta"]["truncated"] is False
    assert result["meta"]["warnings"] == []
    assert result["meta"]["schema_used"] is None
    assert result["meta"]["status"] is None
    assert isinstance(result["meta"]["duration_ms"], int)
    assert result["meta"]["duration_ms"] >= 0
