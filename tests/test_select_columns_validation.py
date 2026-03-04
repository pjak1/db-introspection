from src.adapters.base import AdapterResult, DatabaseAdapter
from src.config import Settings
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
        return AdapterResult(data=[])

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
        return AdapterResult(
            data=[{column: f"v_{column}" for column in columns}],
            schema_used=schema,
        )

    def run_select(self, sql_query: str, timeout_ms: int) -> AdapterResult:
        return AdapterResult(data=[])


def _single_schema_settings() -> Settings:
    return Settings(
        db_dialect="postgres",
        db_dsn="postgresql://user:pass@localhost:5432/db",
        allowed_schemas=("sample_schema_a",),
        default_sample_limit=10,
        max_sample_limit=100,
        max_select_limit=200,
        statement_timeout_ms=5000,
        include_system_schemas=False,
    )


def _multi_schema_settings() -> Settings:
    return Settings(
        db_dialect="postgres",
        db_dsn="postgresql://user:pass@localhost:5432/db",
        allowed_schemas=("sample_schema_a", "sample_schema_b"),
        default_sample_limit=10,
        max_sample_limit=100,
        max_select_limit=200,
        statement_timeout_ms=5000,
        include_system_schemas=False,
    )


def test_select_columns_rejects_empty_columns():
    service = IntrospectionService(adapter=DummyAdapter(), settings=_single_schema_settings())
    result = service.select_columns(table="sample_table", columns=[], schema="sample_schema_a", limit=10)
    assert result["ok"] is False
    assert result["error"]["code"] == "invalid_columns"


def test_select_columns_rejects_invalid_identifier():
    service = IntrospectionService(adapter=DummyAdapter(), settings=_single_schema_settings())
    result = service.select_columns(
        table="sample_table",
        columns=["id", "bad-column"],
        schema="sample_schema_a",
        limit=10,
    )
    assert result["ok"] is False
    assert result["error"]["code"] == "invalid_columns"


def test_select_columns_valid_input_returns_ok():
    service = IntrospectionService(adapter=DummyAdapter(), settings=_single_schema_settings())
    result = service.select_columns(
        table="sample_table",
        columns=["id", "nazev"],
        schema="sample_schema_a",
        limit=10,
    )
    assert result["ok"] is True
    assert result["data"] == [{"id": "v_id", "nazev": "v_nazev"}]


def test_select_columns_requires_schema_even_for_single_allowed():
    service = IntrospectionService(adapter=DummyAdapter(), settings=_single_schema_settings())
    result = service.select_columns(
        table="sample_table",
        columns=["id"],
        schema="",
        limit=10,
    )
    assert result["ok"] is False
    assert result["error"]["code"] == "missing_schema"


def test_select_columns_requires_schema_when_multiple_allowed():
    service = IntrospectionService(adapter=DummyAdapter(), settings=_multi_schema_settings())
    result = service.select_columns(
        table="sample_table",
        columns=["id"],
        schema="",
        limit=10,
    )
    assert result["ok"] is False
    assert result["error"]["code"] == "missing_schema"


def test_select_columns_limit_is_truncated_to_max():
    settings = Settings(
        db_dialect="postgres",
        db_dsn="postgresql://user:pass@localhost:5432/db",
        allowed_schemas=("sample_schema_a",),
        default_sample_limit=10,
        max_sample_limit=5,
        max_select_limit=200,
        statement_timeout_ms=5000,
        include_system_schemas=False,
    )
    service = IntrospectionService(adapter=DummyAdapter(), settings=settings)
    result = service.select_columns(
        table="sample_table",
        columns=["id"],
        schema="sample_schema_a",
        limit=999,
    )
    assert result["ok"] is True
    assert result["meta"]["truncated"] is True
    assert result["meta"]["warnings"]
