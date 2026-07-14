from conftest import BaseStubAdapter, make_settings

from src.adapters.base import AdapterResult
from src.services.introspection_service import IntrospectionService


class DummyAdapter(BaseStubAdapter):
    def __init__(self, dialect: str, jobs_data: list[dict]):
        self._dialect = dialect
        self._jobs_data = jobs_data

    def list_jobs(self) -> AdapterResult:
        return AdapterResult(data=self._jobs_data, status="available")


def _settings(*, db_dialect: str, allowed_schemas: tuple[str, ...]):
    return make_settings(db_dialect=db_dialect, db_dsn="db://test", allowed_schemas=allowed_schemas)


def test_list_jobs_oracle_validates_schema_param():
    jobs = [
        {"schema": "SAMPLE_SCHEMA", "job_name": "A"},
        {"schema": "OTHER", "job_name": "B"},
    ]
    service = IntrospectionService(
        adapter=DummyAdapter(dialect="oracle", jobs_data=jobs),
        settings=_settings(db_dialect="oracle", allowed_schemas=("sample_schema",)),
    )
    result = service.list_jobs(schema="sample_schema")
    assert result["ok"] is True
    assert result["meta"]["schema_used"] == "sample_schema"
    assert result["data"] == jobs


def test_list_jobs_oracle_rejects_non_allowed_schema():
    service = IntrospectionService(
        adapter=DummyAdapter(dialect="oracle", jobs_data=[{"job_name": "A"}]),
        settings=_settings(db_dialect="oracle", allowed_schemas=("sample_schema",)),
    )
    result = service.list_jobs(schema="secret")
    assert result["ok"] is False
    assert result["error"]["code"] == "invalid_schema"


def test_list_jobs_postgres_validates_schema_param():
    service = IntrospectionService(
        adapter=DummyAdapter(dialect="postgres", jobs_data=[{"jobid": 1}]),
        settings=_settings(db_dialect="postgres", allowed_schemas=("public",)),
    )
    result = service.list_jobs(schema="public")
    assert result["ok"] is True
    assert result["meta"]["schema_used"] == "public"
    assert result["data"] == [{"jobid": 1}]


def test_list_jobs_postgres_requires_schema_and_rejects_non_allowed():
    service = IntrospectionService(
        adapter=DummyAdapter(dialect="postgres", jobs_data=[{"jobid": 1}]),
        settings=_settings(db_dialect="postgres", allowed_schemas=("public",)),
    )
    result_missing = service.list_jobs(schema="")
    assert result_missing["ok"] is False
    assert result_missing["error"]["code"] == "missing_schema"

    result_cron = service.list_jobs(schema="cron")
    assert result_cron["ok"] is False
    assert result_cron["error"]["code"] == "invalid_schema"
