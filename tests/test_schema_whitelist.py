from conftest import BaseStubAdapter, make_settings

from src.adapters.base import AdapterResult
from src.services.introspection_service import IntrospectionService


class DummyAdapter(BaseStubAdapter):
    def list_tables(self, schemas, include_system) -> AdapterResult:
        return AdapterResult(data=[{"schema": schemas[0], "table_name": "x"}])


def _settings():
    return make_settings(allowed_schemas=("public", "app"))


def test_allowed_schema_passes():
    service = IntrospectionService(adapter=DummyAdapter(), settings=_settings())
    result = service.list_tables(schema="public", include_system=False)
    assert result["ok"] is True
    assert result["error"] is None


def test_disallowed_schema_fails():
    service = IntrospectionService(adapter=DummyAdapter(), settings=_settings())
    result = service.list_tables(schema="secret", include_system=False)
    assert result["ok"] is False
    assert result["error"]["code"] == "invalid_schema"
