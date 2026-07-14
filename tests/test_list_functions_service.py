from conftest import BaseStubAdapter, make_settings

from src.adapters.base import AdapterResult
from src.errors import ValidationError
from src.services.introspection_service import IntrospectionService


class DummyAdapter(BaseStubAdapter):
    def list_functions(self, schemas) -> AdapterResult:
        raise ValidationError("invalid_functions_query", "Simulated adapter failure.")


def test_list_functions_returns_error_envelope_when_adapter_raises():
    service = IntrospectionService(adapter=DummyAdapter(), settings=make_settings())

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
