from src.contracts import error_envelope, success_envelope


def test_success_envelope_runtime_shape():
    result = success_envelope(
        dialect="postgres",
        data=[{"id": 1}],
        duration_ms=12,
        truncated=False,
        schema_used="public",
        warnings=["warn"],
        status="available",
    )

    assert result["ok"] is True
    assert result["dialect"] == "postgres"
    assert result["data"] == [{"id": 1}]
    assert result["error"] is None
    assert result["meta"]["duration_ms"] == 12
    assert result["meta"]["row_count"] == 1
    assert result["meta"]["truncated"] is False
    assert result["meta"]["schema_used"] == "public"
    assert result["meta"]["warnings"] == ["warn"]
    assert result["meta"]["status"] == "available"


def test_error_envelope_runtime_shape():
    result = error_envelope(
        dialect="oracle",
        code="invalid_sql",
        message="Only SELECT statements are allowed.",
        duration_ms=5,
        details={"line": 1},
        schema_used=None,
        warnings=[],
    )

    assert result["ok"] is False
    assert result["dialect"] == "oracle"
    assert result["data"] is None
    assert result["meta"]["duration_ms"] == 5
    assert result["meta"]["row_count"] == 0
    assert result["meta"]["truncated"] is False
    assert result["meta"]["schema_used"] is None
    assert result["meta"]["warnings"] == []
    assert result["meta"]["status"] is None
    assert result["error"]["code"] == "invalid_sql"
    assert result["error"]["message"] == "Only SELECT statements are allowed."
    assert result["error"]["details"] == {"line": 1}
