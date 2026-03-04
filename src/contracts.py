from __future__ import annotations

from typing import Any, Literal, TypedDict


class ErrorBody(TypedDict):
    """Structured error payload used by failed response envelopes."""
    code: str
    message: str
    details: Any | None


class MetaBody(TypedDict):
    """Metadata payload shared by successful and failed response envelopes."""
    duration_ms: int
    row_count: int | None
    truncated: bool
    schema_used: str | None
    warnings: list[str]
    status: str | None


class SuccessEnvelope(TypedDict):
    """Successful MCP response envelope."""
    ok: Literal[True]
    dialect: str
    data: Any
    meta: MetaBody
    error: None


class ErrorEnvelope(TypedDict):
    """Failed MCP response envelope."""
    ok: Literal[False]
    dialect: str
    data: None
    meta: MetaBody
    error: ErrorBody


type Envelope = SuccessEnvelope | ErrorEnvelope


def _compute_row_count(data: Any) -> int | None:
    """Infer row count from common payload shapes used by MCP responses."""
    if data is None:
        return 0
    if isinstance(data, list):
        return len(data)
    if isinstance(data, dict):
        return 1
    return None


def success_envelope(
    *,
    dialect: str,
    data: Any,
    duration_ms: int,
    row_count: int | None = None,
    truncated: bool = False,
    schema_used: str | None = None,
    warnings: list[str] | None = None,
    status: str | None = None,
) -> SuccessEnvelope:
    """Build a successful response envelope shared by all MCP tools."""
    warning_list = warnings or []
    return {
        "ok": True,
        "dialect": dialect,
        "data": data,
        "meta": {
            "duration_ms": duration_ms,
            "row_count": _compute_row_count(data) if row_count is None else row_count,
            "truncated": truncated,
            "schema_used": schema_used,
            "warnings": warning_list,
            "status": status,
        },
        "error": None,
    }


def error_envelope(
    *,
    dialect: str,
    code: str,
    message: str,
    duration_ms: int,
    details: Any | None = None,
    schema_used: str | None = None,
    warnings: list[str] | None = None,
) -> ErrorEnvelope:
    """Build a failed response envelope shared by all MCP tools."""
    return {
        "ok": False,
        "dialect": dialect,
        "data": None,
        "meta": {
            "duration_ms": duration_ms,
            "row_count": 0,
            "truncated": False,
            "schema_used": schema_used,
            "warnings": warnings or [],
            "status": None,
        },
        "error": {
            "code": code,
            "message": message,
            "details": details,
        },
    }
