from __future__ import annotations

import time

from src.contracts import ErrorEnvelope, error_envelope
from src.errors import AppError


def elapsed_ms(started: float) -> int:
    """Return elapsed milliseconds from a `time.perf_counter()` start value."""
    return int((time.perf_counter() - started) * 1000)


def error_from_exception(dialect: str, started: float, err: Exception) -> ErrorEnvelope:
    """Translate arbitrary exceptions into a standardized error envelope."""
    duration_ms = elapsed_ms(started)
    if isinstance(err, AppError):
        return error_envelope(
            dialect=dialect,
            code=err.code,
            message=err.message,
            duration_ms=duration_ms,
            details=err.details,
        )
    return error_envelope(
        dialect=dialect,
        code="internal_error",
        message="Unexpected internal error.",
        duration_ms=duration_ms,
        details=str(err),
    )
