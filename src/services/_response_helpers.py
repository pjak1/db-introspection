from __future__ import annotations

import functools
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Concatenate, ParamSpec, Protocol, TypeVar

from src.adapters.base import AdapterResult, DatabaseAdapter
from src.contracts import (
    Envelope,
    ErrorEnvelope,
    SuccessEnvelope,
    error_envelope,
    success_envelope,
)
from src.errors import AppError

_P = ParamSpec("_P")


class _ServiceLike(Protocol):
    """Structural type for services that `service_operation` can wrap."""
    _adapter: DatabaseAdapter


_S = TypeVar("_S", bound=_ServiceLike)


def elapsed_ms(started: float) -> int:
    """Return elapsed milliseconds from a `time.perf_counter()` start value."""
    return int((time.perf_counter() - started) * 1000)


@dataclass
class Ok:
    """Success payload returned by a service method.

    The service method returns this data-only object; `service_operation` adds the
    dialect and timing and turns it into a `SuccessEnvelope`. This keeps the
    envelope-building logic in one place instead of each method (and each service)
    calling `success_envelope` on its own.
    """
    result: AdapterResult
    schema_used: str | None = None
    truncated: bool = False
    extra_warnings: tuple[str, ...] | list[str] = field(default_factory=tuple)


def success_from_result(dialect: str, started: float, ok: Ok) -> SuccessEnvelope:
    """Build a success envelope from an `Ok` payload with shared timing/shape.

    Single source of truth for the successful response shape used by every
    service operation.
    """
    return success_envelope(
        dialect=dialect,
        data=ok.result.data,
        duration_ms=elapsed_ms(started),
        truncated=ok.truncated or ok.result.truncated,
        schema_used=ok.schema_used,
        warnings=[*ok.extra_warnings, *ok.result.warnings],
        status=ok.result.status,
    )


def envelope_for_error(dialect: str, duration_ms: int, err: Exception) -> ErrorEnvelope:
    """Map any exception to a standardized error envelope.

    The single `AppError -> envelope` mapping shared by the timed service path
    (`error_from_exception`) and the untimed resolution path in `server.py`.
    """
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


def error_from_exception(dialect: str, started: float, err: Exception) -> ErrorEnvelope:
    """Translate an exception into a timed error envelope."""
    return envelope_for_error(dialect, elapsed_ms(started), err)


def service_operation(
    method: Callable[Concatenate[_S, _P], Ok],
) -> Callable[Concatenate[_S, _P], Envelope]:
    """Time a service method and shape its result into a response envelope.

    The wrapped method receives only its public arguments and returns an `Ok`
    payload on success; this wrapper adds the dialect and timing (turning it into
    a success envelope) and converts any raised exception into an error envelope.
    The parameter types are preserved while the public return type becomes
    `Envelope`, so callers and type checkers see an honest signature.
    """
    @functools.wraps(method)
    def wrapper(self: _S, *args: _P.args, **kwargs: _P.kwargs) -> Envelope:
        started = time.perf_counter()
        dialect = self._adapter.dialect
        try:
            return success_from_result(dialect, started, method(self, *args, **kwargs))
        except Exception as err:
            return error_from_exception(dialect, started, err)
    return wrapper
