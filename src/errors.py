from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class AppError(Exception):
    """Base application error with stable API code and optional details."""
    code: str
    message: str
    details: Any | None = None

    def __str__(self) -> str:
        return f"{self.code}: {self.message}"


class ConfigError(AppError):
    """Configuration-related application error."""
    pass


class ValidationError(AppError):
    """Input validation application error."""
    pass


class DatabaseError(AppError):
    """Database interaction application error."""
    pass
