from __future__ import annotations

import re
from typing import Any

from src.adapters.base import AdapterResult
from src.adapters.normalization import normalize_rows

# Shared, stateless SQL helpers used by the cursor-based adapters (Oracle/MSSQL)
# and, where applicable, PostgreSQL. Kept as free functions so adapters depend on
# them without any change to the class hierarchy.

# Validates a simple `column [ASC|DESC]` ORDER BY expression (single identifier).
ORDER_BY_RE = re.compile(
    r"^\s*([A-Za-z_][A-Za-z0-9_$]*)(?:\s+(asc|desc))?\s*$", re.IGNORECASE)


def int_or_none(value: Any) -> int | None:
    """Convert numeric metadata values to int when present, else None."""
    if value is None or value == "":
        return None
    return int(value)


def rows_from_cursor(cur) -> list[dict]:
    """Normalize a DBAPI cursor result set to a list of lower-cased dict rows.

    Returns an empty list for statements that produce no result set
    (`cur.description is None`).
    """
    if cur.description is None:
        return []
    columns = [desc[0].lower() for desc in cur.description]
    return normalize_rows([dict(zip(columns, row)) for row in cur.fetchall()])


def degraded_or_raise(
    error: Exception,
    *,
    matched: bool,
    warning: str,
    status: str = "not_available",
) -> AdapterResult:
    """Return a degraded (empty + warning) result when `matched`, else re-raise.

    Lets each adapter keep its own dialect-specific error matching while sharing
    the "empty result with a warning, otherwise propagate" shape.
    """
    if matched:
        return AdapterResult(data=[], warnings=[warning], status=status)
    raise error
