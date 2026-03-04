from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal
from uuid import UUID


def normalize_value(value):
    """Convert driver-specific values into JSON-serializable Python values."""
    if value is None:
        return None
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, bytes):
        return value.hex()

    # Oracle LOBs (CLOB/BLOB/NCLOB) must be materialized before connection closes.
    reader = getattr(value, "read", None)
    if callable(reader):
        try:
            return normalize_value(reader())
        except Exception:
            return str(value)

    if isinstance(value, list):
        return [normalize_value(item) for item in value]
    if isinstance(value, tuple):
        return [normalize_value(item) for item in value]
    if isinstance(value, dict):
        return {key: normalize_value(item) for key, item in value.items()}
    return value


def normalize_row(row: dict) -> dict:
    """Normalize all values inside a single row mapping."""
    return {key: normalize_value(value) for key, value in row.items()}


def normalize_rows(rows: list[dict]) -> list[dict]:
    """Normalize all rows returned by an adapter query."""
    return [normalize_row(row) for row in rows]
