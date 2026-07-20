from __future__ import annotations

import csv
import json
import re
from pathlib import Path
from typing import Any

from src.adapters.base import AdapterResult
from src.adapters.normalization import normalize_rows

# Rows fetched per round-trip while streaming an export to disk. Bounds memory
# regardless of the total result size.
_FETCH_BATCH = 1000

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


def _row_writer(fmt: str, fh: Any, columns: list[str]):
    """Return (emit_header, emit_row, finish) closures for the given format.

    CSV emits a header row then one line per record. JSON streams a single array
    of objects, writing each element as it arrives so the full result set is
    never held in memory.
    """
    if fmt == "csv":
        writer = csv.writer(fh)

        def emit_header() -> None:
            writer.writerow(columns)

        def emit_row(row: Any) -> None:
            writer.writerow(["" if value is None else value for value in row])

        def finish() -> None:
            return None

        return emit_header, emit_row, finish

    # json
    state = {"first": True}

    def emit_header() -> None:
        fh.write("[")

    def emit_row(row: Any) -> None:
        obj = {col: row[idx] for idx, col in enumerate(columns)}
        fh.write(("" if state["first"] else ",") + json.dumps(obj, default=str, ensure_ascii=False))
        state["first"] = False

    def finish() -> None:
        fh.write("]")

    return emit_header, emit_row, finish


def stream_cursor_to_file(cursor: Any, destination: Path, fmt: str, max_rows: int) -> AdapterResult:
    """Stream an executed cursor's rows to `destination`, batched and bounded.

    Writes at most `max_rows` rows. The caller is expected to have executed a
    query capped at `max_rows + 1`, so a surviving extra row means the result was
    truncated. Returns an AdapterResult whose data is a summary dict:
    {path, format, row_count, byte_size, truncated}.
    """
    columns = [str(desc[0]) for desc in (cursor.description or [])]
    written = 0
    truncated = False

    with destination.open("w", encoding="utf-8", newline="") as fh:
        emit_header, emit_row, finish = _row_writer(fmt, fh, columns)
        emit_header()
        done = False
        while not done:
            batch = cursor.fetchmany(_FETCH_BATCH)
            if not batch:
                break
            for row in batch:
                if written >= max_rows:
                    truncated = True
                    done = True
                    break
                emit_row(row)
                written += 1
        finish()

    summary = {
        "path": str(destination),
        "format": fmt,
        "row_count": written,
        "byte_size": destination.stat().st_size,
        "truncated": truncated,
    }
    return AdapterResult(data=summary)
