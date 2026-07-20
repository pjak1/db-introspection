from __future__ import annotations

import csv
import io
import json
from typing import Any

from src.errors import ValidationError

# Output formats supported by the data-returning tools. `rows` is the default
# structured payload (list of dicts); `csv`/`json` serialize it to a string so an
# agent can hand the result straight to a file or another tool.
OUTPUT_FORMATS = ("rows", "csv", "json")


def normalize_output_format(fmt: str | None) -> str:
    """Validate and normalize the requested output format (defaults to 'rows')."""
    value = (fmt or "rows").strip().lower()
    if value not in OUTPUT_FORMATS:
        raise ValidationError(
            "invalid_format",
            f"format must be one of: {', '.join(OUTPUT_FORMATS)}.",
        )
    return value


def serialize_rows(data: Any, fmt: str) -> Any:
    """Serialize a list-of-dicts payload to the requested format.

    `rows` returns the data unchanged. `json` returns a JSON string. `csv` returns
    a CSV string (header + rows) when the payload is a non-empty list of dicts;
    any non-tabular payload is returned unchanged so callers never lose data.
    """
    if fmt == "rows":
        return data
    if fmt == "json":
        return json.dumps(data, default=str, ensure_ascii=False)
    # csv
    if not isinstance(data, list) or not data or not isinstance(data[0], dict):
        return data
    fieldnames = list(data[0].keys())
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for row in data:
        writer.writerow({key: ("" if value is None else value) for key, value in row.items()})
    return buffer.getvalue()
