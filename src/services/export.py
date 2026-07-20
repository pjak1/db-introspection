from __future__ import annotations

import csv
import io
import json
import os
from pathlib import Path
from typing import Any

from src.errors import ValidationError

# NOTE: the low-level cursor->file streaming writer lives in
# src/adapters/_sql_helpers.py (adapter layer, operates on DB cursors). This
# module keeps the service-layer concerns: format validation, export-path
# resolution/safety, and the row-count ceiling.

# Output formats supported by the data-returning tools. `rows` is the default
# structured payload (list of dicts); `csv`/`json` serialize it to a string so an
# agent can hand the result straight to a file or another tool.
OUTPUT_FORMATS = ("rows", "csv", "json")

# Formats for the file-export tools. `rows` is intentionally excluded: an export
# always produces a file, so it must be a serializable text format.
EXPORT_FORMATS = ("csv", "json")
_EXPORT_EXTENSIONS = {"csv": ".csv", "json": ".json"}

# Environment override for the export directory; when unset, exports land in an
# `exports/` folder at the project root (gitignored). This is a server-wide
# filesystem location, so it follows the DB_INTROSPECTION_ env convention rather
# than per-connection db_conn.txt settings.
_EXPORT_DIR_ENV = "DB_INTROSPECTION_EXPORT_DIR"
_DEFAULT_EXPORT_DIRNAME = "exports"


def normalize_output_format(fmt: str | None) -> str:
    """Validate and normalize the requested output format (defaults to 'rows')."""
    value = (fmt or "rows").strip().lower()
    if value not in OUTPUT_FORMATS:
        raise ValidationError(
            "invalid_format",
            f"format must be one of: {', '.join(OUTPUT_FORMATS)}.",
        )
    return value


def normalize_export_format(fmt: str | None) -> str:
    """Validate and normalize a file-export format (defaults to 'csv')."""
    value = (fmt or "csv").strip().lower()
    if value not in EXPORT_FORMATS:
        raise ValidationError(
            "invalid_format",
            f"format must be one of: {', '.join(EXPORT_FORMATS)}.",
        )
    return value


def export_base_dir() -> Path:
    """Return the directory exports are written to.

    `DB_INTROSPECTION_EXPORT_DIR` wins when set; otherwise defaults to
    `<project root>/exports`. The path is resolved but not created here.
    """
    override = os.environ.get(_EXPORT_DIR_ENV)
    if override and override.strip():
        return Path(override.strip()).expanduser().resolve()
    # src/services/export.py -> project root (parent of src/)
    return (Path(__file__).resolve().parents[2] / _DEFAULT_EXPORT_DIRNAME).resolve()


def _sanitize_stem(text: str) -> str:
    """Reduce arbitrary text to a safe filename stem (alnum, '.', '_', '-')."""
    cleaned = "".join(c if (c.isalnum() or c in "._-") else "_" for c in text)
    return cleaned.strip("._") or "export"


def resolve_export_path(filename: str | None, fmt: str, default_stem: str) -> Path:
    """Resolve a safe absolute export path inside the export directory.

    A caller-supplied `filename` must be a bare name (no path separators, no
    `..`, not absolute); the correct extension for `fmt` is appended when
    missing. The final path is verified to stay inside the export directory so a
    crafted name can never escape it. The directory is created on demand.
    """
    base = export_base_dir()
    base.mkdir(parents=True, exist_ok=True)
    ext = _EXPORT_EXTENSIONS[fmt]

    if filename and filename.strip():
        raw = filename.strip()
        if "/" in raw or "\\" in raw or ".." in raw or Path(raw).is_absolute():
            raise ValidationError(
                "invalid_filename",
                "filename must be a bare name without path separators or '..'.",
            )
        stem = raw
    else:
        stem = _sanitize_stem(default_stem)

    if not stem.lower().endswith(ext):
        stem += ext

    base_resolved = base.resolve()
    dest = (base_resolved / stem).resolve()
    if dest != base_resolved and base_resolved not in dest.parents:
        raise ValidationError(
            "invalid_filename",
            "resolved export path escapes the export directory.",
        )
    return dest


def effective_export_limit(requested: int | None, ceiling: int) -> tuple[int, list[str]]:
    """Clamp a requested export row count to the configured ceiling.

    Returns the effective cap and any warning about the reduction.
    """
    if requested is None:
        return ceiling, []
    value = max(1, int(requested))
    if value > ceiling:
        return ceiling, [
            f"Requested max_rows {value} was reduced to the configured "
            f"max_export_rows {ceiling}."
        ]
    return value, []


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
