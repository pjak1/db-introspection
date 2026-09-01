from __future__ import annotations

import csv
import io
import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Any

from src.errors import ValidationError

logger = logging.getLogger(__name__)

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

# Retention sweep of the export directory (see prune_export_dir). These are
# server-wide filesystem settings, so they follow the DB_INTROSPECTION_ env
# convention like the export directory itself, not per-connection db_conn.txt.
_RETENTION_DAYS_ENV = "DB_INTROSPECTION_EXPORT_RETENTION_DAYS"
_KEEP_LAST_ENV = "DB_INTROSPECTION_EXPORT_KEEP_LAST"
_PRUNE_INTERVAL_ENV = "DB_INTROSPECTION_EXPORT_PRUNE_INTERVAL_MIN"
_DEFAULT_RETENTION_DAYS = 7
_DEFAULT_KEEP_LAST = 200
_DEFAULT_PRUNE_INTERVAL_MIN = 60
# Never delete a file modified within this window: protects an export another
# instance may still be streaming (the export dir is shared across every server
# instance running in each VS Code window).
_PRUNE_GRACE_SECONDS = 120
# Rate-limit marker written into the export directory; excluded from deletion
# because it carries no export extension.
_PRUNE_STAMP_NAME = ".prune_stamp"
# At most one prune runs per process at a time; extra exports skip immediately.
_prune_lock = threading.Lock()


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


def _env_int(name: str, default: int) -> int:
    """Read a non-negative int from the environment, falling back on any bad value.

    Pruning must never break an export, so a malformed override degrades to the
    default instead of raising.
    """
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = int(raw.strip())
    except ValueError:
        return default
    return value if value >= 0 else default


def _prune_rate_limited(stamp: Path, interval_seconds: int) -> bool:
    """Return True if a sweep ran too recently; otherwise stamp 'now' and return False.

    The marker's mtime is the shared clock across every server instance, so exports
    from different VS Code windows don't each re-sweep the same directory. Stamped
    before sweeping so concurrent instances mostly avoid double work. All marker I/O
    is best-effort: a filesystem hiccup must not stop pruning or the export.
    """
    try:
        if stamp.exists() and (time.time() - stamp.stat().st_mtime) < interval_seconds:
            return True
    except OSError:
        pass
    try:
        stamp.touch()
    except OSError:
        pass
    return False


def prune_export_dir(
    directory: Path | None = None,
    *,
    retention_days: int | None = None,
    keep_last: int | None = None,
    interval_min: int | None = None,
) -> int:
    """Delete stale export files from `directory` (default `export_base_dir()`).

    A candidate is a regular `.csv`/`.json` file directly in the directory. A
    candidate is deleted when it is older than `retention_days` (age rule, applied
    unconditionally) OR ranks beyond the `keep_last` newest (count rule) — bounding
    both age and number. Files modified within `_PRUNE_GRACE_SECONDS` are never
    touched (an in-progress export from another instance). Rate-limited to once per
    `interval_min` via a marker file. Every deletion tolerates races
    (`ENOENT`/`PermissionError`) and the whole sweep swallows errors so it can never
    break an export. Returns the number of files deleted.
    """
    base = directory if directory is not None else export_base_dir()
    days = _DEFAULT_RETENTION_DAYS if retention_days is None else retention_days
    keep = _DEFAULT_KEEP_LAST if keep_last is None else keep_last
    interval = _DEFAULT_PRUNE_INTERVAL_MIN if interval_min is None else interval_min

    if days <= 0 and keep <= 0:
        return 0
    try:
        if not base.is_dir():
            return 0
    except OSError:
        return 0

    if _prune_rate_limited(base / _PRUNE_STAMP_NAME, interval * 60):
        return 0

    try:
        return _sweep(base, days, keep)
    except Exception:  # noqa: BLE001 — pruning must never propagate into an export
        logger.debug("export prune sweep failed", exc_info=True)
        return 0


def _sweep(base: Path, days: int, keep: int) -> int:
    """Core deletion pass; see prune_export_dir for the policy."""
    extensions = set(_EXPORT_EXTENSIONS.values())
    now = time.time()
    max_age = days * 86400

    candidates: list[tuple[float, Path]] = []
    for entry in base.iterdir():
        try:
            if not entry.is_file() or entry.suffix.lower() not in extensions:
                continue
            candidates.append((entry.stat().st_mtime, entry))
        except OSError:
            continue

    # Newest first, so index i is the file's recency rank (0 = newest).
    candidates.sort(key=lambda item: item[0], reverse=True)

    deleted = 0
    for rank, (mtime, path) in enumerate(candidates):
        if (now - mtime) < _PRUNE_GRACE_SECONDS:
            continue  # too fresh — may still be streaming
        too_old = days > 0 and (now - mtime) > max_age
        overflow = keep > 0 and rank >= keep
        if not (too_old or overflow):
            continue
        try:
            path.unlink()
            deleted += 1
        except OSError:
            continue  # already gone / locked by AV or another instance — retry next sweep
    if deleted:
        logger.info("pruned %d export file(s)", deleted)
    return deleted


def prune_export_dir_async() -> None:
    """Run `prune_export_dir` in a fire-and-forget daemon thread.

    Keeps the sweep off the export tool's response path. A non-blocking lock means
    that if a prune is already running in this process, this call returns at once
    instead of piling up threads when several exports fire close together.
    """
    if not _prune_lock.acquire(blocking=False):
        return

    def _run() -> None:
        try:
            prune_export_dir()
        finally:
            _prune_lock.release()

    threading.Thread(target=_run, name="export-prune", daemon=True).start()
