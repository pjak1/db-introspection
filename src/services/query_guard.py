from __future__ import annotations

import re
from dataclasses import dataclass, field

from src.adapters.base import DatabaseAdapter
from src.adapters.discovery import ensure_adapter_modules_loaded
from src.errors import ValidationError

_DOLLAR_TAG_RE = re.compile(r"\$[A-Za-z_][A-Za-z0-9_]*\$|\$\$")
_FORBIDDEN_KEYWORDS = (
    "insert",
    "update",
    "delete",
    "alter",
    "drop",
    "create",
    "truncate",
    "grant",
    "revoke",
    "call",
    "do",
    "copy",
)


@dataclass
class GuardedQuery:
    """Container for a sanitized SELECT query and its applied guard metadata."""
    sql: str
    applied_limit: int
    truncated: bool = False
    warnings: list[str] = field(default_factory=list)


def _strip_literals_and_comments(sql_query: str) -> tuple[str, bool]:
    """Remove comments/literals and detect semicolons outside of quoted regions."""
    i = 0
    n = len(sql_query)
    cleaned: list[str] = []
    has_semicolon = False
    dollar_end: str | None = None

    while i < n:
        ch = sql_query[i]
        nxt = sql_query[i + 1] if i + 1 < n else ""

        if dollar_end:
            if sql_query.startswith(dollar_end, i):
                i += len(dollar_end)
                dollar_end = None
            else:
                i += 1
            continue

        if ch == "-" and nxt == "-":
            # Strip single-line comments to avoid false keyword matches.
            i += 2
            while i < n and sql_query[i] != "\n":
                i += 1
            cleaned.append(" ")
            continue

        if ch == "/" and nxt == "*":
            # Strip block comments before keyword scanning.
            i += 2
            while i < n - 1 and not (sql_query[i] == "*" and sql_query[i + 1] == "/"):
                i += 1
            i += 2
            cleaned.append(" ")
            continue

        if ch == "'":
            # Strip single-quoted literals (including escaped single quotes).
            i += 1
            while i < n:
                if sql_query[i] == "'" and i + 1 < n and sql_query[i + 1] == "'":
                    i += 2
                    continue
                if sql_query[i] == "'":
                    i += 1
                    break
                i += 1
            cleaned.append(" ")
            continue

        if ch == '"':
            # Strip quoted identifiers so keywords inside them are ignored.
            i += 1
            while i < n:
                if sql_query[i] == '"' and i + 1 < n and sql_query[i + 1] == '"':
                    i += 2
                    continue
                if sql_query[i] == '"':
                    i += 1
                    break
                i += 1
            cleaned.append(" ")
            continue

        if ch == "$":
            # Strip PostgreSQL dollar-quoted blocks.
            match = _DOLLAR_TAG_RE.match(sql_query, i)
            if match:
                dollar_end = match.group(0)
                i += len(dollar_end)
                cleaned.append(" ")
                continue

        if ch == ";":
            has_semicolon = True
            i += 1
            continue

        cleaned.append(ch.lower())
        i += 1

    return "".join(cleaned), has_semicolon


class QueryGuard:
    """Validate and wrap read-only SQL before adapter execution."""

    def __init__(self, max_select_limit: int, dialect: str):
        """Initialize guard with dialect-specific wrapper and max row limit."""
        self._max_select_limit = max_select_limit
        self._dialect = dialect.strip().lower()
        ensure_adapter_modules_loaded()
        self._adapter_class = DatabaseAdapter.adapter_class_for(self._dialect)
        if self._adapter_class is None:
            raise ValidationError(
                "invalid_config",
                f"Unsupported SQL dialect for query guard: {dialect}",
            )

    def validate_select(self, sql_query: str) -> str:
        """Validate SQL as a single read-only statement and return normalized text."""
        if not sql_query or not sql_query.strip():
            raise ValidationError("invalid_sql", "SQL query cannot be empty.")

        cleaned, has_semicolon = _strip_literals_and_comments(sql_query)
        normalized = re.sub(r"\s+", " ", cleaned).strip()

        if has_semicolon:
            raise ValidationError(
                "invalid_sql",
                "Multiple SQL statements are not allowed.",
            )

        if not normalized:
            raise ValidationError("invalid_sql", "SQL query cannot be empty.")

        first_token = normalized.split(" ", 1)[0]
        if first_token not in {"select", "with"}:
            raise ValidationError(
                "read_only_violation",
                "Only SELECT statements are allowed.",
            )

        for keyword in _FORBIDDEN_KEYWORDS:
            if re.search(rf"\b{keyword}\b", normalized):
                raise ValidationError(
                    "read_only_violation",
                    f"SQL contains forbidden keyword: {keyword}",
                )

        return sql_query.strip().rstrip(";").rstrip()

    def prepare_select(self, sql_query: str, limit: int | None = None) -> GuardedQuery:
        """Validate SQL as single read-only statement and enforce result limit."""
        validated_sql = self.validate_select(sql_query)

        requested_limit = self._max_select_limit if limit is None else max(1, int(limit))
        applied_limit = min(requested_limit, self._max_select_limit)
        truncated = requested_limit > self._max_select_limit

        wrapped = self._adapter_class.wrap_select(
            query=validated_sql, limit=applied_limit)
        warnings: list[str] = []
        if truncated:
            warnings.append(
                f"Requested limit {requested_limit} was reduced to {self._max_select_limit}."
            )
        return GuardedQuery(
            sql=wrapped,
            applied_limit=applied_limit,
            truncated=truncated,
            warnings=warnings,
        )
