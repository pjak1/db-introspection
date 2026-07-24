# DDL + data-write capability plugin (installed manually into plugins/).
#
# Registers THREE mutating tools in one plugin:
#   - db_execute_ddl(connection, sql)              -> CREATE / ALTER / DROP / TRUNCATE / ...
#   - db_execute_dml(connection, sql, params)      -> INSERT / UPDATE / DELETE / MERGE
#   - db_call_procedure(connection, procedure, args)-> CALL / EXEC a stored procedure
#
# Both are inert unless the server is started with DB_INTROSPECTION_ENABLE_WRITE_PLUGINS=1,
# and both refuse any connection whose db_conn.txt does not set `writable: true`.
# All mutation code lives HERE (in the plugin), never in the read-only core.
#
# Classification LIMITATION (deliberate, not a bug): the DDL/DML split is a guard
# on the leading keyword, not a full SQL parse. It does NOT reject multiple
# ';'-separated statements, because legitimate DDL (Oracle/T-SQL procedure,
# trigger and package bodies) contains internal semicolons; and a read-only
# WITH...SELECT naming a table like a DML verb may be accepted by db_execute_dml
# (it still only runs a read). The real safety boundary is the enable flag plus
# the per-connection allowlist, which decide whether any write runs at all.

from __future__ import annotations

import re
import time

from src.adapters.base import DatabaseAdapter
from src.adapters.normalization import normalize_rows
from src.plugins.api import (
    MUTATING,
    DatabaseError,
    Envelope,
    PluginContext,
    ValidationError,
    elapsed_ms,
    error_from_exception,
    success_envelope,
)

# Statement kinds, classified by the leading keyword. Used to keep DDL and data
# writes on their own tools (an INSERT sent to db_execute_ddl is rejected, etc.).
_DDL_KEYWORDS = frozenset(
    {"create", "alter", "drop", "truncate", "comment", "rename", "grant", "revoke"}
)
_DML_KEYWORDS = frozenset({"insert", "update", "delete", "merge"})

# A (optionally schema/package-qualified) stored-procedure name, 1–3 dot-separated
# identifier parts. The name is interpolated into the CALL statement (identifiers
# can't be bound as parameters), so it is validated strictly to prevent injection;
# the procedure's ARGUMENTS are always passed as bound parameters, never inlined.
_PROC_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$#]*(?:\.[A-Za-z_][A-Za-z0-9_$#]*){0,2}$")

_LEADING_LINE_COMMENT = re.compile(r"^\s*--[^\n]*\n")
_LEADING_BLOCK_COMMENT = re.compile(r"^\s*/\*.*?\*/", re.DOTALL)

# A data-modifying keyword appearing anywhere as a whole word — used to accept a
# `WITH ... INSERT/UPDATE/DELETE/MERGE` (CTE) statement while rejecting a pure
# `WITH ... SELECT` read on the data-write tool.
_DML_PATTERN = re.compile(r"\b(?:insert|update|delete|merge)\b", re.IGNORECASE)

# Comments, string literals and quoted/dollar-quoted spans, so keyword scanning
# never matches text inside them.
_NONCODE = re.compile(
    r"/\*.*?\*/"                 # block comment
    r"|--[^\n]*"                 # line comment
    r"|'(?:[^']|'')*'"           # single-quoted string
    r'|"(?:[^"]|"")*"'           # double-quoted identifier
    r"|\$(\w*)\$.*?\$\1\$",      # dollar-quoted string
    re.DOTALL,
)


def register(context: PluginContext) -> None:
    """Entry point called by the loader; register both write tools."""

    def db_execute_ddl(connection: str, sql: str) -> Envelope:
        """Run a single DDL statement (CREATE/ALTER/DROP/TRUNCATE/...).

        `connection` is a 'project/environment/schema' key and its db_conn.txt
        must set `writable: true`. Returns a status envelope.
        """
        return _run(context, connection, sql, None, _require_ddl, "DDL")

    def db_execute_dml(connection: str, sql: str, params: list | None = None) -> Envelope:
        """Run a single data-modifying statement (INSERT/UPDATE/DELETE/MERGE).

        Also accepts a leading `WITH` (CTE) as long as the statement actually
        modifies data, e.g. `WITH c AS (...) INSERT INTO t SELECT ... FROM c` or a
        data-modifying CTE; a plain `WITH ... SELECT` read is rejected.

        `connection` is a 'project/environment/schema' key and its db_conn.txt
        must set `writable: true`. `params` binds placeholders using the driver's
        own style (psycopg `%s`, oracledb `:name`, pyodbc `?`). Returns the number
        of affected rows.
        """
        return _run(context, connection, sql, params, _require_dml, "DML")

    def db_call_procedure(
        connection: str, procedure: str, args: list | None = None
    ) -> Envelope:
        """Call a stored procedure (Oracle/PostgreSQL/SQL Server).

        `procedure` is a (optionally schema/package-qualified) name such as
        `proc`, `schema.proc` or Oracle `pkg.proc`. `args` are IN parameters bound
        safely by the driver (never inlined). When the procedure produces a result
        set, its first result set is returned in `rows`; otherwise `rows` is null.

        `connection` is a 'project/environment/schema' key and its db_conn.txt must
        set `writable: true`. Returns {procedure, rows_affected, rows}.
        """
        return _call(context, connection, procedure, args)

    context.mcp.add_tool(
        db_execute_ddl, name="db_execute_ddl", annotations=MUTATING)
    context.mcp.add_tool(
        db_execute_dml, name="db_execute_dml", annotations=MUTATING)
    context.mcp.add_tool(
        db_call_procedure, name="db_call_procedure", annotations=MUTATING)


def _run(
    context: PluginContext,
    connection: str,
    sql: str,
    params: list | None,
    validate,
    kind: str,
) -> Envelope:
    started = time.perf_counter()

    # Authorize first: an un-allowlisted connection must not even trigger settings
    # resolution, and gets a clear write_not_allowed error instead of a classifier
    # message. Building the adapter next lets later error envelopes carry a dialect.
    try:
        context.require_writable(connection)
        adapter = context.adapter_for(connection)
    except Exception as exc:  # authorization or config/validation errors
        return error_from_exception("unknown", started, exc)

    dialect = adapter.dialect
    try:
        validate(sql)
        affected = _execute(adapter, sql, params)
        return success_envelope(
            dialect=dialect,
            data={"rows_affected": affected, "kind": kind},
            duration_ms=elapsed_ms(started),
            status="ok",
        )
    except Exception as exc:
        return error_from_exception(dialect, started, exc)


def _call(
    context: PluginContext,
    connection: str,
    procedure: str,
    args: list | None,
) -> Envelope:
    started = time.perf_counter()

    # Authorize before touching settings, exactly like _run: an un-allowlisted
    # connection gets write_not_allowed, and building the adapter gives later
    # error envelopes a real dialect.
    try:
        context.require_writable(connection)
        adapter = context.adapter_for(connection)
    except Exception as exc:
        return error_from_exception("unknown", started, exc)

    dialect = adapter.dialect
    try:
        _require_proc_name(procedure)
        affected, rows = _call_procedure(adapter, procedure, args)
        return success_envelope(
            dialect=dialect,
            data={"procedure": procedure, "rows_affected": affected, "rows": rows},
            duration_ms=elapsed_ms(started),
            status="ok",
        )
    except Exception as exc:
        return error_from_exception(dialect, started, exc)


def _require_proc_name(procedure: str) -> None:
    """Validate the procedure name (interpolated into SQL) against injection."""
    name = (procedure or "").strip()
    if not name:
        raise ValidationError("invalid_procedure_name", "Procedure name is empty.")
    if not _PROC_NAME_RE.match(name):
        raise ValidationError(
            "invalid_procedure_name",
            "Procedure name must be 1-3 dot-separated identifiers "
            "(e.g. 'proc', 'schema.proc', 'pkg.proc').",
        )


def _require_ddl(sql: str) -> None:
    """Accept only statements whose leading keyword is a DDL verb."""
    keyword = _first_keyword(sql)
    if not keyword:
        raise ValidationError("invalid_statement", "SQL statement is empty.")
    if keyword not in _DDL_KEYWORDS:
        raise ValidationError(
            "invalid_statement",
            f"'{keyword}' is not a DDL statement for this tool.",
        )


def _require_dml(sql: str) -> None:
    """Accept a DML verb, or a WITH (CTE) that actually modifies data."""
    keyword = _first_keyword(sql)
    if not keyword:
        raise ValidationError("invalid_statement", "SQL statement is empty.")
    if keyword in _DML_KEYWORDS:
        return
    if keyword == "with":
        if _DML_PATTERN.search(_strip_noncode(sql)):
            return
        raise ValidationError(
            "invalid_statement",
            "WITH statement must modify data (INSERT/UPDATE/DELETE/MERGE); "
            "use db_run_select for read-only CTEs.",
        )
    raise ValidationError(
        "invalid_statement",
        f"'{keyword}' is not a DML statement for this tool.",
    )


def _strip_noncode(sql: str) -> str:
    """Blank out comments and string/quoted spans before keyword scanning."""
    return _NONCODE.sub(" ", sql or "")


def _first_keyword(sql: str) -> str:
    """Return the lowercase leading keyword, skipping leading comments/whitespace."""
    text = sql or ""
    while True:
        stripped = _LEADING_LINE_COMMENT.sub("", text, count=1)
        stripped = _LEADING_BLOCK_COMMENT.sub("", stripped, count=1)
        if stripped == text:
            break
        text = stripped
    match = re.match(r"\s*([A-Za-z]+)", text)
    return match.group(1).lower() if match else ""


def _execute(adapter: DatabaseAdapter, sql: str, params: list | None) -> int:
    """Open a writable connection via the adapter, run one statement, commit.

    Dialect-agnostic: the adapter's `open_connection()` is the same connection
    the read-only path uses, so this covers exactly the core DBs the server
    supports. The plugin owns the mutation — it commits explicitly (mandatory
    for Oracle, which otherwise rolls back on close).
    """
    try:
        with adapter.open_connection() as conn:
            with conn.cursor() as cur:
                if params:
                    cur.execute(sql, params)
                else:
                    cur.execute(sql)
                # Defensive: a DML batch / OUTPUT clause can return result sets on
                # MSSQL; drain them so the whole batch runs before commit/close.
                if adapter.dialect == "mssql":
                    while cur.nextset():
                        pass
                affected = cur.rowcount
            conn.commit()
            return affected
    except Exception as exc:
        raise DatabaseError("database_error", "Write failed.",
                            details=str(exc)) from exc


def _call_procedure(
    adapter: DatabaseAdapter, procedure: str, args: list | None
) -> tuple[int, list[dict] | None]:
    """Call a stored procedure with the dialect's syntax, commit, return results.

    IN args are bound by the driver (never inlined). Any first result set the
    procedure produces is fetched BEFORE commit (some drivers invalidate the
    cursor on commit) and returned as normalized rows; otherwise rows is None.
    The plugin owns the mutation, so it commits explicitly (mandatory for Oracle).
    """
    bind = list(args) if args else []
    dialect = adapter.dialect
    try:
        with adapter.open_connection() as conn:
            with conn.cursor() as cur:
                if dialect == "oracle":
                    # callproc handles binding and package-qualified names.
                    cur.callproc(procedure, bind)
                elif dialect == "postgres":
                    placeholders = ", ".join(["%s"] * len(bind))
                    cur.execute(f"CALL {procedure}({placeholders})", bind)
                elif dialect == "mssql":
                    # ODBC call escape; pyodbc has no reliable callproc.
                    inner = f" ({', '.join(['?'] * len(bind))})" if bind else ""
                    cur.execute(f"{{CALL {procedure}{inner}}}", bind)
                else:
                    raise DatabaseError(
                        "unsupported_dialect",
                        f"Procedure calls are not supported for dialect '{dialect}'.",
                    )

                rows: list[dict] | None = None
                if cur.description is not None:
                    columns = [desc[0] for desc in cur.description]
                    rows = normalize_rows(
                        [dict(zip(columns, row)) for row in cur.fetchall()])
                # Drain any further result sets so the procedure runs to
                # COMPLETION server-side. pyodbc (MSSQL) hands control back at the
                # first mid-procedure result set; committing/closing now aborts the
                # rest. A later set erroring raises here (nextset/fetch) — desirable:
                # real procedure errors surface instead of a silent partial "ok".
                if dialect == "mssql":
                    while cur.nextset():
                        pass
                affected = cur.rowcount
            conn.commit()
            return affected, rows
    except DatabaseError:
        raise
    except Exception as exc:
        raise DatabaseError("database_error", "Procedure call failed.",
                            details=str(exc)) from exc
