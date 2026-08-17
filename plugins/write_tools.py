# DDL + data-write capability plugin (installed manually into plugins/).
#
# Registers FOUR mutating tools in one plugin:
#   - db_execute_ddl(connection, sql)                        -> CREATE / ALTER / DROP / TRUNCATE / ...
#   - db_execute_dml(connection, sql, params, out_params)    -> INSERT / UPDATE / DELETE / MERGE, or an
#                                                               anonymous PL/SQL block; supports Oracle OUT binds
#   - db_call_procedure(connection, procedure, args)         -> CALL / EXEC a stored procedure
#   - db_import_csv(connection, table, filename, ...)        -> bulk INSERT rows from a CSV file
#
# All are inert unless the server is started with DB_INTROSPECTION_ENABLE_WRITE_PLUGINS=1,
# and all refuse any connection whose db_conn.txt does not set `writable: true`.
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

import csv
import os
import re
import time
from pathlib import Path

from src.adapters.base import DatabaseAdapter
from src.adapters.normalization import normalize_rows, normalize_value
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

# Anonymous PL/SQL blocks (Oracle) are accepted by the data-write tool too, so a
# caller can run `BEGIN pkg.proc(:in, :out); END;` / `DECLARE ... BEGIN ... END;`
# with IN and OUT binds. Like the DML/DDL split this is a lexical leading-keyword
# check, not a parse; the real safety boundary remains the writable-connection gate.
_PLSQL_BLOCK_KEYWORDS = frozenset({"begin", "declare"})

# A (optionally schema/package-qualified) stored-procedure name, 1–3 dot-separated
# identifier parts. The name is interpolated into the CALL statement (identifiers
# can't be bound as parameters), so it is validated strictly to prevent injection;
# the procedure's ARGUMENTS are always passed as bound parameters, never inlined.
_PROC_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$#]*(?:\.[A-Za-z_][A-Za-z0-9_$#]*){0,2}$")

# A single SQL identifier (table/schema/column). Like the procedure name, the
# CSV-import target table and column names are interpolated into the INSERT
# (identifiers can't be bound), so they are validated strictly; the row VALUES are
# always passed as bound parameters, never inlined.
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$#]*$")

# CSV import reads from a sandboxed directory, mirroring the export tools: a bare
# filename resolved inside DB_INTROSPECTION_IMPORT_DIR (or `<project>/imports`),
# never an arbitrary path, so the MCP surface can't be used to read random files.
_IMPORT_DIR_ENV = "DB_INTROSPECTION_IMPORT_DIR"
_DEFAULT_IMPORT_DIRNAME = "imports"

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

    def db_execute_dml(
        connection: str,
        sql: str,
        params: list | dict | None = None,
        out_params: dict | None = None,
    ) -> Envelope:
        """Run a single data-modifying statement, optionally binding OUT parameters.

        Accepts INSERT/UPDATE/DELETE/MERGE, a data-modifying `WITH` (CTE) — a plain
        `WITH ... SELECT` read is rejected — or an anonymous Oracle PL/SQL block
        (`BEGIN ... END;` / `DECLARE ... BEGIN ... END;`).

        `params` are the IN binds, either a positional list or an object of named
        binds (`{"id": 1}`), in the driver's placeholder style (psycopg `%s`,
        oracledb `:name`, pyodbc `?`).

        `out_params` (Oracle only) binds OUT variables and returns their values. It
        is an object mapping each OUT bind name to its type — one of `number`,
        `string`, `clob`, `date`, `timestamp`. Use named IN binds (an object, not a
        list) together with `out_params`, since named and positional binds cannot be
        mixed. Examples:
          - `INSERT INTO t(x) VALUES(:x) RETURNING id INTO :new_id`
            params={"x": 5}, out_params={"new_id": "number"}
          - `BEGIN pkg.proc(p_in => :x, p_out => :res); END;`
            params={"x": 5}, out_params={"res": "string"}

        `connection` is a 'project/environment/schema' key and its db_conn.txt must
        set `writable: true`. Returns {rows_affected, kind} plus `out_params` (a name
        -> value object) when OUT binds were requested. Value shape differs by
        statement: a PL/SQL block OUT bind yields a scalar, while `RETURNING ... INTO`
        yields a list with one entry per affected row (a single-row RETURNING gives a
        one-element list).
        """
        return _run(context, connection, sql, params, _require_dml, "DML", out_params=out_params)

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

    def db_import_csv(
        connection: str,
        table: str,
        filename: str,
        schema: str | None = None,
        columns: list | None = None,
        has_header: bool = True,
        delimiter: str = ",",
        encoding: str = "utf-8-sig",
        null_value: str | None = None,
        batch_size: int = 1000,
    ) -> Envelope:
        """Bulk-insert rows from a CSV file into a table (Oracle/PostgreSQL/SQL Server).

        The CSV must live in the import directory (DB_INTROSPECTION_IMPORT_DIR, or
        `<project>/imports`); `filename` is a bare name — no path separators or `..`.

        Columns: with `columns` given, values are inserted in that order (and the
        first CSV line is skipped when `has_header` is true). Without `columns`,
        `has_header` must be true and the header row names the target columns. Every
        column and the table/schema are validated as SQL identifiers (they are
        interpolated into the INSERT); the row VALUES are always bound parameters.

        Values are inserted as text and rely on the database's implicit conversion
        (numbers convert directly; dates need a format the session accepts). A cell
        equal to `null_value` (when set) becomes SQL NULL. Rows are sent with
        `executemany` in `batch_size` chunks and committed once at the end.

        `connection` is a 'project/environment/schema' key and its db_conn.txt must
        set `writable: true`. Returns {table, columns, rows_inserted}.
        """
        return _import(
            context, connection, table, filename, schema, columns,
            has_header, delimiter, encoding, null_value, batch_size,
        )

    context.mcp.add_tool(
        db_execute_ddl, name="db_execute_ddl", annotations=MUTATING)
    context.mcp.add_tool(
        db_execute_dml, name="db_execute_dml", annotations=MUTATING)
    context.mcp.add_tool(
        db_call_procedure, name="db_call_procedure", annotations=MUTATING)
    context.mcp.add_tool(
        db_import_csv, name="db_import_csv", annotations=MUTATING)


def _run(
    context: PluginContext,
    connection: str,
    sql: str,
    params: list | dict | None,
    validate,
    kind: str,
    out_params: dict | None = None,
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
        affected, out_values = _execute(adapter, sql, params, out_params)
        data = {"rows_affected": affected, "kind": kind}
        if out_values is not None:
            data["out_params"] = out_values
        return success_envelope(
            dialect=dialect,
            data=data,
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


def _import(
    context: PluginContext,
    connection: str,
    table: str,
    filename: str,
    schema: str | None,
    columns: list | None,
    has_header: bool,
    delimiter: str,
    encoding: str,
    null_value: str | None,
    batch_size: int,
) -> Envelope:
    started = time.perf_counter()

    # Authorize before touching settings, exactly like _run/_call.
    try:
        context.require_writable(connection)
        adapter = context.adapter_for(connection)
    except Exception as exc:
        return error_from_exception("unknown", started, exc)

    dialect = adapter.dialect
    try:
        _require_identifier(table, "table")
        if schema:
            _require_identifier(schema, "schema")
        path = _resolve_import_path(context, filename)
        cols, inserted = _load_and_insert(
            adapter, table, schema, path, columns,
            has_header, delimiter, encoding, null_value, batch_size,
        )
        target = f"{schema}.{table}" if schema else table
        return success_envelope(
            dialect=dialect,
            data={"table": target, "columns": cols, "rows_inserted": inserted},
            duration_ms=elapsed_ms(started),
            status="ok",
        )
    except Exception as exc:
        return error_from_exception(dialect, started, exc)


def _resolve_import_path(context: PluginContext, filename: str) -> Path:
    """Resolve a safe absolute CSV path inside the import directory.

    Mirrors the export tools' safety: `filename` must be a bare name (no path
    separators, no `..`, not absolute) and the resolved path is verified to stay
    inside the import directory so a crafted name can never escape it.
    """
    override = os.environ.get(_IMPORT_DIR_ENV)
    if override and override.strip():
        base = Path(override.strip()).expanduser().resolve()
    else:
        base = (context.connection_registry.resolve_project_root()
                / _DEFAULT_IMPORT_DIRNAME).resolve()

    raw = (filename or "").strip()
    if not raw:
        raise ValidationError("invalid_filename", "filename is required.")
    if "/" in raw or "\\" in raw or ".." in raw or Path(raw).is_absolute():
        raise ValidationError(
            "invalid_filename",
            "filename must be a bare name without path separators or '..'.",
        )

    dest = (base / raw).resolve()
    if base not in dest.parents:
        raise ValidationError(
            "invalid_filename",
            "resolved import path escapes the import directory.",
        )
    if not dest.is_file():
        raise ValidationError(
            "file_not_found",
            f"CSV file '{raw}' not found in import directory '{base}'.",
        )
    return dest


def _require_identifier(name: str, kind: str) -> None:
    """Validate a single SQL identifier interpolated into the INSERT statement."""
    value = name.strip() if isinstance(name, str) else ""
    if not value or not _IDENTIFIER_RE.match(value):
        raise ValidationError(
            f"invalid_{kind}",
            f"{kind} '{name}' is not a valid SQL identifier.",
        )


def _build_insert_sql(dialect: str, schema: str | None, table: str, cols: list[str]) -> str:
    """Build a parameterized INSERT with the dialect's placeholder style."""
    n = len(cols)
    if dialect == "oracle":
        placeholders = ", ".join(f":{i + 1}" for i in range(n))
    elif dialect == "postgres":
        placeholders = ", ".join(["%s"] * n)
    elif dialect == "mssql":
        placeholders = ", ".join(["?"] * n)
    else:
        raise DatabaseError(
            "unsupported_dialect",
            f"CSV import is not supported for dialect '{dialect}'.",
        )
    target = f"{schema}.{table}" if schema else table
    return f"INSERT INTO {target} ({', '.join(cols)}) VALUES ({placeholders})"


def _executemany_batch(cur, sql: str, batch: list[list], dialect: str, ncols: int) -> None:
    """Run one executemany, pre-sizing Oracle binds from the whole batch.

    python-oracledb derives each bind's type and size from the FIRST row unless
    told otherwise, so an empty/NULL value in the first CSV row would size that
    column's buffer to ~0 and later rows would overflow — raising ORA-01461 or,
    worse, silently landing data in the wrong column. We defuse that by declaring
    each bind's size explicitly from the actual data in this batch.
    """
    if dialect == "oracle":
        _set_oracle_input_sizes(cur, batch, ncols)
    cur.executemany(sql, batch)


def _set_oracle_input_sizes(cur, batch: list[list], ncols: int) -> None:
    """Declare each Oracle bind's size from the batch's real per-column lengths.

    Sizes are in CHARACTERS (python-oracledb allocates the byte buffer from the
    connection charset). To stay valid on both STANDARD and EXTENDED
    `max_string_size`, a column whose longest value exceeds 1000 characters (which
    could need >4000 bytes) binds as CLOB instead of VARCHAR; a fixed VARCHAR2(4000)
    would over-allocate (4000 chars x 4 bytes) and fail on STANDARD. An all-empty
    column gets size 1 (a valid buffer that still binds NULL correctly).
    """
    import oracledb

    sizes: list = []
    for col in range(ncols):
        longest = 0
        for row in batch:
            value = row[col]
            if value is None:
                continue
            length = len(value) if isinstance(value, str) else len(str(value))
            if length > longest:
                longest = length
        if longest > 1000:
            sizes.append(oracledb.DB_TYPE_CLOB)
        else:
            sizes.append(max(1, longest))
    cur.setinputsizes(*sizes)


def _row_values(raw: list[str], ncols: int, null_value: str | None, rownum: int) -> list:
    """Turn one CSV record into a bound-parameter row, applying the NULL marker."""
    if len(raw) != ncols:
        raise ValidationError(
            "row_length_mismatch",
            f"Row {rownum} has {len(raw)} value(s) but {ncols} column(s) were expected.",
        )
    if null_value is None:
        return list(raw)
    return [None if value == null_value else value for value in raw]


def _load_and_insert(
    adapter: DatabaseAdapter,
    table: str,
    schema: str | None,
    path: Path,
    columns: list | None,
    has_header: bool,
    delimiter: str,
    encoding: str,
    null_value: str | None,
    batch_size: int,
) -> tuple[list[str], int]:
    """Stream a CSV into the target table via batched executemany; commit once.

    Column names come from `columns` (explicit) or the header row. Rows are bound
    parameters (never inlined) and sent in `batch_size` chunks so a large file
    streams with bounded memory. The plugin owns the mutation and commits Oracle
    explicitly.
    """
    chunk = max(1, int(batch_size))
    delim = (delimiter or ",")
    try:
        with path.open("r", encoding=encoding, newline="") as handle:
            reader = csv.reader(handle, delimiter=delim)
            try:
                first = next(reader)
            except StopIteration:
                raise ValidationError("empty_csv", "CSV file has no rows.")

            if columns:
                cols = [str(c).strip() for c in columns]
                pending_first = None if has_header else first
            else:
                if not has_header:
                    raise ValidationError(
                        "missing_columns",
                        "Provide `columns`, or set has_header=true so the header row names them.",
                    )
                cols = [c.strip() for c in first]
                pending_first = None

            if not cols:
                raise ValidationError("missing_columns", "No columns resolved for import.")
            for col in cols:
                _require_identifier(col, "column")

            sql = _build_insert_sql(adapter.dialect, schema, table, cols)
            ncols = len(cols)

            with adapter.open_connection() as conn:
                with conn.cursor() as cur:
                    total = 0
                    batch: list[list] = []
                    rownum = 1  # physical CSV line of the last-read record

                    if pending_first is not None:
                        # No header: the first line is data (physical line 1).
                        batch.append(_row_values(pending_first, ncols, null_value, rownum))

                    for record in reader:
                        rownum += 1
                        batch.append(_row_values(record, ncols, null_value, rownum))
                        if len(batch) >= chunk:
                            _executemany_batch(cur, sql, batch, adapter.dialect, ncols)
                            total += len(batch)
                            batch = []

                    if batch:
                        _executemany_batch(cur, sql, batch, adapter.dialect, ncols)
                        total += len(batch)
                conn.commit()
            return cols, total
    except (DatabaseError, ValidationError):
        raise
    except Exception as exc:
        raise DatabaseError("database_error", "CSV import failed.",
                            details=str(exc)) from exc


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
    """Accept a DML verb, an anonymous PL/SQL block, or a data-modifying WITH."""
    keyword = _first_keyword(sql)
    if not keyword:
        raise ValidationError("invalid_statement", "SQL statement is empty.")
    if keyword in _DML_KEYWORDS:
        return
    if keyword in _PLSQL_BLOCK_KEYWORDS:
        # Anonymous PL/SQL block (Oracle): typically used with OUT binds.
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


def _execute(
    adapter: DatabaseAdapter,
    sql: str,
    params: list | dict | None,
    out_params: dict | None = None,
) -> tuple[int, dict | None]:
    """Open a writable connection via the adapter, run one statement, commit.

    Returns (rows_affected, out_values) where out_values is None unless OUT binds
    were requested. Dialect-agnostic for the plain path: the adapter's
    `open_connection()` is the same connection the read-only path uses, so this
    covers exactly the core DBs the server supports. OUT-parameter binding is
    Oracle-only. The plugin owns the mutation — it commits explicitly (mandatory
    for Oracle, which otherwise rolls back on close).
    """
    # Resolve OUT type specs before opening the connection so a bad spec surfaces
    # as a ValidationError (not wrapped as a generic database_error below).
    out_types = _resolve_oracle_out_types(adapter, out_params) if out_params else None
    try:
        with adapter.open_connection() as conn:
            with conn.cursor() as cur:
                out_values: dict | None = None
                if out_types is not None:
                    # Named binds only: create an OUT var per name, merge with the
                    # named IN binds, run once, then read the OUT values back.
                    out_vars = {
                        name: (cur.var(typ, size) if size else cur.var(typ))
                        for name, (typ, size) in out_types.items()
                    }
                    bind = _named_in_binds(params)
                    bind.update(out_vars)
                    cur.execute(sql, bind)
                    out_values = {
                        name: normalize_value(var.getvalue())
                        for name, var in out_vars.items()
                    }
                elif params:
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
            return affected, out_values
    except (DatabaseError, ValidationError):
        raise
    except Exception as exc:
        raise DatabaseError("database_error", "Write failed.",
                            details=str(exc)) from exc


def _named_in_binds(params: list | dict | None) -> dict:
    """Return IN binds as a name->value dict; reject positional binds with OUT.

    Oracle cannot mix a positional list with the named OUT variables in a single
    execute, so when OUT params are used the IN binds must be named (or absent).
    """
    if params is None:
        return {}
    if isinstance(params, dict):
        return dict(params)
    raise ValidationError(
        "invalid_params_with_out",
        "When out_params is used, `params` must be an object of named binds "
        '(e.g. {"x": 1}), not a positional list — named and positional binds '
        "cannot be mixed in one statement.",
    )


def _resolve_oracle_out_types(
    adapter: DatabaseAdapter, out_params: dict | None
) -> dict[str, tuple[object, int]]:
    """Map each requested OUT bind to an (oracledb type, string size) pair.

    OUT binding relies on driver-created bind variables, which only the Oracle
    driver exposes here, so it is rejected for other dialects. Raises
    ValidationError for a non-Oracle connection or an unknown type keyword.
    """
    if adapter.dialect != "oracle":
        raise ValidationError(
            "unsupported_out_params",
            f"OUT parameter binding is only supported on Oracle "
            f"(connection dialect is '{adapter.dialect}').",
        )
    if not isinstance(out_params, dict) or not out_params:
        raise ValidationError(
            "invalid_out_params",
            'out_params must be a non-empty object mapping bind name -> type, '
            'e.g. {"new_id": "number"}.',
        )

    import oracledb

    # A string OUT var needs an explicit buffer size; others size themselves.
    string_size = 32767
    type_map: dict[str, tuple[object, int]] = {
        "number": (oracledb.DB_TYPE_NUMBER, 0),
        "int": (oracledb.DB_TYPE_NUMBER, 0),
        "integer": (oracledb.DB_TYPE_NUMBER, 0),
        "float": (oracledb.DB_TYPE_NUMBER, 0),
        "decimal": (oracledb.DB_TYPE_NUMBER, 0),
        "string": (oracledb.DB_TYPE_VARCHAR, string_size),
        "str": (oracledb.DB_TYPE_VARCHAR, string_size),
        "varchar": (oracledb.DB_TYPE_VARCHAR, string_size),
        "varchar2": (oracledb.DB_TYPE_VARCHAR, string_size),
        "clob": (oracledb.DB_TYPE_CLOB, 0),
        "date": (oracledb.DB_TYPE_DATE, 0),
        "datetime": (oracledb.DB_TYPE_DATE, 0),
        "timestamp": (oracledb.DB_TYPE_TIMESTAMP, 0),
    }
    resolved: dict[str, tuple[object, int]] = {}
    for name, type_key in out_params.items():
        key = str(type_key).strip().lower()
        if key not in type_map:
            raise ValidationError(
                "invalid_out_param_type",
                f"Unsupported OUT type '{type_key}' for '{name}'. Supported: "
                f"{', '.join(sorted(type_map))}.",
            )
        resolved[str(name)] = type_map[key]
    return resolved


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
