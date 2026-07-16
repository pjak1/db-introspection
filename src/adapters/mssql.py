from __future__ import annotations

import re
from typing import Any

from src.adapters._sql_helpers import (
    ORDER_BY_RE,
    degraded_or_raise,
    int_or_none,
    rows_from_cursor,
)
from src.adapters.base import AdapterResult, DatabaseAdapter
from src.adapters.normalization import normalize_rows
from src.errors import DatabaseError, ValidationError


class MssqlAdapter(DatabaseAdapter):
    """Microsoft SQL Server implementation of the generic adapter contract."""
    dialect_name = "mssql"
    ddl_object_types = ("table", "view", "procedure", "function")

    def __init__(self, dsn: str):
        """Initialize adapter with a ready-to-use ODBC connection string."""
        self._dsn = dsn

    @property
    def dialect(self) -> str:
        """Return adapter dialect identifier."""
        return "mssql"

    @classmethod
    def build_dsn(cls, conn_values: dict[str, str]) -> str:
        """Build a SQL Server ODBC DSN from connection-file values."""
        required = ("host", "db_name", "username", "password")
        if any(key not in conn_values for key in required):
            return ""
        host = conn_values["host"]
        port = conn_values.get("port", "1433")
        database = conn_values["db_name"]
        username = conn_values["username"]
        password = conn_values["password"]
        driver = conn_values.get("driver", "ODBC Driver 18 for SQL Server")
        return (
            f"DRIVER={{{driver}}};"
            f"SERVER={host},{port};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
            "Encrypt=no;"
            "TrustServerCertificate=yes;"
        )

    @classmethod
    def default_schema(cls, conn_values: dict[str, str]) -> str:
        """Return SQL Server default schema fallback."""
        return conn_values.get("schema", "dbo")

    @classmethod
    def wrap_select(cls, query: str, limit: int) -> str:
        """Wrap a query to enforce row limit in SQL Server syntax."""
        max_rows = int(limit)
        sql = query.strip().rstrip(";")
        tail = cls._depth_zero_tail_positions(sql)

        # The generic derived-table wrapper (SELECT TOP (n) * FROM (...)) is invalid
        # when the statement is a CTE (WITH cannot be nested in a derived table) or
        # when the OUTER query already has ORDER BY/OFFSET (ORDER BY is illegal inside
        # a derived table without its own TOP/OFFSET). In those cases limit the outer
        # query in place via OFFSET-FETCH, which also correctly bounds UNION results.
        # ORDER BY/OFFSET are detected only at parenthesis depth 0, so a clause inside
        # a subquery never forces this path.
        if cls._starts_with_cte(sql) or tail["order_by"] is not None or tail["offset"] is not None:
            return cls._limit_outer_query(sql, max_rows, tail)

        return f"SELECT TOP ({max_rows}) * FROM ({sql}) mcp_subquery"

    @staticmethod
    def _starts_with_cte(sql: str) -> bool:
        """Return True when the statement begins with a WITH (CTE) clause."""
        return re.match(r"\s*with\b", sql, re.IGNORECASE) is not None

    @staticmethod
    def _skip_noncode_span(sql: str, i: int) -> int | None:
        """If a literal/identifier/comment starts at `i`, return the index past it.

        Returns None when the character at `i` is ordinary SQL code.
        """
        n = len(sql)
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < n else ""
        if ch in "'\"":
            i += 1
            while i < n:
                if sql[i] == ch and i + 1 < n and sql[i + 1] == ch:
                    i += 2
                    continue
                if sql[i] == ch:
                    return i + 1
                i += 1
            return i
        if ch == "[":
            i += 1
            while i < n and sql[i] != "]":
                i += 1
            return i + 1
        if ch == "-" and nxt == "-":
            i += 2
            while i < n and sql[i] != "\n":
                i += 1
            return i
        if ch == "/" and nxt == "*":
            i += 2
            while i < n - 1 and not (sql[i] == "*" and sql[i + 1] == "/"):
                i += 1
            return i + 2
        return None

    @staticmethod
    def _read_word(sql: str, i: int) -> tuple[str, int]:
        """Read an identifier/keyword word at `i`, returning (lowercased, end)."""
        n = len(sql)
        j = i
        while j < n and (sql[j].isalnum() or sql[j] in "_$"):
            j += 1
        return sql[i:j].lower(), j

    @classmethod
    def _depth_zero_tail_positions(cls, sql: str) -> dict[str, int | None]:
        """Locate the outer-query ORDER BY / OFFSET at parenthesis depth 0.

        Literals, quoted/bracketed identifiers and comments are skipped so that
        an ORDER BY/OFFSET inside a CTE body or subquery is not mistaken for the
        outer query's clause.
        """
        i, n, depth = 0, len(sql), 0
        order_by_pos: int | None = None
        offset_pos: int | None = None
        while i < n:
            skipped = cls._skip_noncode_span(sql, i)
            if skipped is not None:
                i = skipped
                continue
            ch = sql[i]
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            elif depth == 0 and (ch.isalpha() or ch == "_"):
                word, end = cls._read_word(sql, i)
                if word == "offset":
                    offset_pos = i
                elif word == "order":
                    k = end
                    while k < n and sql[k].isspace():
                        k += 1
                    next_word, _ = cls._read_word(sql, k)
                    if next_word == "by":
                        order_by_pos = i
                i = end
                continue
            i += 1
        return {"order_by": order_by_pos, "offset": offset_pos}

    @classmethod
    def _limit_outer_query(
        cls,
        sql: str,
        max_rows: int,
        tail: dict[str, int | None] | None = None,
    ) -> str:
        """Apply an OFFSET-FETCH row limit to the outer query in place.

        Used for CTEs and for any query whose outer block already has ORDER BY or
        OFFSET, where the generic derived-table wrapper would produce invalid T-SQL.
        """
        if tail is None:
            tail = cls._depth_zero_tail_positions(sql)
        fetch = f"OFFSET 0 ROWS FETCH NEXT {max_rows} ROWS ONLY"
        if tail["offset"] is not None:
            # Override the outer query's existing OFFSET/FETCH window.
            head = sql[: tail["offset"]].rstrip()
            return f"{head} {fetch}"
        if tail["order_by"] is not None:
            # OFFSET-FETCH is valid because the outer query already orders rows.
            return f"{sql} {fetch}"
        # OFFSET-FETCH requires ORDER BY; add a no-op ordering for an unordered query.
        return f"{sql} ORDER BY (SELECT NULL) {fetch}"

    def open_connection(self) -> Any:
        """Create and return an ODBC connection, translating driver errors."""
        try:
            import pyodbc  # type: ignore
        except Exception as exc:
            raise DatabaseError(
                "missing_dependency",
                "MSSQL adapter requires the 'pyodbc' package.",
                details=str(exc),
            ) from exc
        try:
            return pyodbc.connect(self._dsn, autocommit=False)
        except Exception as exc:
            raise DatabaseError(
                "database_error", "MSSQL connection failed.", details=str(exc)) from exc

    @staticmethod
    def _q(identifier: str) -> str:
        """Safely quote SQL Server identifiers using brackets."""
        return f"[{identifier.replace(']', ']]')}]"

    def _with_full_data_type(self, rows: list[dict]) -> list[dict]:
        """Attach formatted SQL Server type names and strip helper metadata."""
        formatted_rows: list[dict] = []
        for row in rows:
            data_type = str(row.get("data_type") or "")
            normalized_type = data_type.lower()
            char_length = int_or_none(row.get("_character_maximum_length"))
            numeric_precision = int_or_none(row.get("_numeric_precision"))
            numeric_scale = int_or_none(row.get("_numeric_scale"))
            datetime_precision = int_or_none(row.get("_datetime_precision"))

            full_data_type = data_type
            if normalized_type in {"char", "varchar", "binary", "varbinary", "nchar", "nvarchar"}:
                if char_length is not None:
                    size = "max" if char_length == -1 else str(char_length)
                    full_data_type = f"{data_type}({size})"
            elif normalized_type in {"decimal", "numeric"}:
                if numeric_precision is not None:
                    scale = 0 if numeric_scale is None else numeric_scale
                    full_data_type = f"{data_type}({numeric_precision},{scale})"
            elif normalized_type in {"datetime2", "datetimeoffset", "time"}:
                if datetime_precision is not None:
                    full_data_type = f"{data_type}({datetime_precision})"

            public_row = {
                key: value for key, value in row.items() if not key.startswith("_")
            }
            public_row["full_data_type"] = full_data_type
            formatted_rows.append(public_row)
        return formatted_rows

    @staticmethod
    def _normalize_explain_rows(columns: list[str], fetched_rows: list[tuple[Any, ...]]) -> list[dict]:
        """Map SQL Server SHOWPLAN rows to the common public plan row shape."""
        stmt_index = 0
        for idx, column in enumerate(columns):
            if str(column).lower() == "stmttext":
                stmt_index = idx
                break
        return normalize_rows(
            [{"plan_text": row[stmt_index]} for row in fetched_rows]
        )

    def _fetch_all(
        self,
        query: str,
        params: tuple[Any, ...] | None = None,
        timeout_ms: int | None = None,
    ) -> list[dict]:
        """Execute SQL and return normalized rows as dictionaries.

        Read path only. SQL Server has no engine-level read-only transaction mode,
        so read-only is enforced by never committing: the transaction is always
        rolled back, discarding any side effect a read might have triggered (a
        plain `with conn` would otherwise commit on exit).
        """
        conn = self.open_connection()
        try:
            if timeout_ms is not None:
                conn.timeout = max(1, int(timeout_ms) // 1000)
            with conn.cursor() as cur:
                cur.execute(query, params or ())
                rows = rows_from_cursor(cur)
            conn.rollback()
            return rows
        except DatabaseError:
            raise
        except Exception as exc:
            raise DatabaseError(
                "database_error", "MSSQL query failed.", details=str(exc)) from exc
        finally:
            conn.close()

    @staticmethod
    def _in_clause(values: tuple[str, ...]) -> tuple[str, tuple[Any, ...]]:
        """Build positional placeholders and tuple parameters for IN filters."""
        placeholders = ", ".join("?" for _ in values)
        return placeholders, tuple(values)

    def list_tables(self, schemas: tuple[str, ...], include_system: bool) -> AdapterResult:
        """List tables and views available in selected schemas."""
        in_clause, params = self._in_clause(schemas)
        query = f"""
            SELECT
                t.TABLE_SCHEMA AS [schema],
                t.TABLE_NAME AS table_name,
                t.TABLE_TYPE AS table_type,
                CAST((
                    SELECT ep.value
                    FROM sys.extended_properties ep
                    WHERE ep.major_id = OBJECT_ID(
                            QUOTENAME(t.TABLE_SCHEMA) + '.' + QUOTENAME(t.TABLE_NAME))
                      AND ep.minor_id = 0
                      AND ep.name = 'MS_Description'
                ) AS nvarchar(max)) AS table_comment
            FROM INFORMATION_SCHEMA.TABLES t
            WHERE t.TABLE_SCHEMA IN ({in_clause})
              AND (
                    ? = 1
                    OR t.TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA')
              )
            ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME
        """
        data = self._fetch_all(query, params + (1 if include_system else 0,))
        return AdapterResult(data=data)

    def list_columns(self, table: str, schemas: tuple[str, ...]) -> AdapterResult:
        """List columns for a table in the selected schema scope."""
        in_clause, params = self._in_clause(schemas)
        query = f"""
            SELECT
                c.TABLE_SCHEMA AS [schema],
                c.TABLE_NAME AS table_name,
                c.COLUMN_NAME AS column_name,
                c.ORDINAL_POSITION AS ordinal_position,
                c.DATA_TYPE AS data_type,
                c.DATA_TYPE AS udt_name,
                CASE c.IS_NULLABLE WHEN 'YES' THEN 1 ELSE 0 END AS is_nullable,
                c.COLUMN_DEFAULT AS column_default,
                CAST((
                    SELECT ep.value
                    FROM sys.extended_properties ep
                    WHERE ep.major_id = OBJECT_ID(
                            QUOTENAME(c.TABLE_SCHEMA) + '.' + QUOTENAME(c.TABLE_NAME))
                      AND ep.minor_id = COLUMNPROPERTY(
                            OBJECT_ID(QUOTENAME(c.TABLE_SCHEMA) + '.' + QUOTENAME(c.TABLE_NAME)),
                            c.COLUMN_NAME, 'ColumnId')
                      AND ep.name = 'MS_Description'
                ) AS nvarchar(max)) AS comment,
                c.CHARACTER_MAXIMUM_LENGTH AS _character_maximum_length,
                c.NUMERIC_PRECISION AS _numeric_precision,
                c.NUMERIC_SCALE AS _numeric_scale,
                c.DATETIME_PRECISION AS _datetime_precision
            FROM INFORMATION_SCHEMA.COLUMNS c
            WHERE c.TABLE_NAME = ?
              AND c.TABLE_SCHEMA IN ({in_clause})
            ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION
        """
        data = self._with_full_data_type(self._fetch_all(query, (table,) + params))
        return AdapterResult(data=data)

    def list_constraints(
        self,
        schemas: tuple[str, ...],
        table: str | None = None,
        constraint_type: str | None = None,
    ) -> AdapterResult:
        """List table constraints with optional filters."""
        in_clause, params = self._in_clause(schemas)
        query = f"""
            SELECT
                tc.CONSTRAINT_SCHEMA AS [schema],
                tc.TABLE_NAME AS table_name,
                tc.CONSTRAINT_NAME AS constraint_name,
                tc.CONSTRAINT_TYPE AS constraint_type,
                STRING_AGG(kcu.COLUMN_NAME, ', ') AS columns,
                ccu.TABLE_SCHEMA AS foreign_table_schema,
                ccu.TABLE_NAME AS foreign_table_name,
                STRING_AGG(ccu.COLUMN_NAME, ', ') AS foreign_columns,
                chk.CHECK_CLAUSE AS check_clause
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
              ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
             AND tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
             AND tc.TABLE_NAME = kcu.TABLE_NAME
            LEFT JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu
              ON tc.CONSTRAINT_NAME = ccu.CONSTRAINT_NAME
             AND tc.CONSTRAINT_SCHEMA = ccu.CONSTRAINT_SCHEMA
            LEFT JOIN INFORMATION_SCHEMA.CHECK_CONSTRAINTS chk
              ON tc.CONSTRAINT_NAME = chk.CONSTRAINT_NAME
             AND tc.CONSTRAINT_SCHEMA = chk.CONSTRAINT_SCHEMA
            WHERE tc.CONSTRAINT_SCHEMA IN ({in_clause})
              AND (? IS NULL OR tc.TABLE_NAME = ?)
              AND (? IS NULL OR tc.CONSTRAINT_TYPE = ?)
            GROUP BY
                tc.CONSTRAINT_SCHEMA,
                tc.TABLE_NAME,
                tc.CONSTRAINT_NAME,
                tc.CONSTRAINT_TYPE,
                ccu.TABLE_SCHEMA,
                ccu.TABLE_NAME,
                chk.CHECK_CLAUSE
            ORDER BY tc.CONSTRAINT_SCHEMA, tc.TABLE_NAME, tc.CONSTRAINT_NAME
        """
        normalized_type = constraint_type.upper() if constraint_type else None
        bind = params + (table, table, normalized_type, normalized_type)
        data = self._fetch_all(query, bind)
        return AdapterResult(data=data)

    def list_sequences(self, schemas: tuple[str, ...]) -> AdapterResult:
        """List sequences for selected schemas."""
        in_clause, params = self._in_clause(schemas)
        query = f"""
            SELECT
                SCHEMA_NAME(s.schema_id) AS [schema],
                s.name AS sequence_name,
                CONVERT(nvarchar(128), s.start_value) AS start_value,
                CONVERT(nvarchar(128), s.minimum_value) AS min_value,
                CONVERT(nvarchar(128), s.maximum_value) AS max_value,
                CONVERT(nvarchar(128), s.increment) AS increment_by,
                s.is_cycling AS cycle,
                s.cache_size,
                CONVERT(nvarchar(128), s.current_value) AS last_value
            FROM sys.sequences s
            WHERE SCHEMA_NAME(s.schema_id) IN ({in_clause})
            ORDER BY [schema], sequence_name
        """
        data = self._fetch_all(query, params)
        return AdapterResult(data=data)

    def list_procedures(self, schemas: tuple[str, ...]) -> AdapterResult:
        """List stored procedures for selected schemas."""
        in_clause, params = self._in_clause(schemas)
        query = f"""
            SELECT
                SCHEMA_NAME(p.schema_id) AS [schema],
                p.name AS procedure_name,
                NULL AS arguments,
                'T-SQL' AS language,
                NULL AS volatility
            FROM sys.procedures p
            WHERE SCHEMA_NAME(p.schema_id) IN ({in_clause})
            ORDER BY [schema], procedure_name
        """
        data = self._fetch_all(query, params)
        return AdapterResult(data=data)

    def list_functions(self, schemas: tuple[str, ...]) -> AdapterResult:
        """List functions for selected schemas."""
        in_clause, params = self._in_clause(schemas)
        query = f"""
            SELECT
                SCHEMA_NAME(o.schema_id) AS [schema],
                o.name AS function_name,
                NULL AS arguments,
                NULL AS return_type,
                'T-SQL' AS language,
                NULL AS volatility
            FROM sys.objects o
            WHERE o.type IN ('FN', 'IF', 'TF', 'FS', 'FT')
              AND SCHEMA_NAME(o.schema_id) IN ({in_clause})
            ORDER BY [schema], function_name
        """
        data = self._fetch_all(query, params)
        return AdapterResult(data=data)

    def list_jobs(self) -> AdapterResult:
        """List SQL Server Agent jobs when the catalog is accessible."""
        try:
            data = self._fetch_all(
                """
                SELECT job_id, name AS job_name, enabled, date_created, date_modified
                FROM msdb.dbo.sysjobs
                ORDER BY name
                """
            )
            return AdapterResult(data=data, status="available")
        except DatabaseError as exc:
            details = str(exc.details or "").lower()
            has_sysjobs_target = "msdb.dbo.sysjobs" in details or "sysjobs" in details
            # SQLSTATE for object not found.
            missing_catalog = "42s02" in details
            permission_denied = "(229)" in details or "permission" in details or "denied" in details
            return degraded_or_raise(
                exc,
                matched=has_sysjobs_target and (missing_catalog or permission_denied),
                warning="SQL Server scheduler catalog (Agent jobs) is not available for this user.",
            )

    def list_indexes(self, schemas: tuple[str, ...], table: str | None = None) -> AdapterResult:
        """List indexes for selected schemas, optionally filtered by table."""
        in_clause, params = self._in_clause(schemas)
        query = f"""
            SELECT
                SCHEMA_NAME(t.schema_id) AS [schema],
                t.name AS table_name,
                ix.name AS index_name,
                ix.is_unique,
                ix.is_primary_key AS is_primary,
                ix.type_desc AS index_type,
                STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal) AS columns
            FROM sys.indexes ix
            JOIN sys.tables t ON t.object_id = ix.object_id
            JOIN sys.index_columns ic
              ON ic.object_id = ix.object_id AND ic.index_id = ix.index_id
            JOIN sys.columns c
              ON c.object_id = ic.object_id AND c.column_id = ic.column_id
            WHERE ix.type > 0
              AND SCHEMA_NAME(t.schema_id) IN ({in_clause})
              AND (? IS NULL OR t.name = ?)
            GROUP BY SCHEMA_NAME(t.schema_id), t.name, ix.name,
                     ix.is_unique, ix.is_primary_key, ix.type_desc
            ORDER BY [schema], table_name, index_name
        """
        normalized_table = table.strip() if isinstance(table, str) else None
        bind = params + (normalized_table, normalized_table)
        data = self._fetch_all(query, bind)
        return AdapterResult(data=data, status="available")

    @staticmethod
    def _format_column_type(row: dict) -> str:
        """Format a SQL Server column type with length/precision/scale."""
        data_type = str(row.get("data_type") or "")
        normalized = data_type.lower()
        max_length = int_or_none(row.get("_max_length"))
        precision = int_or_none(row.get("_precision"))
        scale = int_or_none(row.get("_scale"))
        if normalized in {"varchar", "char", "varbinary", "binary"} and max_length is not None:
            size = "max" if max_length == -1 else str(max_length)
            return f"{data_type}({size})"
        if normalized in {"nvarchar", "nchar"} and max_length is not None:
            size = "max" if max_length == -1 else str(max_length // 2)
            return f"{data_type}({size})"
        if normalized in {"decimal", "numeric"} and precision is not None:
            return f"{data_type}({precision},{scale if scale is not None else 0})"
        if normalized in {"datetime2", "datetimeoffset", "time"} and scale is not None:
            return f"{data_type}({scale})"
        return data_type

    def _ddl_column_lines(self, schema: str, table: str) -> list[str]:
        """Build the column definition lines for a reconstructed CREATE TABLE."""
        rows = self._fetch_all(
            """
            SELECT
                c.name AS name,
                t.name AS data_type,
                c.max_length AS _max_length,
                c.precision AS _precision,
                c.scale AS _scale,
                c.is_nullable AS is_nullable,
                c.is_identity AS is_identity,
                CONVERT(nvarchar(64), ic.seed_value) AS seed_value,
                CONVERT(nvarchar(64), ic.increment_value) AS increment_value,
                dc.definition AS default_def,
                cc.definition AS computed_def
            FROM sys.columns c
            JOIN sys.types t ON t.user_type_id = c.user_type_id
            LEFT JOIN sys.identity_columns ic
              ON ic.object_id = c.object_id AND ic.column_id = c.column_id
            LEFT JOIN sys.default_constraints dc ON dc.object_id = c.default_object_id
            LEFT JOIN sys.computed_columns cc
              ON cc.object_id = c.object_id AND cc.column_id = c.column_id
            WHERE c.object_id = OBJECT_ID(QUOTENAME(?) + '.' + QUOTENAME(?))
            ORDER BY c.column_id
            """,
            (schema, table),
        )
        lines: list[str] = []
        for row in rows:
            name_q = self._q(str(row["name"]))
            if row.get("computed_def"):
                lines.append(f"{name_q} AS {row['computed_def']}")
                continue
            line = f"{name_q} {self._format_column_type(row)}"
            if row.get("is_identity"):
                seed = row.get("seed_value") or "1"
                increment = row.get("increment_value") or "1"
                line += f" IDENTITY({seed},{increment})"
            line += " NULL" if row.get("is_nullable") else " NOT NULL"
            if row.get("default_def"):
                line += f" DEFAULT {row['default_def']}"
            lines.append(line)
        return lines

    def _ddl_constraint_lines(self, schema: str, table: str) -> list[str]:
        """Build PRIMARY KEY / UNIQUE / FOREIGN KEY / CHECK constraint lines."""
        keys = self._fetch_all(
            """
            SELECT kc.name AS name, kc.type AS ctype,
                (SELECT STRING_AGG(QUOTENAME(col.name), ', ')
                        WITHIN GROUP (ORDER BY ic.key_ordinal)
                 FROM sys.index_columns ic
                 JOIN sys.columns col
                   ON col.object_id = ic.object_id AND col.column_id = ic.column_id
                 WHERE ic.object_id = kc.parent_object_id
                   AND ic.index_id = kc.unique_index_id) AS cols
            FROM sys.key_constraints kc
            WHERE kc.parent_object_id = OBJECT_ID(QUOTENAME(?) + '.' + QUOTENAME(?))
            ORDER BY kc.type DESC, kc.name
            """,
            (schema, table),
        )
        lines = [
            f"CONSTRAINT {self._q(str(row['name']))} "
            f"{'PRIMARY KEY' if row.get('ctype', '').strip() == 'PK' else 'UNIQUE'} "
            f"({row['cols']})"
            for row in keys
        ]

        fks = self._fetch_all(
            """
            SELECT fk.name AS name,
                (SELECT STRING_AGG(QUOTENAME(pc.name), ', ')
                        WITHIN GROUP (ORDER BY fkc.constraint_column_id)
                 FROM sys.foreign_key_columns fkc
                 JOIN sys.columns pc
                   ON pc.object_id = fkc.parent_object_id AND pc.column_id = fkc.parent_column_id
                 WHERE fkc.constraint_object_id = fk.object_id) AS parent_cols,
                QUOTENAME(SCHEMA_NAME(rt.schema_id)) + '.' + QUOTENAME(rt.name) AS ref_table,
                (SELECT STRING_AGG(QUOTENAME(rc.name), ', ')
                        WITHIN GROUP (ORDER BY fkc.constraint_column_id)
                 FROM sys.foreign_key_columns fkc
                 JOIN sys.columns rc
                   ON rc.object_id = fkc.referenced_object_id
                  AND rc.column_id = fkc.referenced_column_id
                 WHERE fkc.constraint_object_id = fk.object_id) AS ref_cols,
                fk.delete_referential_action_desc AS on_delete,
                fk.update_referential_action_desc AS on_update
            FROM sys.foreign_keys fk
            JOIN sys.tables rt ON rt.object_id = fk.referenced_object_id
            WHERE fk.parent_object_id = OBJECT_ID(QUOTENAME(?) + '.' + QUOTENAME(?))
            ORDER BY fk.name
            """,
            (schema, table),
        )
        for row in fks:
            line = (
                f"CONSTRAINT {self._q(str(row['name']))} FOREIGN KEY ({row['parent_cols']}) "
                f"REFERENCES {row['ref_table']} ({row['ref_cols']})"
            )
            on_delete = str(row.get("on_delete") or "NO_ACTION")
            on_update = str(row.get("on_update") or "NO_ACTION")
            if on_delete != "NO_ACTION":
                line += f" ON DELETE {on_delete.replace('_', ' ')}"
            if on_update != "NO_ACTION":
                line += f" ON UPDATE {on_update.replace('_', ' ')}"
            lines.append(line)

        checks = self._fetch_all(
            """
            SELECT name, definition
            FROM sys.check_constraints
            WHERE parent_object_id = OBJECT_ID(QUOTENAME(?) + '.' + QUOTENAME(?))
            ORDER BY name
            """,
            (schema, table),
        )
        lines.extend(
            f"CONSTRAINT {self._q(str(row['name']))} CHECK {row['definition']}"
            for row in checks
        )
        return lines

    def _ddl_index_statements(self, schema: str, table: str) -> list[str]:
        """Build CREATE INDEX statements for secondary (non-constraint) indexes."""
        rows = self._fetch_all(
            """
            SELECT ix.name AS name, ix.is_unique AS is_unique, ix.type_desc AS type_desc,
                (SELECT STRING_AGG(QUOTENAME(col.name), ', ')
                        WITHIN GROUP (ORDER BY ic.key_ordinal)
                 FROM sys.index_columns ic
                 JOIN sys.columns col
                   ON col.object_id = ic.object_id AND col.column_id = ic.column_id
                 WHERE ic.object_id = ix.object_id AND ic.index_id = ix.index_id
                   AND ic.is_included_column = 0) AS cols,
                (SELECT STRING_AGG(QUOTENAME(col.name), ', ')
                        WITHIN GROUP (ORDER BY ic.index_column_id)
                 FROM sys.index_columns ic
                 JOIN sys.columns col
                   ON col.object_id = ic.object_id AND col.column_id = ic.column_id
                 WHERE ic.object_id = ix.object_id AND ic.index_id = ix.index_id
                   AND ic.is_included_column = 1) AS included
            FROM sys.indexes ix
            WHERE ix.object_id = OBJECT_ID(QUOTENAME(?) + '.' + QUOTENAME(?))
              AND ix.is_primary_key = 0 AND ix.is_unique_constraint = 0
              AND ix.type > 0 AND ix.name IS NOT NULL
            ORDER BY ix.name
            """,
            (schema, table),
        )
        table_ref = f"{self._q(schema)}.{self._q(table)}"
        statements: list[str] = []
        for row in rows:
            unique = "UNIQUE " if row.get("is_unique") else ""
            kind = str(row.get("type_desc") or "NONCLUSTERED")
            stmt = (
                f"CREATE {unique}{kind} INDEX {self._q(str(row['name']))} "
                f"ON {table_ref} ({row['cols']})"
            )
            if row.get("included"):
                stmt += f" INCLUDE ({row['included']})"
            statements.append(stmt + ";")
        return statements

    def _table_ddl(self, schema: str, table: str) -> AdapterResult:
        """Reconstruct a CREATE TABLE statement from the SQL Server catalogs.

        SQL Server has no single "get table DDL" primitive (unlike Oracle's
        DBMS_METADATA), so the statement is assembled from sys.columns,
        sys.key_constraints/foreign_keys/check_constraints and sys.indexes. The
        result is a faithful reconstruction, not necessarily byte-identical to the
        original CREATE.
        """
        column_lines = self._ddl_column_lines(schema, table)
        if not column_lines:
            return AdapterResult(
                data=[],
                warnings=[f"No table '{schema}.{table}' found."],
                status="not_found",
            )
        body_lines = column_lines + self._ddl_constraint_lines(schema, table)
        table_ref = f"{self._q(schema)}.{self._q(table)}"
        ddl = f"CREATE TABLE {table_ref} (\n    " + ",\n    ".join(body_lines) + "\n);"
        for stmt in self._ddl_index_statements(schema, table):
            ddl += f"\n{stmt}"
        return AdapterResult(
            data=[{
                "object_type": "table",
                "schema": schema,
                "object_name": table,
                "ddl": ddl,
            }],
            warnings=[
                "Table DDL is reconstructed from the catalog and may differ from "
                "the original CREATE statement."
            ],
            status="available",
        )

    def get_ddl(self, schema: str, object_name: str, object_type: str) -> AdapterResult:
        """Return the DDL of a table, view, procedure or function."""
        normalized_type = object_type.strip().lower()
        if normalized_type == "table":
            return self._table_ddl(schema, object_name)
        if normalized_type not in {"view", "procedure", "function"}:
            return AdapterResult(
                data=[],
                warnings=[
                    "SQL Server DDL retrieval supports object_type 'table', 'view', "
                    "'procedure' or 'function'."
                ],
                status="not_supported",
            )
        query = """
            SELECT
                ? AS object_type,
                ? AS [schema],
                ? AS object_name,
                OBJECT_DEFINITION(OBJECT_ID(QUOTENAME(?) + '.' + QUOTENAME(?))) AS ddl
        """
        data = self._fetch_all(
            query,
            (normalized_type, schema, object_name, schema, object_name),
        )
        if not data or data[0].get("ddl") is None:
            return AdapterResult(
                data=[],
                warnings=[f"No {normalized_type} '{schema}.{object_name}' found."],
                status="not_found",
            )
        return AdapterResult(data=data, status="available")

    def search_objects(
        self,
        schemas: tuple[str, ...],
        pattern: str,
        object_types: tuple[str, ...],
    ) -> AdapterResult:
        """Search objects by case-insensitive name substring across selected schemas."""
        in1, p1 = self._in_clause(schemas)
        in2, p2 = self._in_clause(schemas)
        in3, p3 = self._in_clause(schemas)
        in4, p4 = self._in_clause(schemas)
        types_in = ", ".join("?" for _ in object_types)
        query = f"""
            SELECT o.[schema], o.object_name, o.object_type
            FROM (
                SELECT TABLE_SCHEMA AS [schema], TABLE_NAME AS object_name,
                       CASE WHEN TABLE_TYPE = 'VIEW' THEN 'view' ELSE 'table' END AS object_type
                FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA IN ({in1})
                UNION ALL
                SELECT SCHEMA_NAME(schema_id), name, 'sequence'
                FROM sys.sequences WHERE SCHEMA_NAME(schema_id) IN ({in2})
                UNION ALL
                SELECT SCHEMA_NAME(schema_id), name, 'procedure'
                FROM sys.procedures WHERE SCHEMA_NAME(schema_id) IN ({in3})
                UNION ALL
                SELECT SCHEMA_NAME(schema_id), name, 'function'
                FROM sys.objects
                WHERE type IN ('FN', 'IF', 'TF', 'FS', 'FT')
                  AND SCHEMA_NAME(schema_id) IN ({in4})
            ) o
            WHERE LOWER(o.object_name) LIKE LOWER(?)
              AND o.object_type IN ({types_in})
            ORDER BY o.[schema], o.object_type, o.object_name
        """
        like = f"%{pattern}%"
        bind = p1 + p2 + p3 + p4 + (like,) + tuple(object_types)
        data = self._fetch_all(query, bind)
        return AdapterResult(data=data, status="available")

    def sample_table(
        self,
        schema: str,
        table: str,
        limit: int,
        order_by: str | None,
    ) -> AdapterResult:
        """Return a bounded table preview with optional ORDER BY."""
        schema_q = self._q(schema)
        table_q = self._q(table)
        query = f"SELECT TOP ({int(limit)}) * FROM {schema_q}.{table_q}"
        if order_by:
            match = ORDER_BY_RE.match(order_by)
            if not match:
                raise ValidationError(
                    "invalid_order_by",
                    "order_by must be in format 'column' or 'column ASC|DESC'.",
                )
            col = self._q(match.group(1))
            direction = (match.group(2) or "ASC").upper()
            query += f" ORDER BY {col} {direction}"
        data = self._fetch_all(query)
        return AdapterResult(data=data, schema_used=schema)

    def select_columns(
        self,
        schema: str,
        table: str,
        columns: list[str],
        limit: int,
    ) -> AdapterResult:
        """Return a bounded projection for selected table columns."""
        schema_q = self._q(schema)
        table_q = self._q(table)
        cols = ", ".join(self._q(column) for column in columns)
        query = f"SELECT TOP ({int(limit)}) {cols} FROM {schema_q}.{table_q}"
        data = self._fetch_all(query)
        return AdapterResult(data=data, schema_used=schema)

    def run_select(self, sql_query: str, timeout_ms: int) -> AdapterResult:
        """Run a read-only SQL query with timeout controls when supported."""
        data = self._fetch_all(sql_query, timeout_ms=timeout_ms)
        return AdapterResult(data=data)

    def explain_select(self, sql_query: str, timeout_ms: int) -> AdapterResult:
        """Return a SQL Server estimated execution plan for a validated SELECT."""
        try:
            with self.open_connection() as conn:
                conn.timeout = max(1, int(timeout_ms) // 1000)
                with conn.cursor() as cur:
                    cur.execute("SET SHOWPLAN_TEXT ON")
                    execution_error: Exception | None = None
                    collected_rows: list[dict] = []
                    try:
                        cur.execute(sql_query)
                        while True:
                            if cur.description is not None:
                                columns = [desc[0] for desc in cur.description]
                                collected_rows.extend(
                                    self._normalize_explain_rows(columns, cur.fetchall())
                                )
                            if not cur.nextset():
                                break
                    except Exception as exc:
                        execution_error = exc
                    try:
                        cur.execute("SET SHOWPLAN_TEXT OFF")
                    except Exception as off_exc:
                        if execution_error is None:
                            raise off_exc
                    if execution_error is not None:
                        raise execution_error
                    return AdapterResult(data=collected_rows, status="explain")
        except DatabaseError:
            raise
        except Exception as exc:
            raise DatabaseError(
                "database_error",
                "MSSQL explain plan failed.",
                details=str(exc),
            ) from exc
