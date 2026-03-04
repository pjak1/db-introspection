from __future__ import annotations

import re
from typing import Any

from src.adapters.base import AdapterResult, DatabaseAdapter
from src.adapters.normalization import normalize_rows
from src.errors import DatabaseError, ValidationError

_ORDER_BY_RE = re.compile(
    r"^\s*([A-Za-z_][A-Za-z0-9_$]*)(?:\s+(asc|desc))?\s*$", re.IGNORECASE)


class MssqlAdapter(DatabaseAdapter):
    """Microsoft SQL Server implementation of the generic adapter contract."""
    dialect_name = "mssql"
    dsn_env_var = "MSSQL_DSN"

    def __init__(self, dsn: str):
        """Initialize adapter with a ready-to-use ODBC connection string."""
        self._dsn = dsn

    @property
    def dialect(self) -> str:
        """Return adapter dialect identifier."""
        return "mssql"

    @classmethod
    def build_dsn(cls, conn_values: dict[str, str], env: dict[str, str]) -> str:
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

        # ORDER BY inside a derived table is not valid in MSSQL unless combined with
        # TOP/OFFSET/FOR XML. When ORDER BY is present, keep the original query as-is
        # and append/override OFFSET-FETCH row limiting.
        if re.search(r"\border\s+by\b", sql, re.IGNORECASE):
            if re.search(r"\boffset\s+\d+\s+rows\b", sql, re.IGNORECASE):
                return re.sub(
                    r"\boffset\s+\d+\s+rows(?:\s+fetch\s+next\s+\d+\s+rows\s+only)?",
                    f"OFFSET 0 ROWS FETCH NEXT {max_rows} ROWS ONLY",
                    sql,
                    count=1,
                    flags=re.IGNORECASE,
                )
            return f"{sql} OFFSET 0 ROWS FETCH NEXT {max_rows} ROWS ONLY"

        return f"SELECT TOP ({max_rows}) * FROM ({sql}) mcp_subquery"

    def _connect(self) -> Any:
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

    def _fetch_all(
        self,
        query: str,
        params: tuple[Any, ...] | None = None,
        timeout_ms: int | None = None,
    ) -> list[dict]:
        """Execute SQL and return normalized rows as dictionaries."""
        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    if timeout_ms is not None:
                        conn.timeout = max(1, int(timeout_ms) // 1000)
                    cur.execute(query, params or ())
                    if cur.description is None:
                        return []
                    columns = [desc[0].lower() for desc in cur.description]
                    rows = [dict(zip(columns, row)) for row in cur.fetchall()]
                    return normalize_rows(rows)
        except DatabaseError:
            raise
        except Exception as exc:
            raise DatabaseError(
                "database_error", "MSSQL query failed.", details=str(exc)) from exc

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
                TABLE_SCHEMA AS [schema],
                TABLE_NAME AS table_name,
                TABLE_TYPE AS table_type
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA IN ({in_clause})
              AND (
                    ? = 1
                    OR TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA')
              )
            ORDER BY TABLE_SCHEMA, TABLE_NAME
        """
        data = self._fetch_all(query, params + (1 if include_system else 0,))
        return AdapterResult(data=data)

    def list_columns(self, table: str, schemas: tuple[str, ...]) -> AdapterResult:
        """List columns for a table in the selected schema scope."""
        in_clause, params = self._in_clause(schemas)
        query = f"""
            SELECT
                TABLE_SCHEMA AS [schema],
                TABLE_NAME AS table_name,
                COLUMN_NAME AS column_name,
                ORDINAL_POSITION AS ordinal_position,
                DATA_TYPE AS data_type,
                DATA_TYPE AS udt_name,
                CASE IS_NULLABLE WHEN 'YES' THEN 1 ELSE 0 END AS is_nullable,
                COLUMN_DEFAULT AS column_default
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = ?
              AND TABLE_SCHEMA IN ({in_clause})
            ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
        """
        data = self._fetch_all(query, (table,) + params)
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
            if (
                has_sysjobs_target
                and (missing_catalog or permission_denied)
            ):
                return AdapterResult(
                    data=[],
                    warnings=[
                        "SQL Server scheduler catalog (Agent jobs) is not available for this user."],
                    status="not_available",
                )
            raise

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
            match = _ORDER_BY_RE.match(order_by)
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
