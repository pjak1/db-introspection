from __future__ import annotations

import re
from urllib.parse import quote_plus
from typing import Any

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from src.adapters.base import AdapterResult, DatabaseAdapter
from src.adapters.normalization import normalize_rows
from src.errors import DatabaseError, ValidationError

_ORDER_BY_RE = re.compile(
    r"^\s*([A-Za-z_][A-Za-z0-9_$]*)(?:\s+(asc|desc))?\s*$", re.IGNORECASE)


class PostgresAdapter(DatabaseAdapter):
    """PostgreSQL implementation of the generic database adapter contract."""
    dialect_name = "postgres"
    dsn_env_var = "POSTGRES_DSN"

    def __init__(self, dsn: str):
        """Initialize adapter with a ready-to-use PostgreSQL DSN."""
        self._dsn = dsn

    @property
    def dialect(self) -> str:
        """Return adapter dialect identifier."""
        return "postgres"

    @classmethod
    def build_dsn(cls, conn_values: dict[str, str], env: dict[str, str]) -> str:
        """Build a PostgreSQL DSN from connection-file values."""
        required = ("host", "db_name", "port", "username", "password")
        if any(key not in conn_values for key in required):
            return ""
        username = quote_plus(conn_values["username"])
        password = quote_plus(conn_values["password"])
        host = conn_values["host"]
        port = conn_values["port"]
        db_name = conn_values["db_name"]
        return f"postgresql://{username}:{password}@{host}:{port}/{db_name}"

    @classmethod
    def default_schema(cls, conn_values: dict[str, str]) -> str:
        """Return PostgreSQL default schema fallback."""
        return conn_values.get("schema", "public")

    @classmethod
    def wrap_select(cls, query: str, limit: int) -> str:
        """Wrap a query to enforce row limit in PostgreSQL syntax."""
        return f"SELECT * FROM ({query}) AS mcp_subquery LIMIT {int(limit)}"

    def _fetch_all(
        self,
        query: str | sql.Composable,
        params: tuple[Any, ...] | None = None,
        timeout_ms: int | None = None,
    ) -> list[dict]:
        """Execute SQL and return normalized rows as dictionaries."""
        try:
            with psycopg.connect(self._dsn, autocommit=False) as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    if timeout_ms is not None:
                        cur.execute(
                            f"SET LOCAL statement_timeout = {int(timeout_ms)}")
                    cur.execute(query, params or ())
                    if cur.description is None:
                        return []
                    rows = cur.fetchall()
                    return normalize_rows(rows)
        except psycopg.Error as exc:
            raise DatabaseError(
                "database_error", "Database query failed.", details=str(exc)) from exc

    def list_tables(self, schemas: tuple[str, ...], include_system: bool) -> AdapterResult:
        """List tables and views available in selected schemas."""
        query = """
            SELECT
                table_schema AS schema,
                table_name,
                table_type
            FROM information_schema.tables
            WHERE table_schema = ANY(%s)
              AND (
                    %s
                    OR (table_schema NOT LIKE 'pg_%%' AND table_schema <> 'information_schema')
              )
            ORDER BY table_schema, table_name
        """
        data = self._fetch_all(query, (list(schemas), include_system))
        return AdapterResult(data=data)

    def list_columns(self, table: str, schemas: tuple[str, ...]) -> AdapterResult:
        """List columns for a table in the selected schema scope."""
        query = """
            SELECT
                cols.table_schema AS schema,
                cols.table_name,
                cols.column_name,
                cols.ordinal_position,
                cols.data_type,
                cols.udt_name,
                (cols.is_nullable = 'YES') AS is_nullable,
                cols.column_default
            FROM information_schema.columns cols
            WHERE cols.table_name = %s
              AND cols.table_schema = ANY(%s)
            ORDER BY cols.table_schema, cols.table_name, cols.ordinal_position
        """
        data = self._fetch_all(query, (table, list(schemas)))
        return AdapterResult(data=data)

    def list_constraints(
        self,
        schemas: tuple[str, ...],
        table: str | None = None,
        constraint_type: str | None = None,
    ) -> AdapterResult:
        """List table constraints with optional filters."""
        query = """
            SELECT
                tc.constraint_schema AS schema,
                tc.table_name,
                tc.constraint_name,
                tc.constraint_type,
                COALESCE(
                    string_agg(DISTINCT kcu.column_name, ', ' ORDER BY kcu.column_name),
                    ''
                ) AS columns,
                ccu.table_schema AS foreign_table_schema,
                ccu.table_name AS foreign_table_name,
                COALESCE(
                    string_agg(DISTINCT ccu.column_name, ', ' ORDER BY ccu.column_name),
                    ''
                ) AS foreign_columns,
                chk.check_clause
            FROM information_schema.table_constraints tc
            LEFT JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.constraint_schema = kcu.constraint_schema
             AND tc.table_name = kcu.table_name
            LEFT JOIN information_schema.constraint_column_usage ccu
              ON tc.constraint_name = ccu.constraint_name
             AND tc.constraint_schema = ccu.constraint_schema
            LEFT JOIN information_schema.check_constraints chk
              ON tc.constraint_name = chk.constraint_name
             AND tc.constraint_schema = chk.constraint_schema
            WHERE tc.constraint_schema = ANY(%s)
        """
        params: list[Any] = [list(schemas)]

        normalized_table = table.strip() if isinstance(table, str) else None
        if normalized_table:
            query += "\n  AND tc.table_name = %s"
            params.append(normalized_table)

        normalized_type = constraint_type.strip().upper(
        ) if isinstance(constraint_type, str) else None
        if normalized_type:
            query += "\n  AND tc.constraint_type = %s"
            params.append(normalized_type)

        query += """
            GROUP BY
                tc.constraint_schema,
                tc.table_name,
                tc.constraint_name,
                tc.constraint_type,
                ccu.table_schema,
                ccu.table_name,
                chk.check_clause
            ORDER BY tc.constraint_schema, tc.table_name, tc.constraint_name
        """
        data = self._fetch_all(query, tuple(params))
        return AdapterResult(data=data)

    def list_sequences(self, schemas: tuple[str, ...]) -> AdapterResult:
        """List sequences for selected schemas."""
        query = """
            SELECT
                schemaname AS schema,
                sequencename AS sequence_name,
                start_value,
                min_value,
                max_value,
                increment_by,
                cycle,
                cache_size,
                last_value
            FROM pg_sequences
            WHERE schemaname = ANY(%s)
            ORDER BY schemaname, sequencename
        """
        data = self._fetch_all(query, (list(schemas),))
        return AdapterResult(data=data)

    def list_procedures(self, schemas: tuple[str, ...]) -> AdapterResult:
        """List stored procedures for selected schemas."""
        query = """
            SELECT
                n.nspname AS schema,
                p.proname AS procedure_name,
                pg_get_function_identity_arguments(p.oid) AS arguments,
                l.lanname AS language,
                p.provolatile AS volatility
            FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            JOIN pg_language l ON l.oid = p.prolang
            WHERE n.nspname = ANY(%s)
              AND p.prokind = 'p'
            ORDER BY n.nspname, p.proname
        """
        data = self._fetch_all(query, (list(schemas),))
        return AdapterResult(data=data)

    def list_functions(self, schemas: tuple[str, ...]) -> AdapterResult:
        """List functions for selected schemas."""
        query = """
            SELECT
                n.nspname AS schema,
                p.proname AS function_name,
                pg_get_function_identity_arguments(p.oid) AS arguments,
                pg_get_function_result(p.oid) AS return_type,
                l.lanname AS language,
                p.provolatile AS volatility
            FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            JOIN pg_language l ON l.oid = p.prolang
            WHERE n.nspname = ANY(%s)
              AND p.prokind = 'f'
            ORDER BY n.nspname, p.proname
        """
        data = self._fetch_all(query, (list(schemas),))
        return AdapterResult(data=data)

    def list_jobs(self) -> AdapterResult:
        """List pg_cron jobs when the extension is installed and accessible."""
        try:
            data = self._fetch_all("SELECT * FROM cron.job ORDER BY jobid")
            return AdapterResult(data=data, status="available")
        except DatabaseError as exc:
            details = str(exc.details or "")
            details_lower = details.lower()
            if (
                "relation \"cron.job\" does not exist" in details_lower
                or "schema \"cron\" does not exist" in details_lower
                or "permission denied for schema cron" in details_lower
                or "permission denied for table job" in details_lower
            ):
                warning = "PostgreSQL cron catalog (pg_cron) is not available for this database/user."
                return AdapterResult(data=[], warnings=[warning], status="not_available")
            raise

    def sample_table(
        self,
        schema: str,
        table: str,
        limit: int,
        order_by: str | None,
    ) -> AdapterResult:
        """Return a bounded table preview with optional ORDER BY."""
        base_query = sql.SQL("SELECT * FROM {}.{}").format(
            sql.Identifier(schema),
            sql.Identifier(table),
        )
        params: list[Any] = [limit]

        if order_by:
            match = _ORDER_BY_RE.match(order_by)
            if not match:
                raise ValidationError(
                    "invalid_order_by",
                    "order_by must be in format 'column' or 'column ASC|DESC'.",
                )
            column_name = match.group(1)
            direction = (match.group(2) or "ASC").upper()
            base_query += sql.SQL(" ORDER BY {} {}").format(
                sql.Identifier(column_name),
                sql.SQL(direction),
            )

        base_query += sql.SQL(" LIMIT %s")
        data = self._fetch_all(base_query, tuple(params))
        return AdapterResult(data=data, schema_used=schema)

    def select_columns(
        self,
        schema: str,
        table: str,
        columns: list[str],
        limit: int,
    ) -> AdapterResult:
        """Return a bounded projection for selected table columns."""
        query = sql.SQL("SELECT {} FROM {}.{} LIMIT %s").format(
            sql.SQL(", ").join(sql.Identifier(column) for column in columns),
            sql.Identifier(schema),
            sql.Identifier(table),
        )
        data = self._fetch_all(query, (limit,))
        return AdapterResult(data=data, schema_used=schema)

    def run_select(self, sql_query: str, timeout_ms: int) -> AdapterResult:
        """Run a read-only SQL query with statement timeout applied."""
        data = self._fetch_all(sql_query, timeout_ms=timeout_ms)
        return AdapterResult(data=data)
