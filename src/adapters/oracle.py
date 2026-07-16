from __future__ import annotations

from typing import Any
from urllib.parse import quote_plus

from src.adapters._sql_helpers import (
    ORDER_BY_RE,
    degraded_or_raise,
    int_or_none,
    rows_from_cursor,
)
from src.adapters.base import AdapterResult, DatabaseAdapter
from src.adapters.normalization import normalize_rows
from src.errors import DatabaseError, ValidationError

# Driver error fragments that mean "catalog/object not reachable", matched to
# degrade gracefully instead of failing the whole call.
_JOBS_UNAVAILABLE = ("ORA-00942", "ORA-01031")
_DDL_NOT_FOUND = ("ORA-31603", "ORA-31604")


class OracleAdapter(DatabaseAdapter):
    """Oracle implementation of the generic database adapter contract."""
    dialect_name = "oracle"

    def __init__(self, dsn: str):
        """Initialize adapter with a ready-to-use Oracle DSN."""
        self._dsn = dsn

    @property
    def dialect(self) -> str:
        """Return adapter dialect identifier."""
        return "oracle"

    @classmethod
    def build_dsn(cls, conn_values: dict[str, str]) -> str:
        """Build an Oracle DSN from connection-file values."""
        required = ("username", "password", "host")
        if any(key not in conn_values for key in required):
            return ""
        username = quote_plus(conn_values["username"])
        password = quote_plus(conn_values["password"])
        host = conn_values["host"]
        port = conn_values.get("port", "1521")
        service = conn_values.get("service_name") or conn_values.get("db_name")
        if not service:
            return ""
        return f"{username}/{password}@{host}:{port}/{service}"

    @classmethod
    def default_schema(cls, conn_values: dict[str, str]) -> str:
        """Return Oracle default schema fallback."""
        return conn_values.get("schema", conn_values.get("username", "SYSTEM"))

    @classmethod
    def wrap_select(cls, query: str, limit: int) -> str:
        """Wrap a query to enforce row limit in Oracle syntax."""
        return f"SELECT * FROM ({query}) mcp_subquery FETCH FIRST {int(limit)} ROWS ONLY"

    def open_connection(self) -> Any:
        """Create and return an Oracle connection, translating driver errors."""
        try:
            import oracledb  # type: ignore
        except Exception as exc:
            raise DatabaseError(
                "missing_dependency",
                "Oracle adapter requires the 'oracledb' package.",
                details=str(exc),
            ) from exc
        try:
            return oracledb.connect(dsn=self._dsn)
        except Exception as exc:
            raise DatabaseError(
                "database_error", "Oracle connection failed.", details=str(exc)) from exc

    def _fetch_all(
        self,
        query: str,
        params: dict[str, Any] | tuple[Any, ...] | None = None,
        timeout_ms: int | None = None,
    ) -> list[dict]:
        """Execute SQL and return normalized rows as dictionaries.

        Read path only. Defense in depth: the whole read runs inside an
        engine-enforced read-only transaction (`SET TRANSACTION READ ONLY`, which
        must be the first statement of the transaction), so Oracle itself rejects
        any write regardless of what the lexical QueryGuard let through.
        """
        try:
            with self.open_connection() as conn:
                # python-oracledb timeout property name differs by version.
                if timeout_ms is not None:
                    if hasattr(conn, "call_timeout"):
                        setattr(conn, "call_timeout", int(timeout_ms))
                    if hasattr(conn, "callTimeout"):
                        setattr(conn, "callTimeout", int(timeout_ms))
                with conn.cursor() as cur:
                    cur.execute("SET TRANSACTION READ ONLY")
                    cur.execute(query, params or {})
                    return rows_from_cursor(cur)
        except DatabaseError:
            raise
        except Exception as exc:
            raise DatabaseError(
                "database_error", "Oracle query failed.", details=str(exc)) from exc

    @staticmethod
    def _schema_params(schemas: tuple[str, ...]) -> tuple[str, dict[str, str]]:
        """Build Oracle named placeholders and bind values for schema IN filters."""
        normalized = [schema.upper() for schema in schemas]
        placeholders = []
        params: dict[str, str] = {}
        for idx, schema in enumerate(normalized):
            key = f"s{idx}"
            placeholders.append(f":{key}")
            params[key] = schema
        return ", ".join(placeholders), params

    @staticmethod
    def _q(identifier: str) -> str:
        """Safely quote Oracle identifiers using double quotes."""
        return f"\"{identifier.replace('\"', '\"\"')}\""

    def _with_full_data_type(self, rows: list[dict]) -> list[dict]:
        """Attach formatted Oracle type names and strip internal helper columns."""
        formatted_rows: list[dict] = []
        for row in rows:
            data_type = str(row.get("data_type") or "")
            normalized_type = data_type.upper()
            char_used = str(row.get("helper_char_used") or "").upper()
            char_length = int_or_none(row.get("helper_char_length"))
            data_length = int_or_none(row.get("helper_data_length"))
            data_precision = int_or_none(row.get("helper_data_precision"))
            data_scale = int_or_none(row.get("helper_data_scale"))

            full_data_type = data_type
            if normalized_type in {"CHAR", "VARCHAR2"}:
                if char_used == "C" and char_length is not None:
                    full_data_type = f"{data_type}({char_length} CHAR)"
                elif char_used == "B" and data_length is not None:
                    full_data_type = f"{data_type}({data_length} BYTE)"
                elif char_length is not None:
                    full_data_type = f"{data_type}({char_length})"
            elif normalized_type in {"NCHAR", "NVARCHAR2"}:
                if char_length is not None:
                    full_data_type = f"{data_type}({char_length})"
            elif normalized_type == "RAW":
                if data_length is not None:
                    full_data_type = f"{data_type}({data_length})"
            elif normalized_type == "NUMBER":
                if data_precision is not None and data_scale is None:
                    full_data_type = f"{data_type}({data_precision})"
                elif data_precision is not None and data_scale == 0:
                    full_data_type = f"{data_type}({data_precision})"
                elif data_precision is not None and data_scale is not None:
                    full_data_type = f"{data_type}({data_precision},{data_scale})"
                elif data_precision is None and data_scale is not None:
                    full_data_type = f"{data_type}(*,{data_scale})"

            public_row = {
                key: value
                for key, value in row.items()
                if not key.startswith("helper_")
            }
            public_row["full_data_type"] = full_data_type
            formatted_rows.append(public_row)
        return formatted_rows

    def list_tables(self, schemas: tuple[str, ...], include_system: bool) -> AdapterResult:
        """List tables and views available in selected schemas."""
        in_clause, params = self._schema_params(schemas)
        in_clause_views = []
        view_params: dict[str, str] = {}
        for idx, schema in enumerate([s.upper() for s in schemas]):
            key = f"v_s{idx}"
            in_clause_views.append(f":{key}")
            view_params[key] = schema
        excluded = "AND owner NOT IN ('SYS', 'SYSTEM', 'XDB', 'MDSYS', 'CTXSYS')"
        query = f"""
            SELECT owner AS schema, table_name, 'BASE TABLE' AS table_type,
                (SELECT comments FROM all_tab_comments tc
                  WHERE tc.owner = all_tables.owner
                    AND tc.table_name = all_tables.table_name) AS table_comment
            FROM all_tables
            WHERE owner IN ({in_clause})
            {" " if include_system else excluded}
            UNION ALL
            SELECT owner AS schema, view_name AS table_name, 'VIEW' AS table_type,
                (SELECT comments FROM all_tab_comments tc
                  WHERE tc.owner = all_views.owner
                    AND tc.table_name = all_views.view_name) AS table_comment
            FROM all_views
            WHERE owner IN ({", ".join(in_clause_views)})
            {" " if include_system else excluded}
            ORDER BY schema, table_name
        """
        all_params = params | view_params
        data = self._fetch_all(query, all_params)
        return AdapterResult(data=data)

    def list_columns(self, table: str, schemas: tuple[str, ...]) -> AdapterResult:
        """List columns for a table in the selected schema scope."""
        in_clause, params = self._schema_params(schemas)
        params["table_name"] = table.upper()
        query = f"""
            SELECT
                owner AS schema,
                table_name,
                column_name,
                column_id AS ordinal_position,
                data_type,
                data_type AS udt_name,
                CASE nullable WHEN 'Y' THEN 1 ELSE 0 END AS is_nullable,
                data_default AS column_default,
                (SELECT comments FROM all_col_comments cc
                  WHERE cc.owner = all_tab_columns.owner
                    AND cc.table_name = all_tab_columns.table_name
                    AND cc.column_name = all_tab_columns.column_name) AS "comment",
                char_used AS helper_char_used,
                char_length AS helper_char_length,
                data_length AS helper_data_length,
                data_precision AS helper_data_precision,
                data_scale AS helper_data_scale
            FROM all_tab_columns
            WHERE table_name = :table_name
              AND owner IN ({in_clause})
            ORDER BY owner, table_name, column_id
        """
        data = self._with_full_data_type(self._fetch_all(query, params))
        return AdapterResult(data=data)

    def list_constraints(
        self,
        schemas: tuple[str, ...],
        table: str | None = None,
        constraint_type: str | None = None,
    ) -> AdapterResult:
        """List table constraints with optional filters."""
        in_clause, params = self._schema_params(schemas)
        params["table_name"] = table.upper() if table else None
        type_map = {
            "PRIMARY KEY": "P",
            "FOREIGN KEY": "R",
            "UNIQUE": "U",
            "CHECK": "C",
        }
        params["constraint_type"] = type_map.get(
            (constraint_type or "").upper()) if constraint_type else None
        query = f"""
            SELECT
                c.owner AS schema,
                c.table_name,
                c.constraint_name,
                CASE c.constraint_type
                    WHEN 'P' THEN 'PRIMARY KEY'
                    WHEN 'R' THEN 'FOREIGN KEY'
                    WHEN 'U' THEN 'UNIQUE'
                    WHEN 'C' THEN 'CHECK'
                    ELSE c.constraint_type
                END AS constraint_type,
                LISTAGG(col.column_name, ', ') WITHIN GROUP (ORDER BY col.position) AS columns,
                r.owner AS foreign_table_schema,
                r.table_name AS foreign_table_name,
                LISTAGG(rcol.column_name, ', ') WITHIN GROUP (ORDER BY rcol.position) AS foreign_columns,
                c.search_condition_vc AS check_clause
            FROM all_constraints c
            LEFT JOIN all_cons_columns col
              ON c.owner = col.owner
             AND c.constraint_name = col.constraint_name
            LEFT JOIN all_constraints r
              ON c.r_owner = r.owner
             AND c.r_constraint_name = r.constraint_name
            LEFT JOIN all_cons_columns rcol
              ON r.owner = rcol.owner
             AND r.constraint_name = rcol.constraint_name
             AND rcol.position = col.position
            WHERE c.owner IN ({in_clause})
              AND c.constraint_type IN ('P','R','U','C')
              AND (:table_name IS NULL OR c.table_name = :table_name)
              AND (:constraint_type IS NULL OR c.constraint_type = :constraint_type)
            GROUP BY c.owner, c.table_name, c.constraint_name, c.constraint_type,
                     r.owner, r.table_name, c.search_condition_vc
            ORDER BY c.owner, c.table_name, c.constraint_name
        """
        data = self._fetch_all(query, params)
        return AdapterResult(data=data)

    def list_sequences(self, schemas: tuple[str, ...]) -> AdapterResult:
        """List sequences for selected schemas."""
        in_clause, params = self._schema_params(schemas)
        query = f"""
            SELECT
                sequence_owner AS schema,
                sequence_name,
                -- Oracle does not retain the original START WITH; expose the key
                -- as NULL so the row shape matches the other dialects.
                NULL AS start_value,
                min_value,
                max_value,
                increment_by,
                cycle_flag AS cycle,
                cache_size,
                last_number AS last_value
            FROM all_sequences
            WHERE sequence_owner IN ({in_clause})
            ORDER BY sequence_owner, sequence_name
        """
        data = self._fetch_all(query, params)
        return AdapterResult(data=data)

    def list_procedures(self, schemas: tuple[str, ...]) -> AdapterResult:
        """List stored procedures for selected schemas."""
        in_clause, params = self._schema_params(schemas)
        query = f"""
            SELECT
                owner AS schema,
                object_name AS procedure_name,
                NULL AS arguments,
                NULL AS language,
                NULL AS volatility
            FROM all_procedures
            WHERE owner IN ({in_clause})
              AND object_type = 'PROCEDURE'
            ORDER BY owner, object_name
        """
        data = self._fetch_all(query, params)
        return AdapterResult(data=data)

    def list_functions(self, schemas: tuple[str, ...]) -> AdapterResult:
        """List functions for selected schemas."""
        in_clause, params = self._schema_params(schemas)
        query = f"""
            SELECT
                owner AS schema,
                object_name AS function_name,
                NULL AS arguments,
                NULL AS return_type,
                NULL AS language,
                NULL AS volatility
            FROM all_procedures
            WHERE owner IN ({in_clause})
              AND object_type = 'FUNCTION'
            ORDER BY owner, object_name
        """
        data = self._fetch_all(query, params)
        return AdapterResult(data=data)

    def list_jobs(self) -> AdapterResult:
        """List scheduler jobs when Oracle scheduler metadata is accessible."""
        try:
            data = self._fetch_all(
                """
                SELECT
                    owner AS schema,
                    job_name,
                    enabled,
                    state,
                    TO_CHAR(last_start_date, 'YYYY-MM-DD"T"HH24:MI:SS TZH:TZM') AS last_start_date,
                    TO_CHAR(next_run_date, 'YYYY-MM-DD"T"HH24:MI:SS TZH:TZM') AS next_run_date
                FROM all_scheduler_jobs
                ORDER BY owner, job_name
                """
            )
            return AdapterResult(data=data, status="available")
        except DatabaseError as exc:
            details = str(exc.details or "")
            return degraded_or_raise(
                exc,
                matched=any(code in details for code in _JOBS_UNAVAILABLE),
                warning="Oracle scheduler catalog is not available for this user.",
            )

    def list_indexes(self, schemas: tuple[str, ...], table: str | None = None) -> AdapterResult:
        """List indexes for selected schemas, optionally filtered by table."""
        in_clause, params = self._schema_params(schemas)
        params["table_name"] = table.upper() if table else None
        query = f"""
            SELECT
                i.owner AS schema,
                i.table_name,
                i.index_name,
                CASE i.uniqueness WHEN 'UNIQUE' THEN 1 ELSE 0 END AS is_unique,
                CASE WHEN c.constraint_type = 'P' THEN 1 ELSE 0 END AS is_primary,
                i.index_type,
                LISTAGG(col.column_name, ', ')
                    WITHIN GROUP (ORDER BY col.column_position) AS columns
            FROM all_indexes i
            LEFT JOIN all_ind_columns col
              ON i.owner = col.index_owner
             AND i.index_name = col.index_name
            LEFT JOIN all_constraints c
              ON c.owner = i.owner
             AND c.index_name = i.index_name
             AND c.constraint_type = 'P'
            WHERE i.owner IN ({in_clause})
              AND (:table_name IS NULL OR i.table_name = :table_name)
            GROUP BY i.owner, i.table_name, i.index_name, i.uniqueness,
                     i.index_type, c.constraint_type
            ORDER BY i.owner, i.table_name, i.index_name
        """
        data = self._fetch_all(query, params)
        return AdapterResult(data=data, status="available")

    def get_ddl(self, schema: str, object_name: str, object_type: str) -> AdapterResult:
        """Return the DDL of a table, view, procedure or function via DBMS_METADATA."""
        normalized_type = object_type.strip().upper()
        if normalized_type not in {"TABLE", "VIEW", "PROCEDURE", "FUNCTION"}:
            return AdapterResult(
                data=[],
                warnings=[
                    "Oracle DDL retrieval supports object_type 'table', 'view', "
                    "'procedure' or 'function'."
                ],
                status="not_supported",
            )
        try:
            data = self._fetch_all(
                """
                SELECT
                    :otype_label AS object_type,
                    :oowner AS schema,
                    :oname AS object_name,
                    DBMS_METADATA.GET_DDL(:otype, :oname2, :oowner2) AS ddl
                FROM dual
                """,
                {
                    "otype_label": normalized_type.lower(),
                    "oowner": schema.upper(),
                    "oname": object_name.upper(),
                    "otype": normalized_type,
                    "oname2": object_name.upper(),
                    "oowner2": schema.upper(),
                },
            )
        except DatabaseError as exc:
            details = str(exc.details or "")
            return degraded_or_raise(
                exc,
                matched=any(code in details for code in _DDL_NOT_FOUND),
                warning=f"No {normalized_type.lower()} '{schema}.{object_name}' found.",
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
        in_clause, params = self._schema_params(schemas)
        type_map = {
            "table": "TABLE",
            "view": "VIEW",
            "sequence": "SEQUENCE",
            "procedure": "PROCEDURE",
            "function": "FUNCTION",
        }
        oracle_types = [type_map[t] for t in object_types if t in type_map]
        type_placeholders = []
        for idx, oracle_type in enumerate(oracle_types):
            key = f"t{idx}"
            type_placeholders.append(f":{key}")
            params[key] = oracle_type
        params["pattern"] = f"%{pattern.upper()}%"
        query = f"""
            SELECT
                owner AS schema,
                object_name,
                CASE object_type
                    WHEN 'TABLE' THEN 'table'
                    WHEN 'VIEW' THEN 'view'
                    WHEN 'SEQUENCE' THEN 'sequence'
                    WHEN 'PROCEDURE' THEN 'procedure'
                    WHEN 'FUNCTION' THEN 'function'
                END AS object_type
            FROM all_objects
            WHERE owner IN ({in_clause})
              AND object_type IN ({", ".join(type_placeholders)})
              AND UPPER(object_name) LIKE :pattern
            ORDER BY owner, object_type, object_name
        """
        data = self._fetch_all(query, params)
        return AdapterResult(data=data, status="available")

    def sample_table(
        self,
        schema: str,
        table: str,
        limit: int,
        order_by: str | None,
    ) -> AdapterResult:
        """Return a bounded table preview with optional ORDER BY."""
        schema_q = self._q(schema.upper())
        table_q = self._q(table.upper())
        query = f"SELECT * FROM {schema_q}.{table_q}"
        if order_by:
            match = ORDER_BY_RE.match(order_by)
            if not match:
                raise ValidationError(
                    "invalid_order_by",
                    "order_by must be in format 'column' or 'column ASC|DESC'.",
                )
            col_q = self._q(match.group(1).upper())
            direction = (match.group(2) or "ASC").upper()
            query += f" ORDER BY {col_q} {direction}"
        query += f" FETCH FIRST {int(limit)} ROWS ONLY"
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
        schema_q = self._q(schema.upper())
        table_q = self._q(table.upper())
        cols = ", ".join(self._q(column.upper()) for column in columns)
        query = f"SELECT {cols} FROM {schema_q}.{table_q} FETCH FIRST {int(limit)} ROWS ONLY"
        data = self._fetch_all(query)
        return AdapterResult(data=data, schema_used=schema)

    def run_select(self, sql_query: str, timeout_ms: int) -> AdapterResult:
        """Run a read-only SQL query with timeout controls when supported."""
        data = self._fetch_all(sql_query, timeout_ms=timeout_ms)
        return AdapterResult(data=data)

    def explain_select(self, sql_query: str, timeout_ms: int) -> AdapterResult:
        """Return an Oracle estimated execution plan for a validated SELECT."""
        try:
            with self.open_connection() as conn:
                if hasattr(conn, "call_timeout"):
                    setattr(conn, "call_timeout", int(timeout_ms))
                if hasattr(conn, "callTimeout"):
                    setattr(conn, "callTimeout", int(timeout_ms))
                with conn.cursor() as cur:
                    cur.execute(f"EXPLAIN PLAN FOR {sql_query}")
                    cur.execute(
                        "SELECT plan_table_output FROM TABLE(DBMS_XPLAN.DISPLAY())"
                    )
                    data = normalize_rows(
                        [{"plan_text": row[0]} for row in cur.fetchall()]
                    )
                    return AdapterResult(data=data, status="explain")
        except DatabaseError:
            raise
        except Exception as exc:
            raise DatabaseError(
                "database_error",
                "Oracle explain plan failed.",
                details=str(exc),
            ) from exc
