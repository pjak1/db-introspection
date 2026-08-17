from __future__ import annotations

from pathlib import Path
from urllib.parse import quote_plus
from typing import Any

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from src.adapters._sql_helpers import ORDER_BY_RE, degraded_or_raise, stream_cursor_to_file
from src.adapters.base import AdapterResult, DatabaseAdapter
from src.adapters.session import SessionPolicy
from src.adapters.normalization import normalize_rows
from src.errors import DatabaseError, ValidationError


class _PostgresSessionPolicy:
    """Read-only enforcement for a PostgreSQL session: a connection-level flag."""

    def begin(self, conn: Any) -> None:
        """Put the connection in read-only mode for every transaction it starts.

        psycopg refuses to set `read_only` once a transaction is in progress, so
        this has to happen before the session's first query rather than per query.
        """
        conn.read_only = True

    def end(self, conn: Any) -> None:
        """Discard the read transaction and close the connection."""
        try:
            conn.rollback()
        finally:
            conn.close()

    def recover(self, conn: Any) -> None:
        """Clear the aborted transaction so the session stays usable.

        PostgreSQL rejects every command after a failed one with "current
        transaction is aborted" until the transaction ends. Without this rollback
        a single failing query would take down every later query of the same
        tool call — `health_check`, which degrades per check, depends on it.
        `read_only` survives the rollback because it applies to new transactions.
        """
        conn.rollback()


class PostgresAdapter(DatabaseAdapter):
    """PostgreSQL implementation of the generic database adapter contract."""
    dialect_name = "postgres"
    ddl_object_types = ("table", "view", "procedure", "function")

    def __init__(self, dsn: str):
        """Initialize adapter with a ready-to-use PostgreSQL DSN."""
        self._dsn = dsn

    @property
    def dialect(self) -> str:
        """Return adapter dialect identifier."""
        return "postgres"

    @classmethod
    def build_dsn(cls, conn_values: dict[str, str]) -> str:
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

    @staticmethod
    def _q(identifier: str) -> str:
        """Safely quote PostgreSQL identifiers using double quotes."""
        return f"\"{identifier.replace('\"', '\"\"')}\""

    def open_connection(self) -> Any:
        """Create and return a PostgreSQL connection, translating driver errors."""
        try:
            return psycopg.connect(self._dsn, autocommit=False)
        except psycopg.Error as exc:
            raise DatabaseError(
                "database_error", "PostgreSQL connection failed.", details=str(exc)) from exc

    def session_policy(self) -> SessionPolicy:
        """Return PostgreSQL's read-only session policy."""
        return _PostgresSessionPolicy()

    def _fetch_all(
        self,
        query: str | sql.Composable,
        params: tuple[Any, ...] | None = None,
        timeout_ms: int | None = None,
    ) -> list[dict]:
        """Execute SQL and return normalized rows as dictionaries.

        Read path only. Defense in depth: the connection is put in an
        engine-enforced read-only transaction by `_PostgresSessionPolicy`, so
        PostgreSQL itself rejects any write regardless of what the lexical
        QueryGuard let through.
        """
        try:
            with self.session() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    if timeout_ms is not None:
                        cur.execute(
                            f"SET LOCAL statement_timeout = {int(timeout_ms)}")
                    cur.execute(query, params or ())
                    rows = [] if cur.description is None else normalize_rows(cur.fetchall())
                    if timeout_ms is not None:
                        # SET LOCAL is transaction-scoped and the transaction now
                        # spans the whole session, so undo it or it leaks into the
                        # next query of the same tool call. Only on the success
                        # path: after a failure the transaction is aborted and the
                        # session rollback discards the setting anyway.
                        cur.execute("SET LOCAL statement_timeout = DEFAULT")
                    return rows
        except psycopg.Error as exc:
            raise DatabaseError(
                "database_error", "Database query failed.", details=str(exc)) from exc

    def list_tables(self, schemas: tuple[str, ...], include_system: bool) -> AdapterResult:
        """List tables and views available in selected schemas."""
        query = """
            SELECT
                t.table_schema AS schema,
                t.table_name,
                t.table_type,
                (
                    SELECT obj_description(c.oid, 'pg_class')
                    FROM pg_catalog.pg_class c
                    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = t.table_schema
                      AND c.relname = t.table_name
                ) AS table_comment
            FROM information_schema.tables t
            WHERE t.table_schema = ANY(%s)
              AND (
                    %s
                    OR (t.table_schema NOT LIKE 'pg_%%' AND t.table_schema <> 'information_schema')
              )
            ORDER BY t.table_schema, t.table_name
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
                cols.column_default,
                pg_catalog.format_type(attr.atttypid, attr.atttypmod) AS full_data_type,
                pg_catalog.col_description(cls.oid, attr.attnum) AS comment
            FROM information_schema.columns cols
            JOIN pg_catalog.pg_namespace ns
              ON ns.nspname = cols.table_schema
            JOIN pg_catalog.pg_class cls
              ON cls.relname = cols.table_name
             AND cls.relnamespace = ns.oid
             AND cls.relkind IN ('r', 'v', 'm', 'f', 'p')
            JOIN pg_catalog.pg_attribute attr
              ON attr.attrelid = cls.oid
             AND attr.attname = cols.column_name
             AND attr.attnum > 0
             AND NOT attr.attisdropped
            WHERE cols.table_name = %s
              AND cols.table_schema = ANY(%s)
            ORDER BY cols.table_schema, cols.table_name, cols.ordinal_position
        """
        data = self._fetch_all(query, (table, list(schemas)))
        return AdapterResult(data=data)

    @staticmethod
    def _normalize_explain_rows(rows: list[dict]) -> list[dict]:
        """Map PostgreSQL EXPLAIN rows to the common public plan row shape."""
        normalized: list[dict] = []
        for row in rows:
            plan_text = row.get("QUERY PLAN")
            if plan_text is None:
                plan_text = row.get("query_plan")
            if plan_text is None and row:
                plan_text = next(iter(row.values()))
            normalized.append({"plan_text": plan_text})
        return normalize_rows(normalized)

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
            details_lower = str(exc.details or "").lower()
            cron_unavailable = (
                "relation \"cron.job\" does not exist" in details_lower
                or "schema \"cron\" does not exist" in details_lower
                or "permission denied for schema cron" in details_lower
                or "permission denied for table job" in details_lower
            )
            return degraded_or_raise(
                exc,
                matched=cron_unavailable,
                warning="PostgreSQL cron catalog (pg_cron) is not available for this database/user.",
            )

    def list_indexes(self, schemas: tuple[str, ...], table: str | None = None) -> AdapterResult:
        """List indexes for selected schemas, optionally filtered by table.

        Key and INCLUDE columns are reported separately: `indkey` holds both and
        `indnkeyatts` marks where the key ends, so the split is what tells a
        covering index from a plain one. `is_valid` exposes an index left behind
        by a failed `CREATE INDEX CONCURRENTLY` — it costs writes and is never
        used for reads.
        """
        query = """
            SELECT
                n.nspname AS schema,
                t.relname AS table_name,
                i.relname AS index_name,
                ix.indisunique AS is_unique,
                ix.indisprimary AS is_primary,
                am.amname AS index_type,
                (
                    SELECT string_agg(
                        pg_get_indexdef(ix.indexrelid, k.i, true),
                        ', ' ORDER BY k.i
                    )
                    FROM generate_series(1, ix.indnkeyatts) AS k(i)
                ) AS columns,
                (
                    SELECT string_agg(
                        pg_get_indexdef(ix.indexrelid, k.i, true),
                        ', ' ORDER BY k.i
                    )
                    FROM generate_series(ix.indnkeyatts + 1, ix.indnatts) AS k(i)
                ) AS included_columns,
                ix.indpred IS NOT NULL AS is_partial,
                ix.indisvalid AS is_valid,
                pg_get_indexdef(ix.indexrelid) AS definition
            FROM pg_catalog.pg_index ix
            JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid
            JOIN pg_catalog.pg_class t ON t.oid = ix.indrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
            JOIN pg_catalog.pg_am am ON am.oid = i.relam
            WHERE n.nspname = ANY(%s)
        """
        params: list[Any] = [list(schemas)]
        normalized_table = table.strip() if isinstance(table, str) else None
        if normalized_table:
            query += "\n  AND t.relname = %s"
            params.append(normalized_table)
        query += "\n            ORDER BY n.nspname, t.relname, i.relname"
        data = self._fetch_all(query, tuple(params))
        return AdapterResult(data=data, status="available")

    def index_usage(
        self,
        schemas: tuple[str, ...],
        table: str | None = None,
        include_fragmentation: bool = False,
    ) -> AdapterResult:
        """Return per-index scan counters from pg_stat_all_indexes."""
        # Counters and the reset timestamp are two queries; one session for both.
        with self.session():
            return self._index_usage_in_session(schemas, table, include_fragmentation)

    def _index_usage_in_session(
        self,
        schemas: tuple[str, ...],
        table: str | None,
        include_fragmentation: bool,
    ) -> AdapterResult:
        """Collect index usage rows; assumes a session is already open."""
        # LEFT JOIN from pg_index so an index with no recorded scan still appears.
        query = """
            SELECT
                n.nspname AS schema,
                t.relname AS "table",
                i.relname AS index_name,
                ix.indisunique AS is_unique,
                COALESCE(s.idx_scan, 0) AS reads,
                NULL::bigint AS writes_overhead,
                NULL::timestamptz AS last_used,
                pg_relation_size(ix.indexrelid) AS size_bytes,
                COALESCE(s.idx_scan, 0) = 0 AS never_used,
                COALESCE(s.idx_tup_read, 0) AS idx_tup_read,
                COALESCE(s.idx_tup_fetch, 0) AS idx_tup_fetch
            FROM pg_catalog.pg_index ix
            JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid
            JOIN pg_catalog.pg_class t ON t.oid = ix.indrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
            LEFT JOIN pg_catalog.pg_stat_all_indexes s ON s.indexrelid = ix.indexrelid
            WHERE n.nspname = ANY(%s)
        """
        params: list[Any] = [list(schemas)]
        normalized_table = table.strip() if isinstance(table, str) else None
        if normalized_table:
            query += "\n  AND t.relname = %s"
            params.append(normalized_table)
        query += '\n            ORDER BY n.nspname, t.relname, i.relname'
        rows = self._fetch_all(query, tuple(params))
        stats_since, warnings = self._index_stats_since()
        for row in rows:
            row["stats_since"] = stats_since
        if include_fragmentation:
            warnings.append(
                "PostgreSQL exposes no built-in index fragmentation metric; "
                "install pgstattuple to measure it."
            )
        return AdapterResult(data=rows, warnings=warnings, status="available")

    def column_stats(
        self,
        schema: str,
        table: str,
        column: str | None = None,
        include_histogram: bool = False,
    ) -> AdapterResult:
        """Return per-column planner statistics from pg_stats."""
        # Column stats and extended statistics are two queries; one session.
        with self.session():
            return self._column_stats_in_session(schema, table, column, include_histogram)

    def _column_stats_in_session(
        self,
        schema: str,
        table: str,
        column: str | None,
        include_histogram: bool,
    ) -> AdapterResult:
        """Collect column and extended statistics; assumes an open session."""
        # most_common_vals is anyarray and histogram_bounds can be long, so both
        # are cast to text and only selected when asked for.
        histogram_select = (
            """,
                s.most_common_vals::text AS most_common_vals,
                s.most_common_freqs::text AS most_common_freqs,
                s.histogram_bounds::text AS histogram_bounds"""
            if include_histogram else ""
        )
        query = f"""
            SELECT
                s.schemaname AS schema,
                s.tablename AS "table",
                s.attname AS "column",
                'column' AS kind,
                s.n_distinct AS distinct_estimate,
                s.null_frac AS null_fraction,
                s.avg_width,
                to_char(GREATEST(t.last_analyze, t.last_autoanalyze),
                        'YYYY-MM-DD"T"HH24:MI:SS') AS last_analyzed,
                'pg_stats' AS source,
                s.correlation,
                s.n_distinct < 0 AS distinct_is_fraction{histogram_select}
            FROM pg_catalog.pg_stats s
            LEFT JOIN pg_catalog.pg_stat_all_tables t
              ON t.schemaname = s.schemaname AND t.relname = s.tablename
            WHERE s.schemaname = %s AND s.tablename = %s
        """
        params: list[Any] = [schema, table]
        if column:
            query += "\n  AND s.attname = %s"
            params.append(column)
        query += "\n            ORDER BY s.attname"
        rows = self._fetch_all(query, tuple(params))
        warnings: list[str] = []
        if not rows:
            warnings.append(
                f"No column statistics for '{schema}.{table}'"
                + (f".{column}" if column else "")
                + " — the table may not exist, was never analyzed, or is not "
                "readable by this user."
            )
        if any(row.get("distinct_is_fraction") for row in rows):
            # Documented pg_stats quirk that silently misleads otherwise.
            warnings.append(
                "A negative distinct_estimate is PostgreSQL's way of expressing "
                "distinct values as a fraction of the row count, not a count."
            )
        extended, extended_warnings = self._extended_column_stats(schema, table)
        return AdapterResult(
            data=rows + extended,
            warnings=warnings + extended_warnings,
            status="available" if rows else "not_found",
        )

    def _extended_column_stats(self, schema: str, table: str) -> tuple[list[dict], list[str]]:
        """Return CREATE STATISTICS (extended) statistics, or degrade.

        These are what teach the planner that two columns are correlated. The
        `pg_stats_ext` view is newer than the feature itself, so an older server
        degrades with a warning instead of failing.
        """
        try:
            rows = self._fetch_all(
                """
                SELECT
                    e.schemaname AS schema,
                    e.tablename AS "table",
                    array_to_string(e.attnames, ', ') AS "column",
                    'extended' AS kind,
                    NULL::real AS distinct_estimate,
                    NULL::real AS null_fraction,
                    NULL::integer AS avg_width,
                    NULL::text AS last_analyzed,
                    'pg_stats_ext' AS source,
                    e.statistics_name,
                    e.kinds::text AS kinds,
                    e.n_distinct::text AS extended_n_distinct,
                    e.dependencies::text AS dependencies
                FROM pg_catalog.pg_stats_ext e
                WHERE e.schemaname = %s AND e.tablename = %s
                ORDER BY e.statistics_name
                """,
                (schema, table),
            )
        except DatabaseError as exc:
            details = str(exc.details or "").lower()
            if "pg_stats_ext" not in details:
                raise
            return [], [
                "Extended statistics are unavailable: pg_stats_ext does not exist "
                "on this server version."
            ]
        return rows, []

    def _index_stats_since(self) -> tuple[Any, list[str]]:
        """Return when the statistics collector was last reset, plus warnings.

        `never_used` is only meaningful relative to this timestamp. `track_counts`
        is checked in the same query because with it off nothing is counted at
        all and every index would look unused.
        """
        rows = self._fetch_all(
            """
            SELECT
                s.stats_reset AS stats_since,
                current_setting('track_counts') AS track_counts
            FROM pg_catalog.pg_stat_database s
            WHERE s.datname = current_database()
            """
        )
        if not rows:
            return None, []
        warnings: list[str] = []
        if str(rows[0].get("track_counts", "")).lower() not in {"on", "true", "1"}:
            warnings.append(
                "track_counts is off, so index scan counters are not collected; "
                "every index will look unused."
            )
        return rows[0].get("stats_since"), warnings

    @staticmethod
    def _ddl_quote(identifier: str) -> str:
        """Double-quote an identifier for inclusion in reconstructed DDL text."""
        return '"' + identifier.replace('"', '""') + '"'

    def _table_ddl(self, schema: str, table: str) -> AdapterResult:
        """Reconstruct a CREATE TABLE statement from the PostgreSQL catalogs.

        PostgreSQL has no single "get table DDL" primitive (unlike Oracle's
        DBMS_METADATA), so the statement is assembled from pg_attribute (columns),
        pg_get_constraintdef (constraints) and pg_get_indexdef (secondary indexes).
        The result is a faithful reconstruction, not necessarily byte-identical to
        the original CREATE.
        """
        # Columns, constraints and indexes share one session — three logins for
        # one `db_get_ddl` call would buy nothing.
        with self.session():
            return self._table_ddl_in_session(schema, table)

    def _table_ddl_in_session(self, schema: str, table: str) -> AdapterResult:
        """Assemble the CREATE TABLE text; assumes a session is already open."""
        columns = self._fetch_all(
            """
            SELECT
                a.attname AS name,
                pg_catalog.format_type(a.atttypid, a.atttypmod) AS type,
                a.attnotnull AS notnull,
                pg_catalog.pg_get_expr(ad.adbin, ad.adrelid) AS default_expr,
                a.attidentity AS identity,
                a.attgenerated AS generated
            FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_catalog.pg_attrdef ad
              ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
            WHERE n.nspname = %s AND c.relname = %s
              AND c.relkind IN ('r', 'p', 'f')
              AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY a.attnum
            """,
            (schema, table),
        )
        if not columns:
            return AdapterResult(
                data=[],
                warnings=[f"No table '{schema}.{table}' found."],
                status="not_found",
            )

        constraints = self._fetch_all(
            """
            SELECT con.conname AS name,
                   pg_catalog.pg_get_constraintdef(con.oid, true) AS def
            FROM pg_catalog.pg_constraint con
            JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relname = %s
            ORDER BY CASE con.contype
                         WHEN 'p' THEN 0 WHEN 'u' THEN 1
                         WHEN 'f' THEN 2 WHEN 'c' THEN 3 ELSE 4 END,
                     con.conname
            """,
            (schema, table),
        )

        indexes = self._fetch_all(
            """
            SELECT pg_catalog.pg_get_indexdef(ix.indexrelid) AS def
            FROM pg_catalog.pg_index ix
            JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid
            JOIN pg_catalog.pg_class c ON c.oid = ix.indrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relname = %s
              AND NOT EXISTS (
                  SELECT 1 FROM pg_catalog.pg_constraint con
                  WHERE con.conindid = ix.indexrelid
              )
            ORDER BY i.relname
            """,
            (schema, table),
        )

        body_lines: list[str] = []
        for col in columns:
            line = f"{self._ddl_quote(col['name'])} {col['type']}"
            identity = col.get("identity") or ""
            generated = col.get("generated") or ""
            default_expr = col.get("default_expr")
            if identity == "a":
                line += " GENERATED ALWAYS AS IDENTITY"
            elif identity == "d":
                line += " GENERATED BY DEFAULT AS IDENTITY"
            elif generated == "s" and default_expr:
                line += f" GENERATED ALWAYS AS ({default_expr}) STORED"
            elif default_expr:
                line += f" DEFAULT {default_expr}"
            if col.get("notnull"):
                line += " NOT NULL"
            body_lines.append(line)

        body_lines.extend(
            f"CONSTRAINT {self._ddl_quote(con['name'])} {con['def']}"
            for con in constraints
        )

        table_ref = f"{self._ddl_quote(schema)}.{self._ddl_quote(table)}"
        ddl = f"CREATE TABLE {table_ref} (\n    " + ",\n    ".join(body_lines) + "\n);"
        for idx in indexes:
            ddl += f"\n{idx['def']};"

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
        """Return the DDL of a table, view, function or procedure."""
        normalized_type = object_type.strip().lower()
        if normalized_type == "table":
            return self._table_ddl(schema, object_name)
        if normalized_type == "view":
            query = """
                SELECT 'view' AS object_type, n.nspname AS schema, c.relname AS object_name,
                       pg_get_viewdef(c.oid, true) AS ddl
                FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = %s AND c.relname = %s AND c.relkind IN ('v', 'm')
            """
            data = self._fetch_all(query, (schema, object_name))
        elif normalized_type in {"function", "procedure"}:
            prokind = "p" if normalized_type == "procedure" else "f"
            query = """
                SELECT %s AS object_type, n.nspname AS schema, p.proname AS object_name,
                       pg_get_functiondef(p.oid) AS ddl
                FROM pg_catalog.pg_proc p
                JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
                WHERE n.nspname = %s AND p.proname = %s AND p.prokind = %s
                ORDER BY p.oid
            """
            data = self._fetch_all(query, (normalized_type, schema, object_name, prokind))
        else:
            return AdapterResult(
                data=[],
                warnings=[
                    "PostgreSQL DDL retrieval supports object_type 'table', 'view', "
                    "'function' or 'procedure'."
                ],
                status="not_supported",
            )
        if not data:
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
        query = """
            SELECT o.schema, o.object_name, o.object_type
            FROM (
                SELECT table_schema AS schema, table_name AS object_name,
                       CASE WHEN table_type = 'VIEW' THEN 'view' ELSE 'table' END AS object_type
                FROM information_schema.tables
                WHERE table_schema = ANY(%s)
                UNION ALL
                SELECT schemaname, sequencename, 'sequence'
                FROM pg_catalog.pg_sequences
                WHERE schemaname = ANY(%s)
                UNION ALL
                SELECT n.nspname, p.proname,
                       CASE p.prokind WHEN 'p' THEN 'procedure' ELSE 'function' END
                FROM pg_catalog.pg_proc p
                JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
                WHERE n.nspname = ANY(%s) AND p.prokind IN ('f', 'p')
            ) o
            WHERE o.object_name ILIKE %s
              AND o.object_type = ANY(%s)
            ORDER BY o.schema, o.object_type, o.object_name
        """
        schema_list = list(schemas)
        like = f"%{pattern}%"
        data = self._fetch_all(
            query,
            (schema_list, schema_list, schema_list, like, list(object_types)),
        )
        return AdapterResult(data=data, status="available")

    def sample_table(
        self,
        schema: str,
        table: str,
        limit: int,
        order_by: str | None,
        offset: int = 0,
    ) -> AdapterResult:
        """Return a bounded table preview with optional ORDER BY and offset."""
        base_query = sql.SQL("SELECT * FROM {}.{}").format(
            sql.Identifier(schema),
            sql.Identifier(table),
        )

        if order_by:
            match = ORDER_BY_RE.match(order_by)
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

        base_query += sql.SQL(" LIMIT %s OFFSET %s")
        data = self._fetch_all(base_query, (limit, max(0, int(offset))))
        return AdapterResult(data=data, schema_used=schema)

    def select_columns(
        self,
        schema: str,
        table: str,
        columns: list[str],
        limit: int,
        offset: int = 0,
    ) -> AdapterResult:
        """Return a bounded projection for selected table columns."""
        query = sql.SQL("SELECT {} FROM {}.{} LIMIT %s OFFSET %s").format(
            sql.SQL(", ").join(sql.Identifier(column) for column in columns),
            sql.Identifier(schema),
            sql.Identifier(table),
        )
        data = self._fetch_all(query, (limit, max(0, int(offset))))
        return AdapterResult(data=data, schema_used=schema)

    def run_select(self, sql_query: str, timeout_ms: int) -> AdapterResult:
        """Run a read-only SQL query with statement timeout applied."""
        data = self._fetch_all(sql_query, timeout_ms=timeout_ms)
        return AdapterResult(data=data)

    def export_query(
        self,
        sql_query: str,
        destination: Path,
        fmt: str,
        timeout_ms: int,
        max_rows: int,
    ) -> AdapterResult:
        """Stream a validated SELECT to a file inside a read-only transaction."""
        wrapped = self.wrap_select(sql_query, max_rows + 1)
        try:
            with self.open_connection() as conn:
                conn.read_only = True
                # Plain (tuple) cursor: streaming writer maps values by position.
                with conn.cursor() as cur:
                    cur.arraysize = 1000
                    if timeout_ms is not None:
                        cur.execute(f"SET LOCAL statement_timeout = {int(timeout_ms)}")
                    cur.execute(wrapped)
                    return stream_cursor_to_file(cur, destination, fmt, max_rows)
        except psycopg.Error as exc:
            raise DatabaseError(
                "database_error", "Export query failed.", details=str(exc)) from exc

    def export_table(
        self,
        schema: str,
        table: str,
        columns: list[str] | None,
        order_by: str | None,
        destination: Path,
        fmt: str,
        timeout_ms: int,
        max_rows: int,
    ) -> AdapterResult:
        """Build a dialect-correct table SELECT and stream it to a file."""
        cols = ", ".join(self._q(column) for column in columns) if columns else "*"
        query = f"SELECT {cols} FROM {self._q(schema)}.{self._q(table)}"
        if order_by:
            match = ORDER_BY_RE.match(order_by)
            if not match:
                raise ValidationError(
                    "invalid_order_by",
                    "order_by must be in format 'column' or 'column ASC|DESC'.",
                )
            direction = (match.group(2) or "ASC").upper()
            query += f" ORDER BY {self._q(match.group(1))} {direction}"
        result = self.export_query(query, destination, fmt, timeout_ms, max_rows)
        result.schema_used = schema
        return result

    def explain_select(self, sql_query: str, timeout_ms: int) -> AdapterResult:
        """Return a PostgreSQL estimated execution plan for a validated SELECT."""
        data = self._fetch_all(
            f"EXPLAIN (FORMAT TEXT) {sql_query}",
            timeout_ms=timeout_ms,
        )
        return AdapterResult(
            data=self._normalize_explain_rows(data),
            status="explain",
        )

    def table_stats(self, schema: str, table: str) -> AdapterResult:
        """Return row-count estimate and size statistics for one table."""
        query = """
            SELECT
                n.nspname AS schema,
                c.relname AS "table",
                CASE WHEN c.reltuples < 0 THEN NULL ELSE c.reltuples::bigint END AS row_estimate,
                pg_table_size(c.oid) AS table_bytes,
                pg_indexes_size(c.oid) AS index_bytes,
                pg_total_relation_size(c.oid) AS total_bytes,
                (SELECT count(*) FROM information_schema.columns col
                  WHERE col.table_schema = n.nspname AND col.table_name = c.relname) AS column_count,
                to_char(GREATEST(s.last_analyze, s.last_autoanalyze),
                        'YYYY-MM-DD"T"HH24:MI:SS') AS last_analyzed
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_catalog.pg_stat_all_tables s ON s.relid = c.oid
            WHERE n.nspname = %s AND c.relname = %s
              AND c.relkind IN ('r', 'p', 'f', 'm')
        """
        data = self._fetch_all(query, (schema, table))
        if not data:
            return AdapterResult(
                data=[],
                warnings=[f"No table '{schema}.{table}' found."],
                status="not_found",
            )
        return AdapterResult(data=data, status="available")

    def list_foreign_keys(self, schemas: tuple[str, ...], table: str | None = None) -> AdapterResult:
        """List foreign-key edges for the selected schemas, optionally for one table."""
        action = (
            "CASE {col} WHEN 'a' THEN 'NO ACTION' WHEN 'r' THEN 'RESTRICT' "
            "WHEN 'c' THEN 'CASCADE' WHEN 'n' THEN 'SET NULL' "
            "WHEN 'd' THEN 'SET DEFAULT' END"
        )
        query = f"""
            SELECT
                con.conname AS constraint_name,
                n.nspname AS schema,
                c.relname AS "table",
                (SELECT string_agg(a.attname, ', ' ORDER BY u.ord)
                   FROM unnest(con.conkey) WITH ORDINALITY AS u(attnum, ord)
                   JOIN pg_catalog.pg_attribute a
                     ON a.attrelid = con.conrelid AND a.attnum = u.attnum) AS columns,
                rn.nspname AS ref_schema,
                rc.relname AS ref_table,
                (SELECT string_agg(a.attname, ', ' ORDER BY u.ord)
                   FROM unnest(con.confkey) WITH ORDINALITY AS u(attnum, ord)
                   JOIN pg_catalog.pg_attribute a
                     ON a.attrelid = con.confrelid AND a.attnum = u.attnum) AS ref_columns,
                {action.format(col="con.confdeltype")} AS on_delete,
                {action.format(col="con.confupdtype")} AS on_update
            FROM pg_catalog.pg_constraint con
            JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_catalog.pg_class rc ON rc.oid = con.confrelid
            JOIN pg_catalog.pg_namespace rn ON rn.oid = rc.relnamespace
            WHERE con.contype = 'f'
              AND n.nspname = ANY(%s)
        """
        params: list[Any] = [list(schemas)]
        normalized_table = table.strip() if isinstance(table, str) else None
        if normalized_table:
            query += "\n  AND (c.relname = %s OR rc.relname = %s)"
            params.extend([normalized_table, normalized_table])
        query += "\n            ORDER BY n.nspname, c.relname, con.conname"
        data = self._fetch_all(query, tuple(params))
        return AdapterResult(data=data, status="available")

    def top_queries(self, limit: int) -> AdapterResult:
        """Return the slowest queries by total execution time (pg_stat_statements)."""
        query = """
            SELECT
                queryid AS query_id,
                query,
                calls,
                round(total_exec_time::numeric, 2) AS total_ms,
                round(mean_exec_time::numeric, 2) AS mean_ms,
                rows
            FROM pg_stat_statements
            ORDER BY total_exec_time DESC
            LIMIT %s
        """
        try:
            data = self._fetch_all(query, (int(limit),))
        except DatabaseError as exc:
            details = str(exc.details or "").lower()
            matched = any(
                fragment in details
                for fragment in ("pg_stat_statements", "total_exec_time", "permission denied")
            )
            return degraded_or_raise(
                exc,
                matched=matched,
                warning=(
                    "pg_stat_statements is not installed or not accessible for this "
                    "user; top queries are unavailable."
                ),
            )
        return AdapterResult(data=data, status="available")

    def health_check(self) -> AdapterResult:
        """Run a pragmatic set of PostgreSQL health checks, each degrading alone."""
        checks: list[tuple[str, str, str]] = [
            (
                "cache_hit_ratio",
                """SELECT round(sum(blks_hit) * 100.0
                          / nullif(sum(blks_hit + blks_read), 0), 2) AS v
                   FROM pg_stat_database WHERE datname = current_database()""",
                "%% buffer cache hit ratio (higher is better).",
            ),
            (
                "unused_indexes",
                "SELECT count(*) AS v FROM pg_stat_user_indexes WHERE idx_scan = 0",
                "indexes never used since stats reset.",
            ),
            (
                "invalid_indexes",
                "SELECT count(*) AS v FROM pg_index WHERE indisvalid = false",
                "invalid indexes (failed CONCURRENTLY builds).",
            ),
            (
                "tables_needing_vacuum",
                "SELECT count(*) AS v FROM pg_stat_user_tables WHERE n_dead_tup > 10000",
                "tables with > 10k dead tuples.",
            ),
            (
                "connection_usage",
                """SELECT round(count(*) * 100.0
                          / nullif((SELECT setting::int FROM pg_settings
                                    WHERE name = 'max_connections'), 0), 1) AS v
                   FROM pg_stat_activity""",
                "%% of max_connections in use.",
            ),
        ]
        # One session for all checks. Each check still degrades on its own: a
        # failed query aborts the transaction, and the session policy's recover rolls
        # it back so the remaining checks can still run.
        with self.session():
            rows = [self._run_health_check(name, sql, detail) for name, sql, detail in checks]
        return AdapterResult(data=rows, status="available")

    def _run_health_check(self, name: str, sql: str, detail: str) -> dict:
        """Execute one health-check query, returning a normalized result row."""
        try:
            result = self._fetch_all(sql)
        except DatabaseError as exc:
            return {"check": name, "status": "unknown", "detail": str(exc.details or exc.message)}
        value = result[0].get("v") if result else None
        return {"check": name, "status": "ok", "value": value, "detail": detail}
