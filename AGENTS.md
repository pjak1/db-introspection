# AGENTS.md

## Scope

These instructions apply to Codex behavior in this repository only.
Do not change runtime behavior of MCP tools to enforce these rules.

## DB Tool Selection Policy

When a user request can be fulfilled by a specialized DB introspection tool, use that tool first.
Use `db_run_select` only as a last resort (fallback) for requests that cannot be expressed with specialized tools.

### Preferred tool mapping

- List tables: use `db_list_tables`.
- Find an object by partial/unknown name: use `db_search_objects`.
- List columns of a table: use `db_list_columns`.
- Metadata and schema objects: use `db_list_constraints`, `db_list_indexes`, `db_list_sequences`, `db_list_procedures`, `db_list_functions`, `db_list_jobs`.
- Read the source/definition of a view, procedure or function (tables only on Oracle): use `db_get_ddl`.
- Show table data preview / rows from one table: use `db_sample_table`.
- Select specific columns from one table: use `db_select_columns`.
- Use `db_run_select` only for advanced queries that specialized tools cannot cover:
  - joins across tables
  - CTE queries
  - aggregate/grouped queries
  - complex filtering logic
  - window functions

## Do / Don't examples

- Don't: "list tables" -> `db_run_select`
- Do: "list tables" -> `db_list_tables`
- Don't: "show data from table X" -> `db_run_select`
- Do: "show data from table X" -> `db_sample_table`
- Don't: "find the table that stores invoices" -> `db_run_select`
- Do: "find the table that stores invoices" -> `db_search_objects`
- Don't: "show the definition of view V" -> `db_run_select`
- Do: "show the definition of view V" -> `db_get_ddl`

## Output formatting

- Please present data obtained from the DB in table form.

## Implementation constraints

- Keep `server.py` and `src/services/*` behavior unchanged for this policy.
- Keep public MCP tool signatures and response envelopes unchanged.
