"""Guard the referenced side of SQL Server foreign-key constraints.

`INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE` lists the columns a constraint is
*defined on*, so using it for the referenced table makes a foreign key point at
itself. The referenced side has to come from the `sys` catalogs instead.
"""
from src.adapters.mssql import MssqlAdapter


def _capture(adapter):
    """Patch `_fetch_all` to capture the emitted query and bind params."""
    captured: dict = {}

    def fake_fetch(query, params=None, timeout_ms=None):  # noqa: ANN001
        captured["query"] = query
        captured["params"] = params
        return []

    adapter._fetch_all = fake_fetch  # type: ignore[method-assign]
    return captured


def test_list_constraints_resolves_referenced_side_from_sys_catalogs():
    adapter = MssqlAdapter("DRIVER={unused}")
    captured = _capture(adapter)
    adapter.list_constraints(schemas=("dbo",))

    query = captured["query"]
    # The referenced table/columns must come from sys.foreign_keys, which knows
    # both ends of the relationship.
    assert "sys.foreign_keys" in query
    assert "sys.foreign_key_columns" in query
    assert "fkc.referenced_object_id" in query
    assert "fk.ref_table AS foreign_table_name" in query
    assert "fk.ref_columns AS foreign_columns" in query
    # CONSTRAINT_COLUMN_USAGE describes the referencing side, so it must not be
    # the source of the foreign_* columns.
    assert "CONSTRAINT_COLUMN_USAGE" not in query


def test_list_constraints_orders_composite_columns_deterministically():
    """A composite key's columns must come back in declaration order."""
    adapter = MssqlAdapter("DRIVER={unused}")
    captured = _capture(adapter)
    adapter.list_constraints(schemas=("dbo",))

    query = captured["query"]
    assert "WITHIN GROUP (ORDER BY kcu.ORDINAL_POSITION)" in query
    assert "WITHIN GROUP (ORDER BY fkc.constraint_column_id)" in query


def test_list_constraints_passes_optional_filters():
    adapter = MssqlAdapter("DRIVER={unused}")
    captured = _capture(adapter)
    adapter.list_constraints(
        schemas=("dbo",),
        table="sample_table",
        constraint_type="primary key",
    )

    assert captured["params"] == ("dbo", "sample_table", "sample_table",
                                  "PRIMARY KEY", "PRIMARY KEY")
