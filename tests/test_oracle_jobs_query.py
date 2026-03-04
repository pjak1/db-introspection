from src.adapters.oracle import OracleAdapter
from src.errors import DatabaseError


def test_oracle_list_jobs_uses_to_char_for_timezone_columns(monkeypatch):
    captured: dict[str, str] = {}

    def fake_fetch_all(self, query, params=None, timeout_ms=None):
        captured["query"] = query
        return [
            {
                "schema": "SAMPLE_SCHEMA",
                "job_name": "DATA_UPDATE",
                "enabled": "TRUE",
                "state": "SCHEDULED",
                "last_start_date": "2026-02-27T00:05:01 +01:00",
                "next_run_date": "2026-02-28T00:05:00 +01:00",
            }
        ]

    monkeypatch.setattr(OracleAdapter, "_fetch_all", fake_fetch_all)
    adapter = OracleAdapter(dsn="user/pass@db")

    result = adapter.list_jobs()

    assert result.status == "available"
    assert result.data[0]["schema"] == "SAMPLE_SCHEMA"
    assert 'TO_CHAR(last_start_date, \'YYYY-MM-DD"T"HH24:MI:SS TZH:TZM\')' in captured["query"]
    assert 'TO_CHAR(next_run_date, \'YYYY-MM-DD"T"HH24:MI:SS TZH:TZM\')' in captured["query"]


def test_oracle_list_jobs_returns_not_available_on_missing_catalog(monkeypatch):
    def raise_ora_942(self, query, params=None, timeout_ms=None):
        raise DatabaseError("database_error", "Oracle query failed.",
                            details="ORA-00942: table or view does not exist")

    monkeypatch.setattr(OracleAdapter, "_fetch_all", raise_ora_942)
    adapter = OracleAdapter(dsn="user/pass@db")

    result = adapter.list_jobs()

    assert result.status == "not_available"
    assert result.data == []
    assert result.warnings
