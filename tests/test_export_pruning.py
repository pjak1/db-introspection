from __future__ import annotations

import os
import threading
import time
from pathlib import Path

from conftest import BaseStubAdapter, make_settings

from src.services import export
from src.services import introspection_service as intro_mod
from src.services import select_service as sel_mod
from src.services.introspection_service import IntrospectionService
from src.services.select_service import SelectService


def _make(directory: Path, name: str, age_seconds: float) -> Path:
    """Create a file and back-date its mtime by `age_seconds`."""
    path = directory / name
    path.write_text("x", encoding="utf-8")
    stamp = time.time() - age_seconds
    os.utime(path, (stamp, stamp))
    return path


# --------------------------------------------------------------------------
# Policy: age rule, count rule, combination
# --------------------------------------------------------------------------

def test_age_rule_deletes_old_keeps_recent(tmp_path):
    old = _make(tmp_path, "old.csv", 10 * 86400)
    recent = _make(tmp_path, "recent.csv", 86400)  # 1 day
    deleted = export.prune_export_dir(tmp_path, retention_days=7, keep_last=0, interval_min=0)
    assert deleted == 1
    assert not old.exists()
    assert recent.exists()


def test_count_rule_keeps_only_newest(tmp_path):
    # Five files, all past the grace window, distinct mtimes.
    files = [_make(tmp_path, f"f{i}.csv", 3600 + i * 60) for i in range(5)]  # f0 newest
    deleted = export.prune_export_dir(tmp_path, retention_days=0, keep_last=2, interval_min=0)
    assert deleted == 3
    assert files[0].exists() and files[1].exists()  # two newest survive
    assert not any(f.exists() for f in files[2:])


def test_combined_age_or_count(tmp_path):
    recent = [_make(tmp_path, f"r{i}.csv", 3600 + i * 60) for i in range(4)]  # r0 newest
    old = _make(tmp_path, "old.csv", 10 * 86400)
    deleted = export.prune_export_dir(tmp_path, retention_days=7, keep_last=2, interval_min=0)
    # old deleted by age; r2,r3 deleted by count; r0,r1 survive.
    assert deleted == 3
    assert not old.exists()
    assert recent[0].exists() and recent[1].exists()
    assert not recent[2].exists() and not recent[3].exists()


# --------------------------------------------------------------------------
# Safety: grace window, extension filter, race, disabled
# --------------------------------------------------------------------------

def test_grace_window_protects_fresh_files(tmp_path):
    fresh = [_make(tmp_path, f"n{i}.csv", 1) for i in range(3)]  # 1s old
    deleted = export.prune_export_dir(tmp_path, retention_days=0, keep_last=1, interval_min=0)
    assert deleted == 0
    assert all(f.exists() for f in fresh)


def test_extension_filter_leaves_non_exports_and_marker(tmp_path):
    old_csv = _make(tmp_path, "data.csv", 10 * 86400)
    old_txt = _make(tmp_path, "notes.txt", 10 * 86400)
    marker = _make(tmp_path, export._PRUNE_STAMP_NAME, 10 * 86400)
    deleted = export.prune_export_dir(tmp_path, retention_days=7, keep_last=0, interval_min=0)
    assert deleted == 1
    assert not old_csv.exists()
    assert old_txt.exists()
    assert marker.exists()


def test_unlink_race_does_not_raise(tmp_path, monkeypatch):
    _make(tmp_path, "old.csv", 10 * 86400)

    def _boom(self):  # noqa: ANN001
        raise FileNotFoundError("already gone")

    monkeypatch.setattr(Path, "unlink", _boom)
    # Must not raise, and reports nothing deleted.
    assert export.prune_export_dir(tmp_path, retention_days=7, keep_last=0, interval_min=0) == 0


def test_disabled_when_both_rules_zero(tmp_path):
    old = _make(tmp_path, "old.csv", 10 * 86400)
    assert export.prune_export_dir(tmp_path, retention_days=0, keep_last=0, interval_min=0) == 0
    assert old.exists()


# --------------------------------------------------------------------------
# Rate limiting
# --------------------------------------------------------------------------

def test_second_call_within_interval_is_noop(tmp_path):
    _make(tmp_path, "a.csv", 10 * 86400)
    first = export.prune_export_dir(tmp_path, retention_days=7, keep_last=0, interval_min=60)
    _make(tmp_path, "b.csv", 10 * 86400)
    second = export.prune_export_dir(tmp_path, retention_days=7, keep_last=0, interval_min=60)
    assert first == 1
    assert second == 0  # rate-limited by the fresh .prune_stamp
    assert (tmp_path / "b.csv").exists()


# --------------------------------------------------------------------------
# Async wrapper + non-blocking lock
# --------------------------------------------------------------------------

def test_async_runs_in_background(monkeypatch):
    done = threading.Event()
    monkeypatch.setattr(export, "prune_export_dir", lambda: done.set())
    export.prune_export_dir_async()
    assert done.wait(timeout=2.0)


def test_async_skips_when_already_running(monkeypatch):
    called = threading.Event()
    monkeypatch.setattr(export, "prune_export_dir", lambda: called.set())
    assert export._prune_lock.acquire(blocking=False)  # simulate an in-progress prune
    try:
        export.prune_export_dir_async()
        assert not called.wait(timeout=0.3)  # no second thread spawned
    finally:
        export._prune_lock.release()


# --------------------------------------------------------------------------
# Wiring: both export services schedule a prune
# --------------------------------------------------------------------------

def test_export_table_schedules_prune(monkeypatch, tmp_path):
    monkeypatch.setenv("DB_INTROSPECTION_EXPORT_DIR", str(tmp_path))
    calls: list[int] = []
    monkeypatch.setattr(intro_mod, "prune_export_dir_async", lambda: calls.append(1))
    service = IntrospectionService(
        adapter=BaseStubAdapter(), settings=make_settings(allowed_schemas=("s",)))
    result = service.export_table(
        table="t", schema="s", columns=None, order_by=None,
        filename=None, output_format="csv", max_rows=None,
    )
    assert result["ok"] is True
    assert calls == [1]


def test_export_select_schedules_prune(monkeypatch, tmp_path):
    monkeypatch.setenv("DB_INTROSPECTION_EXPORT_DIR", str(tmp_path))
    calls: list[int] = []
    monkeypatch.setattr(sel_mod, "prune_export_dir_async", lambda: calls.append(1))
    service = SelectService(adapter=BaseStubAdapter(), settings=make_settings())
    result = service.export_select(
        sql_query="SELECT 1", filename=None, output_format="csv",
        timeout_ms=None, max_rows=None,
    )
    assert result["ok"] is True
    assert calls == [1]
