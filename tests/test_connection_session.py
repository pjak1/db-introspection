"""Test the connection-holding mechanism on its own.

This is what extracting `ConnectionSession` out of `DatabaseAdapter` bought:
reentrancy, always-close and recovery-after-error have nothing to do with any
dialect, and can now be exercised without constructing an Oracle/PostgreSQL/SQL
Server adapter. The per-dialect wiring is covered separately in
`test_session_scope.py`.
"""
from __future__ import annotations

import threading

import pytest

from src.adapters.session import ConnectionSession


class _FakeConnection:
    """Records what the policy did to it."""

    def __init__(self, name: str = "conn") -> None:
        self.name = name
        self.begun = 0
        self.ended = 0
        self.recovered = 0
        self.closed = False


class _RecordingPolicy:
    """A session policy that only counts, so the mechanism is what is measured."""

    def __init__(self, begin_error: Exception | None = None,
                 recover_error: Exception | None = None) -> None:
        self._begin_error = begin_error
        self._recover_error = recover_error

    def begin(self, conn: _FakeConnection) -> None:
        conn.begun += 1
        if self._begin_error is not None:
            raise self._begin_error

    def end(self, conn: _FakeConnection) -> None:
        conn.ended += 1
        conn.closed = True

    def recover(self, conn: _FakeConnection) -> None:
        conn.recovered += 1
        if self._recover_error is not None:
            raise self._recover_error


def _session(policy: _RecordingPolicy | None = None) -> tuple[ConnectionSession, list]:
    """Build a session over a counting connection factory."""
    opened: list[_FakeConnection] = []

    def connect() -> _FakeConnection:
        conn = _FakeConnection(f"conn{len(opened)}")
        opened.append(conn)
        return conn

    return ConnectionSession(connect, policy or _RecordingPolicy()), opened


def test_hold_opens_configures_and_closes_one_connection():
    session, opened = _session()

    with session.hold() as conn:
        assert conn is opened[0]
        assert conn.begun == 1
        assert conn.closed is False

    assert len(opened) == 1
    assert opened[0].ended == 1
    assert opened[0].closed is True


def test_nested_hold_borrows_instead_of_opening_a_second_connection():
    """This is the whole point: a fan-out method pays one login, not one per query."""
    session, opened = _session()

    with session.hold() as outer:
        with session.hold() as inner:
            with session.hold() as innermost:
                assert inner is outer
                assert innermost is outer
        # The inner blocks must not have torn the session down.
        assert outer.ended == 0
        assert outer.closed is False

    assert len(opened) == 1
    # Setup ran once, teardown once, at the outermost boundary only.
    assert opened[0].begun == 1
    assert opened[0].ended == 1


def test_connection_is_closed_even_when_the_body_raises():
    session, opened = _session()

    with pytest.raises(ValueError, match="boom"):
        with session.hold():
            raise ValueError("boom")

    assert opened[0].closed is True


def test_connection_is_closed_even_when_the_policy_setup_fails():
    """A failing read-only setup must not leak the connection it was given."""
    session, opened = _session(_RecordingPolicy(begin_error=RuntimeError("no read-only")))

    with pytest.raises(RuntimeError, match="no read-only"):
        with session.hold():
            pytest.fail("the body must not run when begin() failed")

    assert opened[0].ended == 1
    assert opened[0].closed is True


def test_a_failing_nested_query_recovers_the_session_and_reraises():
    """The session outlives the failed query, so it has to be made usable again."""
    session, opened = _session()

    with session.hold() as outer:
        with pytest.raises(ValueError, match="query failed"):
            with session.hold():
                raise ValueError("query failed")
        assert outer.recovered == 1
        # Recovery keeps the session alive for the next query.
        assert outer.closed is False
        with session.hold() as still_usable:
            assert still_usable is outer

    assert opened[0].ended == 1


def test_a_failing_outermost_query_does_not_bother_recovering():
    """Nothing follows it in this session, and end() runs regardless."""
    session, opened = _session()

    with pytest.raises(ValueError):
        with session.hold():
            raise ValueError("query failed")

    assert opened[0].recovered == 0
    assert opened[0].ended == 1


def test_a_failing_recovery_never_masks_the_original_error():
    session, opened = _session(_RecordingPolicy(recover_error=RuntimeError("rollback failed")))

    with session.hold():
        with pytest.raises(ValueError, match="the real problem"):
            with session.hold():
                raise ValueError("the real problem")

    assert opened[0].recovered == 1


def test_base_exceptions_also_trigger_recovery():
    """KeyboardInterrupt/CancelledError must not leave an aborted transaction."""
    session, opened = _session()

    with session.hold() as outer:
        with pytest.raises(KeyboardInterrupt):
            with session.hold():
                raise KeyboardInterrupt
        assert outer.recovered == 1


def test_held_connection_reports_the_current_scope():
    session, _ = _session()

    assert session.held_connection is None
    with session.hold() as conn:
        assert session.held_connection is conn
    assert session.held_connection is None


def test_connect_is_resolved_at_use_time_not_at_construction():
    """Tests swap `open_connection` after building the adapter; that must work."""
    target = {"conn": _FakeConnection("first")}
    session = ConnectionSession(lambda: target["conn"], _RecordingPolicy())

    with session.hold() as conn:
        assert conn.name == "first"

    target["conn"] = _FakeConnection("second")
    with session.hold() as conn:
        assert conn.name == "second"


def test_each_thread_gets_its_own_connection():
    session, opened = _session()
    seen: dict[str, object] = {}
    started = threading.Event()

    def worker() -> None:
        with session.hold() as conn:
            seen["worker"] = conn
            started.set()

    thread = threading.Thread(target=worker)
    with session.hold() as main_conn:
        thread.start()
        started.wait(timeout=5)
        seen["main"] = main_conn

    thread.join(timeout=5)
    assert seen["main"] is not seen["worker"]
    assert len(opened) == 2
