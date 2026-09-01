from __future__ import annotations

import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any, Protocol


class SessionPolicy(Protocol):
    """Dialect-specific setup and teardown for one held connection.

    Implemented next to the adapter that owns the dialect knowledge, because
    what makes a session read-only differs per engine: Oracle opens a read-only
    transaction, PostgreSQL flips a connection flag, SQL Server has no such mode
    and relies on never committing.
    """

    def begin(self, conn: Any) -> None:
        """Apply the read-only setup, exactly once per session."""

    def end(self, conn: Any) -> None:
        """Tear the session down; must close the connection whatever happens."""

    def recover(self, conn: Any) -> None:
        """Return the session to a state where the next query can still run."""


class ConnectionSession:
    """Hold one connection for the duration of one logical operation.

    Owns the mechanism only — borrow-or-open, reentrancy, always-close, and
    recovery after a failed query. Everything dialect-specific arrives as a
    `SessionPolicy`, so this class never touches SQL and can be tested without a
    database or an adapter.

    Reentrant: a nested `hold()` yields the connection already open, so a method
    that fans out into several queries pays exactly one login instead of one per
    query. The connection is always closed when the outermost block exits — this
    is deliberately not a pool, so the read-only guarantee, the absence of
    leftover session state and the absence of idle connections all keep holding
    for free.
    """

    def __init__(self, connect: Callable[[], Any], policy: SessionPolicy) -> None:
        """Store how to obtain a connection and how to set it up per dialect.

        `connect` is a callable rather than a bound method so it resolves at use
        time; that is what lets a test swap the adapter's `open_connection`
        after the session was created.
        """
        self._connect = connect
        self._policy = policy
        # Thread-local because one instance is shared across every tool call
        # (the registry caches one adapter, and therefore one session, per
        # connection key). Today's dispatch is serial — FastMCP calls sync tool
        # functions inline on the event-loop thread — so this is not strictly
        # required; it is cheap insurance if that ever changes.
        self._local = threading.local()

    @property
    def held_connection(self) -> Any | None:
        """The connection currently held, or None outside a session."""
        return getattr(self._local, "conn", None)

    @contextmanager
    def hold(self) -> Iterator[Any]:
        """Yield the held connection, opening one if this is the outermost call."""
        borrowed = self.held_connection
        if borrowed is not None:
            try:
                yield borrowed
            except BaseException:
                # The session outlives this query, so it must be put back into a
                # usable state — otherwise every later query in the same call
                # fails too (PostgreSQL aborts the whole transaction on error).
                self._safe_recover(borrowed)
                raise
            return

        conn = self._connect()
        self._local.conn = conn
        try:
            self._policy.begin(conn)
            yield conn
        finally:
            self._local.conn = None
            self._policy.end(conn)

    def _safe_recover(self, conn: Any) -> None:
        """Recover the session best-effort; the original error always wins."""
        try:
            self._policy.recover(conn)
        except Exception:
            pass
