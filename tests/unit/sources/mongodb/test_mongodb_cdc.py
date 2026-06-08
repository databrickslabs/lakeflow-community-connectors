"""Unit tests for change-stream recovery logic (no live Mongo).

Covers the ChangeStreamHistoryLost (error 286) -> auto re-snapshot path using a
fake change stream that raises OperationFailure, so the recovery branch is
exercised deterministically without an oplog.
"""

import pytest
from bson.timestamp import Timestamp
from pymongo.errors import OperationFailure

from databricks.labs.community_connector.sources.mongodb import mongodb_cdc as CDC


class _FakeStream:
    def __init__(self, exc):
        self._exc = exc

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def try_next(self):
        raise self._exc

    @property
    def resume_token(self):
        return None


class _FakeColl:
    """Minimal stand-in: .watch() raises, and admin hello() returns a cluster time."""

    full_name = "db.c"

    def __init__(self, exc):
        self._exc = exc
        self.database = self
        self.client = self
        self.admin = self

    def watch(self, *a, **k):
        return _FakeStream(self._exc)

    def command(self, *a, **k):
        return {"operationType": "noop", "operationTime": Timestamp(1700000000, 1)}


def test_history_lost_triggers_resnapshot():
    coll = _FakeColl(OperationFailure("resume point may no longer be in the oplog", 286))
    rows, off = CDC.read_change_batch(
        coll, "db", "c", {"phase": "stream", "resume_token": {"_data": "z"}, "t0": 1}, 100, 50
    )
    assert list(rows) == []
    assert off["phase"] == "initial"
    assert off["last_id"] is None
    assert off.get("resync") is True
    assert off["t0"] == (1700000000 << 32 | 1)  # fresh cluster time captured


def test_non_history_lost_error_reraises():
    coll = _FakeColl(OperationFailure("transient", 91))  # ShutdownInProgress, not 286
    with pytest.raises(OperationFailure):
        CDC.read_change_batch(
            coll, "db", "c", {"phase": "stream", "resume_token": None, "t0": 1}, 100, 50
        )


def test_delete_batch_restarts_on_history_lost():
    coll = _FakeColl(OperationFailure("history lost", 286))
    rows, off = CDC.read_delete_batch(
        coll, "db", "c", {"phase": "stream", "resume_token": {"_data": "z"}, "t0": 1}, 100, 50
    )
    assert list(rows) == []
    assert off["phase"] == "stream" and off["resume_token"] is None
