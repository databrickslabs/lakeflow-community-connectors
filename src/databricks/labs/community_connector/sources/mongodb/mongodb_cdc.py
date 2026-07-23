"""Change-stream micro-batch helpers for the MongoDB connector.

These functions implement *non-blocking* change-stream tailing that converges
under ``Trigger.AvailableNow``: each call drains immediately-available events
(bounded by ``max_records`` and an idle window) and returns. When no new events
are available the returned offset equals the input offset, which the framework
reads as "caught up". This is the fix for PR #56's blocking ``watch()`` that
never terminated.
"""

from __future__ import annotations

import json
from typing import Iterator

from pymongo.errors import OperationFailure

from databricks.labs.community_connector.sources.mongodb.mongodb_utils import (
    bson_ts_to_long,
    change_row,
    delete_row,
    stream_offset,
)

# Raised when the resume token / startAtOperationTime falls outside the oplog
# window. NOT a resumable error — the driver won't auto-retry; we must re-snapshot.
CHANGE_STREAM_HISTORY_LOST = 286


def current_cluster_time(client) -> int | None:
    """Cluster ``operationTime`` as a sortable long — pins the snapshot->tail boundary."""
    hello = client.admin.command("hello")
    return bson_ts_to_long(hello.get("operationTime"))


def _emit_metric(db: str, coll: str, kind: str, count: int) -> None:
    """Emit a structured per-batch metric line (surfaces in pipeline task logs).

    Row-count/throughput is already in the SDP event log (flow_progress); this adds
    per-collection CDC throughput visibility for ops.
    """
    print(
        json.dumps(
            {"mongo_connector_metric": {"ns": f"{db}.{coll}", "kind": kind, "events": count}}
        )
    )


def _open_stream(
    collection,
    resume_token,
    start_at_long,
    pipeline,
    idle_ms,
    full_document="updateLookup",
    pre_image=False,
):
    kwargs = {"max_await_time_ms": idle_ms}
    if full_document:
        kwargs["full_document"] = full_document
    if pre_image:
        kwargs["full_document_before_change"] = "whenAvailable"
    if resume_token:
        # startAfter is a superset of resumeAfter — it also resumes across an
        # invalidate (collection drop/rename), where resumeAfter would fail.
        kwargs["start_after"] = resume_token
    elif start_at_long:
        from bson.timestamp import Timestamp

        kwargs["start_at_operation_time"] = Timestamp(
            start_at_long >> 32, start_at_long & 0xFFFFFFFF
        )
    return collection.watch(pipeline=pipeline, **kwargs)


def _drain(stream, row_fn, db, coll, max_records, pii=None) -> tuple[list, object]:
    """Collect up to ``max_records`` immediately-available events into bronze rows."""
    rows, token = [], None
    while len(rows) < max_records:
        change = stream.try_next()  # blocks up to max_await_time_ms, then returns None
        if change is None:
            break
        rows.append(row_fn(change, db, coll, pii))
        token = stream.resume_token
    return rows, token


def read_change_batch(
    collection, db, coll, offset, max_records, idle_ms, pii=None, include_deletes=False
) -> tuple[Iterator[dict], dict]:
    """Tail insert/update/replace events; converge when idle.

    With ``include_deletes`` (soft-delete mode), delete events are emitted into the
    same stream as rows with ``_operation=delete`` (carrying the pre-image) instead
    of being physically applied — downstream filters ``_operation != 'delete'`` for
    the live view but retains the deletion as a record.

    On ``ChangeStreamHistoryLost`` (resume token aged out of the oplog), signal a
    re-snapshot by returning an ``initial``-phase offset pinned to a fresh cluster
    time — the connector then re-reads the collection and reopens the tail gaplessly.
    """
    resume_token = offset.get("resume_token")
    t0 = offset.get("t0")
    ops = ["insert", "update", "replace"]
    if include_deletes:
        ops.append("delete")
    pipeline = [{"$match": {"operationType": {"$in": ops}}}]
    try:
        with _open_stream(
            collection, resume_token, t0, pipeline, idle_ms, pre_image=include_deletes
        ) as stream:
            rows, token = _drain(stream, change_row, db, coll, max_records, pii)
    except OperationFailure as e:
        if e.code == CHANGE_STREAM_HISTORY_LOST:
            fresh = current_cluster_time(collection.database.client)
            return iter([]), {"phase": "initial", "last_id": None, "t0": fresh, "resync": True}
        raise
    end = stream_offset(token or resume_token, t0)
    if not rows:
        return iter([]), offset
    _emit_metric(db, coll, "cdc", len(rows))
    return iter(rows), end


def read_delete_batch(
    collection, db, coll, offset, max_records, idle_ms, pii=None
) -> tuple[Iterator[dict], dict]:
    """Tail delete events; emit tombstones (with pre-image when enabled).

    On ``ChangeStreamHistoryLost``, restart the delete tail from a fresh cluster
    time (deletes are forward-only; the paired data re-snapshot reconciles state).
    """
    offset = offset or {}
    resume_token = offset.get("resume_token")
    t0 = offset.get("t0")
    if not resume_token and t0 is None:
        t0 = current_cluster_time(collection.database.client)
    pipeline = [{"$match": {"operationType": "delete"}}]
    try:
        with _open_stream(
            collection, resume_token, t0, pipeline, idle_ms, full_document=None, pre_image=True
        ) as stream:
            rows, token = _drain(stream, delete_row, db, coll, max_records, pii)
    except OperationFailure as e:
        if e.code == CHANGE_STREAM_HISTORY_LOST:
            fresh = current_cluster_time(collection.database.client)
            return iter([]), {"phase": "stream", "resume_token": None, "t0": fresh}
        raise
    end = stream_offset(token or resume_token, t0)
    if not rows:
        return iter([]), (offset or {"phase": "stream", "resume_token": None, "t0": t0})
    _emit_metric(db, coll, "delete", len(rows))
    return iter(rows), end
