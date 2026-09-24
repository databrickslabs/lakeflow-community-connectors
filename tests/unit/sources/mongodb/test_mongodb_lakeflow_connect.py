"""Tests for the MongoDB connector.

This connector talks to MongoDB through the PyMongo driver (not HTTP), so
it has no offline simulator. Run the generic suite in live mode against a
real MongoDB / Atlas cluster by supplying credentials via
``CONNECTOR_TEST_CONFIG_JSON`` or ``CONNECTOR_TEST_CONFIG_PATH``::

    CONNECTOR_TEST_MODE=live \\
      CONNECTOR_TEST_CONFIG_JSON='{"connection_uri": "mongodb://...", "database": "mydb"}' \\
      pytest tests/unit/sources/mongodb/ -v
"""

import hashlib
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from bson import ObjectId, json_util
from bson.timestamp import Timestamp
from pymongo.errors import OperationFailure
from pyspark.sql.types import StringType, TimestampType, VariantType

from databricks.labs.community_connector.sources.mongodb.mongodb import (
    MongoDBLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests

_TABLE = "nested_only"
_OID = ObjectId("507f1f77bcf86cd799439011")
_INIT = datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc)
_EMPTY_OPTS: dict[str, str] = {}
_STREAMING_START = {
    "resume_token": json_util.dumps(
        {"_data": "token-0"}, json_options=json_util.RELAXED_JSON_OPTIONS
    )
}


class TestMongoDBConnector(LakeflowConnectTests):
    connector_class = MongoDBLakeflowConnect
    # No simulator: credentials must be supplied per run via
    # CONNECTOR_TEST_CONFIG_JSON / CONNECTOR_TEST_CONFIG_PATH (live mode).


class _FakeChangeStream:
    """Minimal stand-in for ``pymongo.change_stream.ChangeStream``."""

    def __init__(self, events):
        self._events = list(events)
        self._idx = 0
        self.resume_token = {"_data": "token-0"}

    def try_next(self):
        if self._idx >= len(self._events):
            return None
        event = self._events[self._idx]
        self._idx += 1
        self.resume_token = event.get("_id")
        return event

    def close(self):
        return None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        return False


class _FakeFindCursor:
    """Minimal stand-in for a PyMongo ``find().sort().limit()`` cursor."""

    def __init__(self, docs, query=None):
        docs = list(docs)
        if query and "_id" in query and "$gt" in query["_id"]:
            gt = query["_id"]["$gt"]
            docs = [d for d in docs if d["_id"] > gt]
        self._docs = docs
        self._limit = None

    def sort(self, *args, **kwargs):
        self._docs.sort(key=lambda d: d["_id"])
        return self

    def limit(self, n):
        self._limit = n
        return self

    def batch_size(self, n):
        return self

    def __iter__(self):
        docs = self._docs
        if self._limit is not None:
            docs = docs[: self._limit]
        return iter(docs)


def _token(n: int) -> dict:
    return {"_data": f"token-{n}"}


def _token_json(n: int) -> str:
    return json_util.dumps(_token(n), json_options=json_util.RELAXED_JSON_OPTIONS)


def _ts(seconds_offset: int = -60) -> Timestamp:
    return Timestamp(int(_INIT.timestamp()) + seconds_offset, 1)


def _event(
    operation: str,
    *,
    oid: ObjectId = _OID,
    ts: Timestamp | None = None,
    doc: dict | None = None,
    token_n: int = 1,
) -> dict:
    event = {
        "_id": _token(token_n),
        "operationType": operation,
        "clusterTime": ts if ts is not None else _ts(-60),
        "documentKey": {"_id": oid},
    }
    if doc is not None:
        event["fullDocument"] = doc
    return event


def _connector(events=None, watch_side_effect=None, documents=None):
    connector = MongoDBLakeflowConnect(
        {"connection_uri": "mongodb://localhost:27017", "database": "testdb"}
    )
    connector._init_dt = _INIT
    connector.list_tables = lambda: [_TABLE]

    docs = list(documents or [])
    collection = MagicMock()
    stream = _FakeChangeStream(events or [])
    if watch_side_effect is not None:
        collection.watch.side_effect = watch_side_effect
    else:
        collection.watch.return_value = stream
    collection.find.side_effect = lambda query=None, **_kwargs: _FakeFindCursor(docs, query)
    client = MagicMock()
    client.__getitem__.return_value.__getitem__.return_value = collection
    connector._client = lambda: client
    return connector, collection


class TestMongoDBChangeStream:
    def test_metadata_and_schema(self):
        connector, _ = _connector()
        metadata = connector.read_table_metadata(_TABLE, _EMPTY_OPTS)
        assert metadata["ingestion_type"] == "cdc_with_deletes"
        assert metadata["primary_keys"] == ["_id"]
        assert metadata["cursor_field"] == "event_time"
        assert metadata["track_history_columns"] == ["document_hash"]

        schema = connector.get_table_schema(_TABLE, _EMPTY_OPTS)
        names = schema.fieldNames()
        assert names == ["_id", "document", "event_time", "document_hash"]
        assert isinstance(schema["document"].dataType, VariantType)
        assert schema["document"].nullable is True
        assert isinstance(schema["event_time"].dataType, TimestampType)
        assert isinstance(schema["document_hash"].dataType, StringType)
        assert schema["document_hash"].nullable is True

    def test_removed_options_are_rejected(self):
        connector, _ = _connector()
        for option, value in (
            ("cdc_mode", "change_stream"),
            ("cursor_field", "_id"),
            ("cursor_type", "objectid"),
            ("start_timestamp", "2020-01-01T00:00:00+00:00"),
        ):
            with pytest.raises(ValueError, match="no longer supported"):
                connector.read_table_metadata(_TABLE, {option: value})

    def test_update_same_id_is_emitted(self):
        doc = {"_id": _OID, "name": "updated"}
        connector, collection = _connector([_event("update", doc=doc, token_n=1)])
        records, offset = connector.read_table(_TABLE, _STREAMING_START, _EMPTY_OPTS)
        rows = list(records)
        assert len(rows) == 1
        assert rows[0]["_id"] == str(_OID)
        assert "updated" in rows[0]["document"]
        assert rows[0]["event_time"] is not None
        assert rows[0]["document_hash"] is not None
        assert len(rows[0]["document_hash"]) == 64
        assert offset == {"resume_token": _token_json(1)}
        collection.watch.assert_called_once()
        collection.find.assert_not_called()
        kwargs = collection.watch.call_args.kwargs
        assert kwargs["full_document"] == "updateLookup"
        pipeline = kwargs["pipeline"]
        assert pipeline[0]["$match"]["operationType"]["$in"] == [
            "insert",
            "update",
            "replace",
        ]

    def test_delete_emits_pk_and_event_time(self):
        connector, collection = _connector([_event("delete", token_n=2)])
        records, offset = connector.read_table_deletes(_TABLE, {}, _EMPTY_OPTS)
        rows = list(records)
        assert len(rows) == 1
        assert rows[0]["_id"] == str(_OID)
        assert rows[0].get("document") is None
        assert rows[0].get("document_hash") is None
        assert rows[0]["event_time"] is not None
        assert offset == {"resume_token": _token_json(2)}
        collection.find.assert_not_called()
        kwargs = collection.watch.call_args.kwargs
        assert "full_document" not in kwargs
        assert kwargs["pipeline"][0]["$match"]["operationType"]["$in"] == ["delete"]

    def test_resume_after_is_passed_to_watch(self):
        connector, collection = _connector([_event("insert", doc={"_id": _OID}, token_n=3)])
        start = {"resume_token": _token_json(1)}
        _, offset = connector.read_table(_TABLE, start, _EMPTY_OPTS)
        kwargs = collection.watch.call_args.kwargs
        assert kwargs["resume_after"] == _token(1)
        assert "start_at_operation_time" not in kwargs
        assert offset == {"resume_token": _token_json(3)}
        collection.find.assert_not_called()

    def test_empty_batch_keeps_offset(self):
        start = {"resume_token": _token_json(4)}
        connector, _ = _connector([])
        records, offset = connector.read_table(_TABLE, start, _EMPTY_OPTS)
        assert list(records) == []
        assert offset == start

    def test_events_after_init_cap_are_not_emitted(self):
        in_cap = _event(
            "insert",
            doc={"_id": _OID, "name": "old"},
            ts=_ts(-10),
            token_n=5,
        )
        past_cap = _event(
            "insert",
            doc={"_id": ObjectId("507f191e810c19729de860ea"), "name": "new"},
            ts=_ts(60),
            token_n=6,
        )
        connector, _ = _connector([in_cap, past_cap])
        records, offset = connector.read_table(_TABLE, _STREAMING_START, _EMPTY_OPTS)
        rows = list(records)
        assert len(rows) == 1
        assert rows[0]["_id"] == str(_OID)
        assert offset == {"resume_token": _token_json(5)}

        connector2, _ = _connector([past_cap])
        records2, offset2 = connector2.read_table(_TABLE, offset, _EMPTY_OPTS)
        assert list(records2) == []
        assert offset2 == offset

    def test_null_full_document_on_update_is_skipped(self):
        connector, _ = _connector([_event("update", doc=None, token_n=7)])
        records, offset = connector.read_table(_TABLE, _STREAMING_START, _EMPTY_OPTS)
        assert list(records) == []
        assert offset == {"resume_token": _token_json(7)}

    def test_invalidating_event_raises(self):
        connector, _ = _connector([_event("drop", token_n=8)])
        with pytest.raises(ValueError, match="invalidat"):
            connector.read_table(_TABLE, _STREAMING_START, _EMPTY_OPTS)

    def test_standalone_watch_error(self):
        err = OperationFailure("The $changeStream stage is only supported on replica sets", 40573)
        connector, _ = _connector(watch_side_effect=err)
        with pytest.raises(ValueError, match="replica set"):
            connector.read_table(_TABLE, {}, _EMPTY_OPTS)

    def test_document_hash_is_stable_for_same_bson(self):
        connector, _ = _connector()
        doc = {"_id": _OID, "name": "same", "n": 1}
        first = connector._document_to_record(doc, _INIT)
        later = connector._document_to_record(doc, datetime(2026, 7, 1, tzinfo=timezone.utc))
        payload = json_util.dumps(doc, json_options=json_util.RELAXED_JSON_OPTIONS)
        expected = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        assert first["document"] == payload
        assert first["document_hash"] == expected
        assert later["document_hash"] == expected
        assert (
            first["document_hash"]
            != connector._document_to_record({"_id": _OID, "name": "other"}, _INIT)["document_hash"]
        )


class TestMongoDBChangeStreamBootstrap:
    def test_first_read_captures_token_then_dumps(self):
        oid_a = ObjectId("507f1f77bcf86cd799439011")
        oid_b = ObjectId("507f1f77bcf86cd799439012")
        docs = [
            {"_id": oid_a, "name": "a"},
            {"_id": oid_b, "name": "b"},
        ]
        connector, collection = _connector(documents=docs)
        order = []
        stream = collection.watch.return_value
        orig_find = collection.find.side_effect

        def watch(*_args, **_kwargs):
            order.append("watch")
            return stream

        def find(query=None, **_kwargs):
            order.append("find")
            return orig_find(query)

        collection.watch.side_effect = watch
        collection.find.side_effect = find

        records, offset = connector.read_table(
            _TABLE, {}, {**_EMPTY_OPTS, "max_records_per_batch": "10"}
        )
        rows = list(records)
        assert [r["_id"] for r in rows] == [str(oid_a), str(oid_b)]
        assert all("name" in r["document"] for r in rows)
        assert all(r["event_time"] == _INIT for r in rows)
        assert all(r["document_hash"] for r in rows)
        assert offset == {"resume_token": _token_json(0)}
        assert "phase" not in offset
        assert order[0] == "watch"
        assert "find" in order
        assert order.index("watch") < order.index("find")

    def test_dump_is_paginated_by_id(self):
        oids = [
            ObjectId("507f1f77bcf86cd799439011"),
            ObjectId("507f1f77bcf86cd799439012"),
            ObjectId("507f1f77bcf86cd799439013"),
        ]
        docs = [{"_id": oid, "n": i} for i, oid in enumerate(oids)]
        opts = {"max_records_per_batch": "2"}
        connector, _ = _connector(documents=docs)

        rows1, offset1 = connector.read_table(_TABLE, {}, opts)
        batch1 = list(rows1)
        assert [r["_id"] for r in batch1] == [str(oids[0]), str(oids[1])]
        assert offset1["phase"] == "bootstrap"
        assert offset1["resume_token"] == _token_json(0)
        assert (
            json_util.loads(offset1["snapshot_id"], json_options=json_util.RELAXED_JSON_OPTIONS)
            == oids[1]
        )

        connector2, _ = _connector(documents=docs)
        rows2, offset2 = connector2.read_table(_TABLE, offset1, opts)
        batch2 = list(rows2)
        assert [r["_id"] for r in batch2] == [str(oids[2])]
        assert offset2 == {"resume_token": _token_json(0)}

    def test_deletes_do_not_scan(self):
        docs = [{"_id": _OID, "name": "existing"}]
        connector, collection = _connector([_event("delete", token_n=2)], documents=docs)
        records, _ = connector.read_table_deletes(_TABLE, {}, _EMPTY_OPTS)
        list(records)
        collection.find.assert_not_called()

    def test_second_read_uses_resume_after(self):
        docs = [{"_id": _OID, "name": "seed"}]
        connector, _ = _connector(documents=docs)
        _, bootstrap_offset = connector.read_table(_TABLE, {}, _EMPTY_OPTS)
        assert bootstrap_offset == {"resume_token": _token_json(0)}

        connector2, collection = _connector(
            [_event("update", doc={"_id": _OID, "name": "changed"}, token_n=9)],
            documents=docs,
        )
        records, offset = connector2.read_table(_TABLE, bootstrap_offset, _EMPTY_OPTS)
        rows = list(records)
        assert len(rows) == 1
        assert "changed" in rows[0]["document"]
        kwargs = collection.watch.call_args.kwargs
        assert kwargs["resume_after"] == _token(0)
        assert offset == {"resume_token": _token_json(9)}
        collection.find.assert_not_called()
