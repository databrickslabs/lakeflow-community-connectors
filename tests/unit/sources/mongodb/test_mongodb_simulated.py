"""Offline tests: the connector against the in-process fake in ``fake_mongo.py``.

These run in CI without credentials. They exercise the connector's own logic
(paging, offsets, resume tokens, trigger caps, replay, deletes, multibyte
data) and the generic ``LakeflowConnectTests`` suite. Server behaviour is
covered by ``test_mongodb_live.py`` against a real replica set.
"""

import json
import uuid

import pytest
from bson import Decimal128, ObjectId
from bson.timestamp import Timestamp

from databricks.labs.community_connector.sources.mongodb import mongodb_auth
from databricks.labs.community_connector.sources.mongodb.mongodb import MongodbLakeflowConnect
from databricks.labs.community_connector.sources.mongodb.mongodb_utils import dumps, encode_id
from tests.unit.sources.mongodb.fake_mongo import FakeMongoClient
from tests.unit.sources.test_suite import LakeflowConnectTests

MB_COLLECTION = "顧客🍣コレクション"
MB_TEXT = "日本語・ｶﾀｶﾅ・한국어・中文・👨‍👩‍👧‍👦・é (e\u0301)・\u3000全角"


def _fake_create(self):
    return FakeMongoClient(self.uri)


@pytest.fixture()
def fake(monkeypatch):
    monkeypatch.setattr(mongodb_auth.MongoClientFactory, "create", _fake_create)
    uri = f"fake://{uuid.uuid4().hex}"
    return uri, FakeMongoClient(uri)["db"]


def _connector(uri, **extra):
    return MongodbLakeflowConnect({"connection_uri": uri, "database": "db", **extra})


def drive(connector, table, options, start=None, flow="upsert", max_calls=10_000):
    reader = connector.read_table if flow == "upsert" else connector.read_table_deletes
    rows, offset = [], start or {}
    for _ in range(max_calls):
        records, end = reader(table, offset, options)
        rows.extend(records)
        if end == offset:
            return rows, end
        offset = end
    raise AssertionError("read_table never converged")


def apply_changes(upserts, deletes):
    current, seq = {}, {}
    for row in sorted(upserts + deletes, key=lambda r: r["_cdc_sequence"]):
        key, s = row["_id"], row["_cdc_sequence"]
        if s <= seq.get(key, ""):
            continue
        seq[key] = s
        if row["_cdc_operation"] == "delete":
            current.pop(key, None)
        else:
            current[key] = json.loads(row["document"])
    return current


def expected(coll):
    return {encode_id(d["_id"]): json.loads(dumps(d)) for d in coll.find({})}


def test_snapshot_stream_and_deletes(fake):
    uri, db = fake
    coll = db.create_collection(MB_COLLECTION)
    coll.insert_many([{"_id": ObjectId(), "名前": MB_TEXT}, {"_id": "文字列🍣", "v": 1}, {"_id": 7},
                      {"_id": 7.5, "d": Decimal128("1.10")}, {"_id": {"複合": 1}}]
                     + [{"_id": ObjectId(), "i": i} for i in range(60)])
    options = {"max_records_per_batch": "7", "snapshot_batch_size": "7"}
    conn = _connector(uri)
    snap, offset = drive(conn, MB_COLLECTION, options)
    assert offset["phase"] == "stream" and len({r["_id"] for r in snap}) == 65

    coll.update_one({"_id": "文字列🍣"}, {"$set": {"v": "更新🍣"}})
    coll.delete_one({"_id": 7})
    coll.replace_one({"_id": 7.5}, {"置換": True})
    coll.insert_one({"_id": "新規🆕", "本文": MB_TEXT})
    changes, _ = drive(_connector(uri), MB_COLLECTION, options, start=offset)
    deletes, _ = drive(_connector(uri), MB_COLLECTION, options, flow="delete")
    assert apply_changes(snap + changes, deletes) == expected(coll)
    assert all("\\u" not in r["document"] for r in snap + changes)


def test_changes_during_snapshot(fake):
    uri, db = fake
    coll = db.create_collection("c")
    coll.insert_many([{"_id": i, "v": 0} for i in range(30)])
    options = {"max_records_per_batch": "5", "snapshot_batch_size": "5"}
    conn = _connector(uri)
    rows = []
    _, offset = conn.read_table("c", {}, options)
    records, offset = conn.read_table("c", offset, options)
    rows += list(records)
    coll.update_one({"_id": 1}, {"$set": {"v": 1}})     # already read
    coll.update_one({"_id": 29}, {"$set": {"v": 2}})    # not read yet
    coll.delete_one({"_id": 2})
    coll.insert_one({"_id": -1, "v": "below the cursor"})
    more, _ = drive(conn, "c", options, start=offset)
    deletes, _ = drive(_connector(uri), "c", options, flow="delete")
    assert apply_changes(rows + more, deletes) == expected(coll)


def test_trigger_cap_and_idle(fake):
    uri, db = fake
    coll = db.create_collection("c")
    conn = _connector(uri)
    options = {"start_mode": "latest", "idle_advance_seconds": "1"}
    _, offset = conn.read_table("c", {}, options)
    coll.insert_one({"_id": 1})
    # Pretend the trigger started long ago: the new change is past its cap.
    conn._caps[("db.c", "upsert")] = Timestamp(1, 0)  # pylint: disable=protected-access
    rows, offset2 = drive(conn, "c", options, start=offset)
    assert not rows                       # this trigger ends without it
    rows, _ = drive(conn, "c", options, start=offset2)
    assert [r["_id"] for r in rows] == [encode_id(1)]  # the next one reads it


def test_idle_token_keeps_advancing_across_triggers(fake, monkeypatch):
    uri, db = fake
    db.create_collection("quiet")
    other = db.create_collection("busy")
    options = {"start_mode": "latest", "idle_advance_seconds": "60"}
    clock = [1_000_000]
    from databricks.labs.community_connector.sources.mongodb import mongodb  # pylint: disable=import-outside-toplevel
    monkeypatch.setattr(mongodb.MongodbLakeflowConnect, "_now",
                        staticmethod(lambda: Timestamp(clock[0], 0)))
    _, offset = _connector(uri).read_table("quiet", {}, options)
    tokens = [offset["token"]]
    for _ in range(3):              # three triggers, each a new reader, 2 minutes apart
        other.insert_one({"x": 1})  # the oplog moves; "quiet" does not
        clock[0] += 120
        rows, offset = drive(_connector(uri), "quiet", options, start=offset, max_calls=5)
        assert not rows
        tokens.append(offset["token"])
    assert tokens == sorted(tokens) and len(set(tokens)) == 4


def test_replay_between_offsets(fake):
    uri, db = fake
    coll = db.create_collection("log")
    options = {"ingestion_type": "append", "start_mode": "latest", "max_records_per_batch": "3"}
    conn = _connector(uri)
    _, start = conn.read_table("log", {}, options)
    coll.insert_many([{"_id": i} for i in range(6)])
    records, end = conn.read_table("log", start, options)
    original = list(records)
    coll.insert_many([{"_id": i} for i in range(6, 9)])
    assert list(_connector(uri).read_table_range("log", start, end, options)) == original


def test_replay_fails_instead_of_returning_a_partial_batch(fake, monkeypatch):
    uri, db = fake
    coll = db.create_collection("log")
    options = {"ingestion_type": "append", "start_mode": "latest", "max_records_per_batch": "3"}
    conn = _connector(uri)
    _, start = conn.read_table("log", {}, options)
    coll.insert_many([{"_id": i} for i in range(3)])
    _, end = conn.read_table("log", start, options)
    from databricks.labs.community_connector.sources.mongodb import mongodb  # pylint: disable=import-outside-toplevel
    monkeypatch.setattr(mongodb, "REPLAY_TIMEOUT_SECONDS", 0)
    bad_end = dict(end, token=end["token"][:-2] + "7F")  # an end the stream never reaches
    with pytest.raises(RuntimeError, match="did not reach its end offset"):
        list(_connector(uri).read_table_range("log", start, bad_end, options))


def test_replay_with_history_lost_raises(fake, monkeypatch):
    import pymongo.errors  # pylint: disable=import-outside-toplevel
    from tests.unit.sources.mongodb import fake_mongo  # pylint: disable=import-outside-toplevel

    uri, db = fake
    db.create_collection("c")
    conn = _connector(uri)
    start = {"v": 1, "ns": "db.c", "phase": "stream", "token": None, "start_at": 1, "lookback": True}
    end = {"v": 1, "ns": "db.c", "phase": "stream", "token": "82FFFFFFFF", "at": 0}

    def lost(*_a, **_k):
        raise pymongo.errors.OperationFailure("history lost", code=286)

    monkeypatch.setattr(fake_mongo.FakeCollection, "watch", lost)
    with pytest.raises(RuntimeError, match="no longer in the oplog"):
        list(conn.read_table_range("c", start, end, {}, deletes=True))


def test_append_keeps_every_event(fake):
    uri, db = fake
    coll = db.create_collection("log")
    options = {"ingestion_type": "append", "start_mode": "latest"}
    conn = _connector(uri)
    _, offset = conn.read_table("log", {}, options)
    coll.insert_one({"_id": 1, "v": "一"})
    coll.update_one({"_id": 1}, {"$set": {"v": "二"}})
    coll.delete_one({"_id": 1})
    rows, _ = drive(conn, "log", options, start=offset)
    assert [r["_cdc_operation"] for r in rows] == ["insert", "update", "delete"]


def test_drop_and_unknown_and_views(fake):
    uri, db = fake
    coll = db.create_collection("c")
    db.create_collection("v", viewOn="c")
    conn = _connector(uri)
    with pytest.raises(ValueError, match="does not exist"):
        conn.read_table("missing", {}, {})
    with pytest.raises(ValueError, match="only regular collections"):
        conn.read_table("v", {}, {})
    assert conn.list_tables() == ["c"]
    _, offset = conn.read_table("c", {}, {"start_mode": "latest"})
    coll.drop()
    with pytest.raises(RuntimeError, match="dropped or renamed"):
        drive(conn, "c", {"start_mode": "latest"}, start=offset)


def test_sharded_collection_is_refused(fake, monkeypatch):
    uri, db = fake
    db.create_collection("sharded").insert_one({"_id": 1})
    from tests.unit.sources.mongodb import fake_mongo  # pylint: disable=import-outside-toplevel
    monkeypatch.setattr(fake_mongo.FakeDatabase, "command",
                        lambda self, name, coll: {"sharded": True})
    with pytest.raises(ValueError, match="sharded collection"):
        _connector(uri).read_table("sharded", {}, {})


def test_snapshot_reads_use_majority(fake):
    uri, db = fake
    db.create_collection("c").insert_many([{"_id": i} for i in range(3)])
    drive(_connector(uri), "c", {})
    server = FakeMongoClient(uri)._s  # pylint: disable=protected-access
    assert server.read_concerns and set(server.read_concerns) == {"majority"}


def test_collection_alias(fake):
    uri, db = fake
    db.create_collection(MB_COLLECTION).insert_one({"_id": 1, "v": "別名"})
    rows, _ = drive(_connector(uri), "alias", {"collection": MB_COLLECTION})
    assert rows[0]["_cdc_namespace"] == f"db.{MB_COLLECTION}"


class TestMongodbConnectorSimulated(LakeflowConnectTests):
    """The generic conformance suite against the fake."""

    connector_class = MongodbLakeflowConnect
    sample_records = 50
    _uri = "fake://generic-suite"

    @classmethod
    def setup_class(cls):
        cls._orig_create = mongodb_auth.MongoClientFactory.create
        mongodb_auth.MongoClientFactory.create = _fake_create
        db = FakeMongoClient(cls._uri)["suite"]
        db.create_collection("注文").insert_many(
            [{"_id": ObjectId(), "顧客": f"顧客{i}🍣", "金額": i} for i in range(30)])
        db.create_collection("events").insert_many([{"_id": f"evt-{i}"} for i in range(30)])
        cls.config = {"connection_uri": cls._uri, "database": "suite"}
        cls.table_configs = {t: {"max_records_per_batch": "20", "snapshot_batch_size": "20"}
                             for t in ("注文", "events")}
        super().setup_class()

    @classmethod
    def teardown_class(cls):
        super().teardown_class()
        mongodb_auth.MongoClientFactory.create = cls._orig_create
