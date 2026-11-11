"""Live tests against a real MongoDB replica set.

Run against a local single-node replica set, for example::

    docker run -d --name lcc-mongo-rs -p 27017:27017 mongo:8.0 --replSet rs0 --bind_ip_all
    docker exec lcc-mongo-rs mongosh --eval \
        'rs.initiate({_id:"rs0",members:[{_id:0,host:"localhost:27017"}]})'
    MONGODB_TEST_URI='mongodb://localhost:27017/?replicaSet=rs0&directConnection=true' \
        pytest tests/unit/sources/mongodb/test_mongodb_live.py

The tests create and drop their own database (``lakeflow_mongodb_test_*``).
They are skipped when ``MONGODB_TEST_URI`` is not set.
"""

import json
import os
import threading
import time
import uuid

import pytest
from bson import Decimal128, Int64, ObjectId
from bson.raw_bson import RawBSONDocument

from databricks.labs.community_connector.sources.mongodb.mongodb import MongodbLakeflowConnect
from databricks.labs.community_connector.sources.mongodb.mongodb_utils import dumps, encode_id

URI = os.environ.get("MONGODB_TEST_URI")
pytestmark = pytest.mark.skipif(not URI, reason="MONGODB_TEST_URI is not set")

# Collection and database names with multibyte characters are part of the contract.
MB_COLLECTION = "顧客🍣コレクション"
MB_TEXT = "日本語・ｶﾀｶﾅ・한국어・中文・👨‍👩‍👧‍👦・é (e\u0301)・\u3000全角スペース"


@pytest.fixture()
def db():
    import pymongo  # pylint: disable=import-outside-toplevel

    client = pymongo.MongoClient(URI, tz_aware=True)
    name = f"lakeflow_mongodb_test_{uuid.uuid4().hex[:8]}"
    yield client[name]
    client.drop_database(name)
    client.close()


def _connector(database, **extra):
    return MongodbLakeflowConnect({"connection_uri": URI, "database": database, **extra})


def drive(connector, table, options, start=None, flow="upsert", max_calls=10_000):
    """Emulate the framework: call read_table until the offset stops moving."""
    reader = connector.read_table if flow == "upsert" else connector.read_table_deletes
    rows = []
    offset = start or {}
    for _ in range(max_calls):
        records, end = reader(table, offset, options)
        batch = list(records)
        rows.extend(batch)
        if end == offset:
            return rows, end
        offset = end
    raise AssertionError("read_table never converged (end_offset != start_offset)")


def apply_changes(state, upserts, deletes):
    """What apply_changes(keys=[_id], sequence_by=_cdc_sequence) ends up with."""
    seq = {k: v[0] for k, v in state.items()}
    current = {k: v for k, v in state.items() if v[1] is not None}
    tombstones = {}
    for row in sorted(upserts + deletes, key=lambda r: r["_cdc_sequence"]):
        key, s = row["_id"], row["_cdc_sequence"]
        if s <= max(seq.get(key, ""), tombstones.get(key, "")):
            continue
        if row["_cdc_operation"] == "delete":
            tombstones[key] = s
            current.pop(key, None)
        else:
            current[key] = (s, row["document"])
        seq[key] = s
    return current


def expected_state(collection):
    return {encode_id(d["_id"]): json.loads(dumps(d)) for d in collection.find()}


def actual_state(current):
    return {k: json.loads(v[1]) for k, v in current.items()}


def test_snapshot_then_stream_with_mixed_ids_and_multibyte(db):
    coll = db[MB_COLLECTION]
    docs = [
        {"_id": ObjectId(), "名前": MB_TEXT, "n": 1},
        {"_id": "文字列のID🍣", "値": "二"},
        {"_id": 7, "x": Int64(2**40)},
        {"_id": "7", "x": "文字列の 7 は数値の 7 と別のキー"},
        {"_id": 7.5, "dec": Decimal128("1.10")},
        {"_id": {"複合": 1, "k": "v"}, "nested": {"配列": ["あ", "い", "🍣"]}},
    ] + [{"_id": ObjectId(), "i": i, "text": f"{i}番目の文書"} for i in range(250)]
    coll.insert_many(docs)

    conn = _connector(db.name)
    options = {"max_records_per_batch": "40", "snapshot_batch_size": "40"}
    assert MB_COLLECTION in conn.list_tables()

    snapshot_rows, offset = drive(conn, MB_COLLECTION, options)
    assert offset["phase"] == "stream"
    assert all(r["_cdc_operation"] == "snapshot" for r in snapshot_rows)
    assert len({r["_id"] for r in snapshot_rows}) == len(docs)  # every type paged, no skips

    # Changes after the snapshot, including deletes and multibyte updates.
    coll.update_one({"_id": "文字列のID🍣"}, {"$set": {"値": "更新後🍣"}})
    coll.delete_one({"_id": 7})
    coll.replace_one({"_id": 7.5}, {"置換": True})
    coll.insert_one({"_id": "新規🆕", "本文": MB_TEXT})
    deleted = docs[10]["_id"]
    coll.delete_one({"_id": deleted})

    conn2 = _connector(db.name)  # a new trigger: new instance, same offsets
    upserts, _ = drive(conn2, MB_COLLECTION, options, start=offset)
    del_offset = {}
    deletes, _ = drive(conn2, MB_COLLECTION, options, start=del_offset, flow="delete")

    state = apply_changes({}, snapshot_rows + upserts, deletes)
    assert actual_state(state) == expected_state(coll)
    assert {r["_cdc_namespace"] for r in upserts} == {f"{db.name}.{MB_COLLECTION}"}
    # Multibyte text arrives literally, not as \\u escapes.
    assert any(MB_TEXT in r["document"] for r in upserts)
    assert all("\\u" not in r["document"] for r in upserts)


def test_changes_during_snapshot_are_not_lost(db):
    coll = db["during_snapshot"]
    coll.insert_many([{"_id": i, "v": 0} for i in range(300)])
    conn = _connector(db.name)
    options = {"max_records_per_batch": "50", "snapshot_batch_size": "50"}

    # First page only, then mutate rows both before and after the snapshot cursor.
    records, offset = conn.read_table("during_snapshot", {}, options)
    rows = list(records)
    records, offset = conn.read_table("during_snapshot", offset, options)
    rows += list(records)
    coll.update_many({"_id": {"$lt": 20}}, {"$set": {"v": 1}})   # already snapshotted
    coll.update_many({"_id": {"$gt": 280}}, {"$set": {"v": 2}})  # not yet snapshotted
    coll.delete_many({"_id": {"$in": [5, 290]}})
    coll.insert_one({"_id": -1, "v": "inserted below the cursor"})
    more, offset = drive(conn, "during_snapshot", options, start=offset)
    deletes, _ = drive(_connector(db.name), "during_snapshot", options, flow="delete")

    state = apply_changes({}, rows + more, deletes)
    assert actual_state(state) == expected_state(coll)


def test_available_now_terminates_under_constant_writes(db):
    coll = db["busy"]
    coll.insert_one({"_id": 0})
    conn = _connector(db.name)
    options = {"max_records_per_batch": "10", "start_mode": "latest", "max_await_time_ms": "100"}
    _, offset = conn.read_table("busy", {}, options)
    stop = threading.Event()

    def writer():
        i = 1
        while not stop.is_set():
            coll.insert_one({"_id": i})
            i += 1
            time.sleep(0.005)

    thread = threading.Thread(target=writer, daemon=True)
    thread.start()
    try:
        time.sleep(1)
        started = time.time()
        rows, offset2 = drive(conn, "busy", options, start=offset, max_calls=2000)
        assert time.time() - started < 60
        assert rows, "should read the backlog that existed when the trigger started"
    finally:
        stop.set()
        thread.join()

    # The next triggers (same instance, as in continuous mode) pick up the rest.
    # A poll right after the cap moves forward can be empty, so poll twice.
    rest, offset3 = drive(conn, "busy", options, start=offset2)
    more, _ = drive(conn, "busy", options, start=offset3)
    rest += more
    seen = {r["_id"] for r in rows + rest}
    assert seen == {encode_id(d["_id"]) for d in coll.find({"_id": {"$gt": 0}})}


def test_idle_collection_advances_token(db):
    coll = db["idle"]
    coll.insert_one({"_id": 1})
    other = db["other"]
    conn = _connector(db.name)
    options = {"start_mode": "latest", "idle_advance_seconds": "1"}
    _, offset = conn.read_table("idle", {}, options)
    other.insert_many([{"i": i} for i in range(10)])  # oplog moves, our collection does not
    time.sleep(2)
    _, advanced = drive(conn, "idle", options, start=offset)
    assert advanced["token"] > offset["token"]
    coll.insert_one({"_id": 2, "後": "あと"})
    # Same instance, as in continuous mode: the first poll after the cap moves
    # forward may return nothing; the following one reads the new change.
    rows, end = drive(conn, "idle", options, start=advanced)
    if not rows:
        rows, _ = drive(conn, "idle", options, start=end)
    assert [r["_id"] for r in rows] == [encode_id(2)]


def test_append_mode_keeps_every_event(db):
    coll = db.create_collection("log")
    conn = _connector(db.name)
    options = {"ingestion_type": "append", "start_mode": "latest"}
    assert conn.read_table_metadata("log", options) == {
        "cursor_field": "_cdc_sequence", "ingestion_type": "append"}
    _, offset = conn.read_table("log", {}, options)
    coll.insert_one({"_id": 1, "v": "一"})
    coll.update_one({"_id": 1}, {"$set": {"v": "二"}})
    coll.delete_one({"_id": 1})
    rows, _ = drive(conn, "log", options, start=offset)
    assert [r["_cdc_operation"] for r in rows] == ["insert", "update", "delete"]
    assert [r["_cdc_sequence"] for r in rows] == sorted(r["_cdc_sequence"] for r in rows)
    # The update was looked up after the delete, so its document is gone.
    assert rows[1]["document"] is None and rows[2]["document"] is None


def _raw_doc_with_invalid_utf8() -> bytes:
    """{"_id": 1, "s": "日" + an invalid 0xFF byte}, encoded by hand."""
    text = "日".encode("utf-8") + b"\xff"
    elements = (
        b"\x10_id\x00" + (1).to_bytes(4, "little")
        + b"\x02s\x00" + (len(text) + 1).to_bytes(4, "little") + text + b"\x00"
    )
    return (4 + len(elements) + 1).to_bytes(4, "little") + elements + b"\x00"


def test_invalid_utf8_strict_and_replace(db):
    db["bad_utf8"].insert_one(RawBSONDocument(_raw_doc_with_invalid_utf8()))

    strict = _connector(db.name)
    with pytest.raises(RuntimeError, match="not valid UTF-8"):
        drive(strict, "bad_utf8", {})

    replace = _connector(db.name, unicode_decode_error_handler="replace")
    rows, _ = drive(replace, "bad_utf8", {})
    assert json.loads(rows[0]["document"])["s"] == "日\ufffd"


def test_drop_is_reported(db):
    coll = db["dropped"]
    coll.insert_one({"_id": 1})
    conn = _connector(db.name)
    options = {"start_mode": "latest"}
    _, offset = conn.read_table("dropped", {}, options)
    coll.drop()
    with pytest.raises(RuntimeError, match="dropped or renamed"):
        drive(conn, "dropped", options, start=offset)


def test_pickled_connector_reads(db):
    import pickle  # pylint: disable=import-outside-toplevel

    db["p"].insert_one({"_id": 1, "v": "ピクル"})
    conn = _connector(db.name)
    conn.list_tables()  # opens a client
    clone = pickle.loads(pickle.dumps(conn))
    rows, _ = drive(clone, "p", {})
    assert json.loads(rows[0]["document"])["v"] == "ピクル"


def test_offsets_are_json_serializable(db):
    db[MB_COLLECTION].insert_one({"_id": "キー", "v": 1})
    conn = _connector(db.name)
    _, offset = drive(conn, MB_COLLECTION, {})
    assert json.loads(json.dumps(offset, ensure_ascii=False)) == offset
    assert json.loads(json.dumps(offset)) == offset  # ASCII-escaped form round-trips too


def test_collection_option_aliases_multibyte_name(db):
    db[MB_COLLECTION].insert_one({"_id": 1, "v": "別名"})
    conn = _connector(db.name)
    options = {"collection": MB_COLLECTION}
    rows, _ = drive(conn, "customers_alias", options)
    assert rows[0]["_cdc_namespace"] == f"{db.name}.{MB_COLLECTION}"
    assert conn.get_table_schema("customers_alias", options)["_id"].name == "_id"


def test_unknown_collection_is_an_error(db):
    conn = _connector(db.name)
    with pytest.raises(ValueError, match="does not exist"):
        conn.read_table("まだ無い", {}, {})
    db.create_collection("ビュー元")
    db.create_collection("ビュー2", viewOn="ビュー元", pipeline=[])
    with pytest.raises(ValueError, match="only regular collections"):
        conn.read_table("ビュー2", {}, {})


def test_namespaces(db):
    db[MB_COLLECTION].insert_one({"_id": 1})
    db.create_collection("ビュー", viewOn=MB_COLLECTION, pipeline=[])
    conn = MongodbLakeflowConnect({"connection_uri": URI})
    assert [db.name] in conn.list_namespaces()
    tables = conn.list_tables_in_namespace([db.name])
    assert MB_COLLECTION in tables
    assert "ビュー" not in tables  # views cannot be watched
    # Without a default database, "<db>.<collection>" names a table.
    rows, _ = drive(conn, f"{db.name}.{MB_COLLECTION}", {})
    assert rows[0]["_cdc_namespace"] == f"{db.name}.{MB_COLLECTION}"


def test_replay_returns_exactly_the_original_batch(db):
    coll = db.create_collection("replay")
    conn = _connector(db.name)
    options = {"ingestion_type": "append", "start_mode": "latest", "max_records_per_batch": "3"}
    _, start = conn.read_table("replay", {}, options)
    coll.insert_many([{"_id": i} for i in range(5)])
    records, end = conn.read_table("replay", start, options)
    original = list(records)
    assert len(original) == 3
    coll.insert_many([{"_id": i} for i in range(5, 10)])  # the stream moves on
    replayed = list(_connector(db.name).read_table_range("replay", start, end, options))
    assert replayed == original


def test_replay_of_snapshot_page(db):
    coll = db["replay_snap"]
    coll.insert_many([{"_id": i} for i in range(10)])
    conn = _connector(db.name)
    options = {"max_records_per_batch": "4", "snapshot_batch_size": "4"}
    _, start = conn.read_table("replay_snap", {}, options)
    _, page1_end = conn.read_table("replay_snap", start, options)
    records, page2_end = conn.read_table("replay_snap", page1_end, options)
    original = [r["_id"] for r in records]
    coll.insert_one({"_id": 4.5})  # lands inside the original page's _id range
    replayed = [r["_id"] for r in conn.read_table_range("replay_snap", page1_end, page2_end, options)]
    assert set(original) <= set(replayed)
    assert all(r != encode_id(8) for r in replayed)  # does not run into the next page


def test_replay_of_final_snapshot_page_with_inserts_in_range(db):
    coll = db["replay_final"]
    coll.insert_many([{"_id": i} for i in range(10)])
    conn = _connector(db.name)
    options = {"max_records_per_batch": "4", "snapshot_batch_size": "4"}
    offset, pages = {}, []
    while True:
        records, end = conn.read_table("replay_final", offset, options)
        pages.append((offset, end, [r["_id"] for r in records]))
        if end.get("phase") == "stream":
            break
        offset = end
    start, end, original = pages[-1]
    assert original == [encode_id(8), encode_id(9)]
    coll.insert_many([{"_id": 7 + i / 10} for i in range(1, 5)])  # fills the page before 8 and 9
    replayed = [r["_id"] for r in conn.read_table_range("replay_final", start, end, options)]
    assert encode_id(8) in replayed and encode_id(9) in replayed


def test_when_available_falls_back_to_lookup(db):
    # Post-images are not enabled, so whenAvailable gets no fullDocument for updates.
    coll = db.create_collection("post_images")
    coll.insert_one({"_id": 1, "v": "前"})
    conn = _connector(db.name)
    options = {"start_mode": "latest", "full_document": "whenAvailable"}
    _, offset = conn.read_table("post_images", {}, options)
    coll.update_one({"_id": 1}, {"$set": {"v": "後🍣"}})
    rows, _ = drive(conn, "post_images", options, start=offset)
    assert [json.loads(r["document"])["v"] for r in rows] == ["後🍣"]


def test_idle_advance_does_not_keep_available_now_running(db):
    db.create_collection("quiet")
    busy = db["busy_other"]
    conn = _connector(db.name)
    options = {"start_mode": "latest", "idle_advance_seconds": "1", "max_await_time_ms": "1500"}
    _, offset = conn.read_table("quiet", {}, options)
    stop = threading.Event()

    def writer():
        while not stop.is_set():
            busy.insert_one({"x": 1})
            time.sleep(0.01)

    thread = threading.Thread(target=writer, daemon=True)
    thread.start()
    try:
        time.sleep(1.5)
        rows, _ = drive(conn, "quiet", options, start=offset, max_calls=20)
        assert rows == []
    finally:
        stop.set()
        thread.join()


def test_snapshot_mode_needs_no_change_stream(db, monkeypatch):
    db["snap_only"].insert_many([{"_id": i, "名": "全件"} for i in range(3)])
    conn = _connector(db.name)
    conn.list_tables()

    def no_watch(*_args, **_kwargs):
        raise AssertionError("snapshot mode must not open a change stream")

    from pymongo.collection import Collection  # pylint: disable=import-outside-toplevel
    monkeypatch.setattr(Collection, "watch", no_watch)
    records, offset = conn.read_table("snap_only", None, {"ingestion_type": "snapshot"})
    assert offset is None
    assert len(list(records)) == 3
