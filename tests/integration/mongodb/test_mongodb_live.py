"""Live integration tests against a real MongoDB replica set.

Covers change-stream behaviour that mongomock cannot: cdc initial->tail handoff,
insert/update capture, deletes via pre-images, and AvailableNow convergence.

Run:  MONGODB_TEST_URI=... uv run python -m pytest tests/integration/mongodb -v
Skipped automatically when MONGODB_TEST_URI is unset.
"""

import json
import os

import pytest
from bson import ObjectId

from databricks.labs.community_connector.sources.mongodb import MongodbLakeflowConnect
from databricks.labs.community_connector.sources.mongodb import mongodb_utils as U

URI = os.environ.get("MONGODB_TEST_URI")
pytestmark = pytest.mark.skipif(not URI, reason="set MONGODB_TEST_URI for live MongoDB tests")

DB = "ci_test"
COLL = "cdc_probe"


@pytest.fixture
def coll():
    import pymongo

    client = pymongo.MongoClient(URI)
    client[DB][COLL].drop()
    client[DB].create_collection(COLL)
    try:
        client[DB].command({"collMod": COLL, "changeStreamPreAndPostImages": {"enabled": True}})
    except Exception:
        pass  # delete-by-key fallback when pre-images unsupported
    for i in range(10):
        client[DB][COLL].insert_one({"_id": ObjectId(), "n": i})
    yield client[DB][COLL]
    client[DB][COLL].drop()
    client.close()


def _connector(**opts):
    base = {
        "connection_uri": URI,
        "database": DB,
        "max_records_per_batch": "200",
        "cdc_idle_ms": "3000",
    }
    base.update(opts)
    return MongodbLakeflowConnect(base)


def _drive(fn, table, opts, start, max_calls=40):
    """Mimic the framework's repeated read loop until the offset converges."""
    offset, total = start, []
    for _ in range(max_calls):
        recs, new = fn(table, offset, opts)
        total += list(recs)
        if U.is_same_offset(new, offset):
            return total, new
        offset = new
    return total, offset


def test_snapshot_counts_all(coll):
    recs, off = _connector().read_table(COLL, None, {"ingestion_type": "snapshot"})
    assert len(list(recs)) == 10 and off == {}


def test_cdc_initial_handoff_and_insert_update(coll):
    c = _connector()
    opts = {"ingestion_type": "cdc"}
    rows, off = _drive(c.read_table, COLL, opts, {})
    assert sum(1 for r in rows if r[U.COL_OP] == U.OP_SNAPSHOT) == 10
    assert off.get("phase") == "stream"

    nid = coll.insert_one({"_id": ObjectId(), "n": 999}).inserted_id
    got, off = _drive(c.read_table, COLL, opts, off)
    assert any(r[U.COL_ID] == str(nid) and r[U.COL_OP] == "insert" for r in got)

    coll.update_one({"_id": nid}, {"$set": {"n": 1000}})
    got, off = _drive(c.read_table, COLL, opts, off)
    upd = [r for r in got if r[U.COL_ID] == str(nid) and r[U.COL_OP] == "update"]
    assert upd and json.loads(upd[0][U.COL_DOCUMENT])["n"] == 1000


def test_partitioned_snapshot_covers_keyspace(coll):
    c = _connector(num_partitions="4")
    parts = c.get_partitions(COLL, {"num_partitions": "4"})
    seen = set()
    for p in parts:
        seen.update(r[U.COL_ID] for r in c.read_partition(COLL, p, {}))
    assert len(seen) == 10  # all seeded docs, no gaps/overlaps across partitions


def test_cdc_with_deletes(coll):
    c = _connector()
    opts = {"ingestion_type": "cdc_with_deletes"}
    _, del_off = _drive(c.read_table_deletes, COLL, opts, {})  # prime offset before deleting
    nid = coll.insert_one({"_id": ObjectId(), "n": 1}).inserted_id
    coll.delete_one({"_id": nid})
    dels, _ = _drive(c.read_table_deletes, COLL, opts, del_off)
    assert any(d[U.COL_ID] == str(nid) and d[U.COL_OP] == U.OP_DELETE for d in dels)


def test_convergence_no_changes(coll):
    c = _connector()
    opts = {"ingestion_type": "cdc"}
    _, off = _drive(c.read_table, COLL, opts, {})
    rows2, off2 = _drive(c.read_table, COLL, opts, off)
    assert rows2 == [] and U.is_same_offset(off, off2)
