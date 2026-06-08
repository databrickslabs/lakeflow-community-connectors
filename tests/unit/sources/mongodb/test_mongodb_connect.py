"""Connector unit tests using mongomock (no live Mongo, no change streams).

Change-stream behaviour (cdc / deletes) is covered in test_mongodb_live.py since
mongomock does not implement watch().
"""

import json
import pickle

import mongomock
import pytest
from bson import Decimal128, ObjectId

from databricks.labs.community_connector.sources.mongodb import MongodbLakeflowConnect
from databricks.labs.community_connector.sources.mongodb import mongodb_utils as U


def make(client, **opts):
    base = {"connection_uri": "mongodb://x"}
    base.update(opts)
    conn = MongodbLakeflowConnect(base)
    conn._client = client
    return conn


@pytest.fixture
def client():
    m = mongomock.MongoClient()
    for i in range(5):
        m["sales"]["orders"].insert_one(
            {"_id": ObjectId(), "n": i, "nested": {"a": i}, "price": Decimal128("1.50")}
        )
    m["sales"]["members"].insert_one({"_id": ObjectId(), "x": 1})
    m["admin"]["audit"].insert_one({"_id": 1})  # system db -> must be filtered
    return m


def test_list_namespaces_filters_system(client):
    ns = make(client).list_namespaces()
    assert ["sales"] in ns
    assert ["admin"] not in ns


def test_list_tables_in_namespace(client):
    tables = make(client).list_tables_in_namespace(["sales"])
    assert "orders" in tables and "members" in tables


def test_list_tables_with_database(client):
    assert set(make(client, database="sales").list_tables()) >= {"orders", "members"}


def test_list_tables_flat_encoding(client):
    assert "sales.orders" in make(client).list_tables()


def test_get_table_schema_deterministic(client):
    c = make(client, database="sales")
    assert c.get_table_schema("orders", {}) == c.get_table_schema("orders", {})


def test_read_table_metadata(client):
    c = make(client, database="sales")
    meta = c.read_table_metadata("orders", {"ingestion_type": "cdc"})
    assert meta["primary_keys"] == ["_id"]
    assert meta["cursor_field"] == U.COL_CLUSTER_TIME
    assert meta["ingestion_type"] == "cdc"
    with pytest.raises(ValueError):
        c.read_table_metadata("orders", {"ingestion_type": "bogus"})


def test_snapshot_reads_all_as_variant(client):
    c = make(client, database="sales")
    recs, off = c.read_table("orders", None, {"ingestion_type": "snapshot"})
    rows = list(recs)
    assert len(rows) == 5 and off == {}
    assert all(r[U.COL_OP] == U.OP_SNAPSHOT for r in rows)
    doc = json.loads(rows[0][U.COL_DOCUMENT])
    assert "nested" in doc and doc["price"] == "1.50"  # Decimal128 preserved as string


def test_append_paginates_and_converges(client):
    c = make(client, database="sales", max_records_per_batch="2")
    seen, off = [], {}
    for _ in range(10):
        recs, new = c.read_table("orders", off, {"ingestion_type": "append"})
        seen += list(recs)
        if U.is_same_offset(new, off):
            break
        off = new
    assert len({r[U.COL_ID] for r in seen}) == 5  # all five, no dupes


def test_picklable_drops_client(client):
    c = make(client, database="sales")
    restored = pickle.loads(pickle.dumps(c))
    assert restored.__dict__.get("_client") is None


def test_hard_delete_reports_cdc_with_deletes(client):
    c = make(client, database="sales")  # default delete_mode=hard
    meta = c.read_table_metadata("orders", {"ingestion_type": "cdc_with_deletes"})
    assert meta["ingestion_type"] == "cdc_with_deletes"  # framework runs the physical-delete flow


def test_soft_delete_reports_plain_cdc(client):
    c = make(client, database="sales", delete_mode="soft")
    meta = c.read_table_metadata("orders", {"ingestion_type": "cdc_with_deletes"})
    assert meta["ingestion_type"] == "cdc"  # deletes ride the stream; no physical deletes


def test_preflight_snapshot_no_warnings(client):
    c = make(client, database="sales")
    assert c.preflight("orders", {"ingestion_type": "snapshot"}) == []


def test_preflight_warns_when_preimages_disabled(client):
    c = make(client, database="sales")
    warnings = c.preflight("orders", {"ingestion_type": "cdc_with_deletes"})
    assert any("changeStreamPreAndPostImages" in w for w in warnings)


def test_preflight_requires_replica_set_for_cdc():
    class _Standalone:
        def __init__(self):
            self.admin = self

        def command(self, *a, **k):
            return {}  # no setName -> standalone, not a replica set

        def list_collection_names(self):
            return ["orders"]

        def __getitem__(self, _):
            return self

    c = MongodbLakeflowConnect({"connection_uri": "mongodb://x", "database": "sales"})
    c._client = _Standalone()
    with pytest.raises(ValueError, match="replica set"):
        c.preflight("orders", {"ingestion_type": "cdc"})


def test_snapshot_applies_pii_through_connector(client):
    c = make(
        client,
        database="sales",
        pii_drop_fields="nested.a",
        pii_hash_fields="n",
        pii_hash_salt="x",
    )
    recs, _ = c.read_table("orders", None, {"ingestion_type": "snapshot"})
    doc = json.loads(list(recs)[0][U.COL_DOCUMENT])
    assert "a" not in doc.get("nested", {})  # nested PII field dropped before landing
    assert len(doc["n"]) == 64  # hashed to a sha256 hex digest
