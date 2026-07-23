"""Pure-logic unit tests for mongodb_utils (no Mongo, no Spark)."""

import base64
import hashlib
import json
from datetime import datetime, timezone

import pytest
from bson import Binary, Decimal128, ObjectId
from bson.timestamp import Timestamp

from databricks.labs.community_connector.sources.mongodb import mongodb_utils as U


def test_system_db():
    assert U.system_db("admin") and U.system_db("local") and U.system_db("config")
    assert not U.system_db("sales")


def test_redact():
    assert U.redact("mongodb+srv://u:p@host/db") == "mongodb+srv://***:***@host/db"
    assert U.redact("mongodb://host") == "mongodb://host"


def test_parse_options_requires_uri():
    with pytest.raises(ValueError):
        U.parse_options({})
    cfg = U.parse_options(
        {"connection_uri": "mongodb://h", "database": "d", "database_allowlist": "a, b"}
    )
    assert cfg["connection_uri"] == "mongodb://h"
    assert cfg["database"] == "d"
    assert cfg["database_allowlist"] == frozenset({"a", "b"})


def test_resolve_db_collection():
    assert U.resolve_db_collection("orders", {"database": "sales"}, None) == ("sales", "orders")
    assert U.resolve_db_collection("orders", {"namespace": '["sales"]'}, None) == (
        "sales",
        "orders",
    )
    assert U.resolve_db_collection("sales.orders", {}, None) == ("sales", "orders")
    assert U.resolve_db_collection("orders", {}, "defdb") == ("defdb", "orders")
    with pytest.raises(ValueError):
        U.resolve_db_collection("orders", {}, None)


def test_convert_value_scalar_types():
    oid = ObjectId()
    assert U.convert_value(oid) == str(oid)
    assert U.convert_value(Decimal128("1.50")) == "1.50"  # precision preserved (not float)
    assert (
        U.convert_value(datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc))
        == "2026-01-02T03:04:05Z"
    )
    assert U.convert_value(datetime(2026, 1, 2, 3, 4, 5)).endswith("Z")  # naive -> UTC Z
    assert U.convert_value(Binary(b"abc")) == base64.b64encode(b"abc").decode()
    assert U.convert_value(Timestamp(123, 1)) == {"t": 123, "i": 1}
    assert U.convert_value(None) is None and U.convert_value(True) is True


def test_convert_document_nested_is_json_safe():
    oid = ObjectId()
    doc = {
        "_id": oid,
        "items": [{"sku": "A", "price": Decimal128("9.99")}],
        "meta": {"b": Binary(b"x")},
    }
    out = U.convert_document(doc)
    assert out["_id"] == str(oid)
    assert out["items"][0]["price"] == "9.99"
    json.dumps(out)  # must not raise


def test_encode_decode_id():
    oid = ObjectId()
    assert U.encode_id(oid) == {"t": "oid", "v": str(oid)}
    assert U.decode_id({"t": "oid", "v": str(oid)}) == oid
    assert U.encode_id(42) == {"t": "raw", "v": 42}
    assert U.decode_id({"t": "raw", "v": 42}) == 42
    assert U.decode_id(None) is None


def test_row_builders():
    oid = ObjectId()
    r = U.snapshot_row({"_id": oid, "x": 1}, "db", "c")
    assert r[U.COL_ID] == str(oid) and r[U.COL_OP] == U.OP_SNAPSHOT and r[U.COL_DB] == "db"
    assert json.loads(r[U.COL_DOCUMENT])["x"] == 1

    change = {
        "operationType": "insert",
        "documentKey": {"_id": oid},
        "fullDocument": {"_id": oid, "x": 2},
        "clusterTime": Timestamp(5, 1),
    }
    cr = U.change_row(change, "db", "c")
    assert cr[U.COL_OP] == "insert" and json.loads(cr[U.COL_DOCUMENT])["x"] == 2
    assert cr[U.COL_CLUSTER_TIME] == (5 << 32 | 1)

    delete = {
        "operationType": "delete",
        "documentKey": {"_id": oid},
        "clusterTime": Timestamp(6, 1),
    }
    dr = U.delete_row(delete, "db", "c")
    assert dr[U.COL_OP] == U.OP_DELETE and dr[U.COL_DOCUMENT] is None


def test_change_row_delete_uses_preimage():
    oid = ObjectId()
    event = {
        "operationType": "delete",
        "documentKey": {"_id": oid},
        "fullDocumentBeforeChange": {"_id": oid, "x": 7},
        "clusterTime": Timestamp(1, 1),
    }
    row = U.change_row(event, "db", "c")
    assert row[U.COL_OP] == "delete"
    assert json.loads(row[U.COL_DOCUMENT])["x"] == 7  # soft-delete row carries the pre-image


def test_delete_row_with_preimage():
    oid = ObjectId()
    delete = {
        "operationType": "delete",
        "documentKey": {"_id": oid},
        "fullDocumentBeforeChange": {"_id": oid, "x": 9},
        "clusterTime": Timestamp(6, 1),
    }
    dr = U.delete_row(delete, "db", "c")
    assert json.loads(dr[U.COL_DOCUMENT])["x"] == 9


def test_offsets_and_convergence():
    assert U.append_offset({"t": "oid", "v": "x"}) == {
        "phase": "append",
        "last_id": {"t": "oid", "v": "x"},
    }
    assert U.stream_offset({"_data": "z"}, 5) == {
        "phase": "stream",
        "resume_token": {"_data": "z"},
        "cluster_time": 5,
    }
    assert U.is_same_offset({}, None) and U.is_same_offset(None, {})
    assert not U.is_same_offset({"a": 1}, {"a": 2})


def test_bson_ts_to_long():
    assert U.bson_ts_to_long(None) is None
    assert U.bson_ts_to_long(Timestamp(2, 3)) == (2 << 32 | 3)


def test_client_options_defaults_protect_primary():
    o = U.client_options({})
    assert o["readPreference"] == "secondaryPreferred"  # never load the primary
    assert o["retryReads"] is True
    assert o["serverSelectionTimeoutMS"] == 30000
    assert o["socketTimeoutMS"] == 120000  # FINITE (not pymongo's infinite default)
    assert o["maxPoolSize"] == 20
    assert o["heartbeatFrequencyMS"] == 5000
    assert o["appName"] == "databricks-lakeflow-mongodb"
    assert "maxStalenessSeconds" not in o  # not set unless configured


def test_client_options_overrides():
    o = U.client_options(
        {
            "read_preference": "secondary",
            "retry_reads": "false",
            "server_selection_timeout_ms": "5000",
            "read_concern_level": "majority",
            "compressors": "zstd",
            "socket_timeout_ms": "1000",
            "max_staleness_seconds": "120",
            "max_pool_size": "8",
            "tls": "true",
            "tls_ca_file": "/Volumes/c/s/v/atlas-ca.pem",
        }
    )
    assert o["readPreference"] == "secondary"
    assert o["retryReads"] is False
    assert o["serverSelectionTimeoutMS"] == 5000
    assert o["readConcernLevel"] == "majority"
    assert o["compressors"] == "zstd"
    assert o["socketTimeoutMS"] == 1000
    assert o["maxStalenessSeconds"] == 120
    assert o["maxPoolSize"] == 8
    assert o["tls"] is True
    assert o["tlsCAFile"] == "/Volumes/c/s/v/atlas-ca.pem"


def test_client_options_no_maxstaleness_on_primary():
    o = U.client_options({"read_preference": "primary", "max_staleness_seconds": "120"})
    assert "maxStalenessSeconds" not in o  # invalid with primary; must be omitted


def test_parse_pii_none_when_unconfigured():
    assert U.parse_pii({}) is None


def test_apply_pii_drops_and_hashes_nested():
    pii = U.parse_pii(
        {
            "pii_drop_fields": "card.pan, ssn",
            "pii_hash_fields": "email",
            "pii_hash_salt": "s",
        }
    )
    doc = {"ssn": "123", "card": {"pan": "4111", "exp": "12/30"}, "email": "a@b.com", "name": "x"}
    out = U.apply_pii(doc, pii)
    assert "ssn" not in out  # dropped
    assert "pan" not in out["card"] and out["card"]["exp"] == "12/30"  # only nested pan dropped
    assert out["email"] == hashlib.sha256(b"sa@b.com").hexdigest()  # hashed with salt
    assert out["name"] == "x"  # untouched


def test_snapshot_row_applies_pii():
    pii = U.parse_pii({"pii_drop_fields": "secret"})
    row = U.snapshot_row({"_id": ObjectId(), "secret": "x", "ok": 1}, "db", "c", pii=pii)
    doc = json.loads(row[U.COL_DOCUMENT])
    assert "secret" not in doc and doc["ok"] == 1
