"""Framework conformance test — runs the standard LakeflowConnectTests suite.

MongoDB has no HTTP simulator (pymongo uses raw sockets), so this runs in live
mode against a real replica set. Provide creds and force live mode:

    MONGODB_TEST_URI='mongodb+srv://...' CONNECTOR_TEST_MODE=live \
        uv run python -m pytest tests/unit/sources/mongodb/test_mongodb_conformance.py -v

Expects the elera_demo dataset (standalone/scripts/seed_mongo.py). Uses snapshot
ingestion for all tables — fast and deterministic for the conformance invariants.
"""

import os

import pytest

from databricks.labs.community_connector.sources.mongodb import MongodbLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests

URI = os.environ.get("MONGODB_TEST_URI")


@pytest.mark.skipif(not URI, reason="set MONGODB_TEST_URI (+ CONNECTOR_TEST_MODE=live)")
class TestMongodbConformance(LakeflowConnectTests):
    connector_class = MongodbLakeflowConnect
    sample_records = 50
    config = {
        "connection_uri": URI or "",
        "database": "elera_demo",
        "ingestion_type": "snapshot",
        "max_records_per_batch": "200",
        "cdc_idle_ms": "1500",
    }
    # Snapshot rows never set _cluster_time (it's a CDC cursor); exempt it from
    # the every-column-populated invariant for each ingested collection.
    allow_null_columns = {
        "members": {"_cluster_time"},
        "orders": {"_cluster_time"},
        "order_line_items": {"_cluster_time"},
    }
