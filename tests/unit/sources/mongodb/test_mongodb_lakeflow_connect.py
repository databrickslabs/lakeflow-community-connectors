"""Generic LakeflowConnect conformance suite against a live MongoDB replica set.

MongoDB speaks its own wire protocol, not HTTP, so the HTTP source simulator
does not apply. The suite runs live against the deployment in
``MONGODB_TEST_URI`` (see test_mongodb_live.py for a local replica set) and is
skipped when it is not set. The fixture database is seeded with multibyte
collection names, field names and values.
"""

import os
import uuid

import pytest
from bson import ObjectId

from databricks.labs.community_connector.sources.mongodb.mongodb import MongodbLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests

URI = os.environ.get("MONGODB_TEST_URI")
pytestmark = pytest.mark.skipif(not URI, reason="MONGODB_TEST_URI is not set")
DATABASE = f"lakeflow_mongodb_suite_{uuid.uuid4().hex[:8]}"


class TestMongodbConnector(LakeflowConnectTests):
    connector_class = MongodbLakeflowConnect
    sample_records = 50

    @classmethod
    def setup_class(cls):
        import pymongo  # pylint: disable=import-outside-toplevel

        cls._client = pymongo.MongoClient(URI)
        db = cls._client[DATABASE]
        db["注文"].insert_many(
            [{"_id": ObjectId(), "顧客": f"顧客{i}🍣", "金額": i * 100} for i in range(30)]
        )
        db["events"].insert_many([{"_id": f"evt-{i}", "type": "クリック"} for i in range(30)])
        cls.config = {"connection_uri": URI, "database": DATABASE}
        cls.table_configs = {
            t: {"max_records_per_batch": "20", "snapshot_batch_size": "20"}
            for t in ("注文", "events")
        }
        super().setup_class()

    @classmethod
    def teardown_class(cls):
        super().teardown_class()
        cls._client.drop_database(DATABASE)
        cls._client.close()
