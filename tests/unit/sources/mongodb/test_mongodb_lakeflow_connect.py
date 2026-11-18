"""Generic LakeflowConnect conformance suite, default (simulate) mode.

MongoDB itself is not HTTP, so the source simulator stands in for the
connector's HTTP surface only: the Databricks OAuth and Unity Catalog
temporary-service-credentials endpoints used by
``auth_mode=aws_service_credential`` (spec:
``source_simulator/specs/mongodb/endpoints.yaml``). The MongoDB side is the
in-process fake in ``fake_mongo.py``. Every client the connector builds
first fetches temporary AWS keys through the simulated endpoints, so the
suite exercises the authentication path as well.

Live mode (``CONNECTOR_TEST_MODE=live`` with ``CONNECTOR_TEST_CONFIG_JSON``
holding the same keys as ``replay_config``, for a real workspace) calls the
workspace for the two HTTP endpoints and writes the validation and coverage
reports under ``cassettes/``; the MongoDB side stays the fake. The raw
cassette, which would hold the live token and keys, is deleted at teardown
(in record mode too).
"""

import os

import pytest
from bson import ObjectId

from databricks.labs.community_connector.sources.mongodb import mongodb_auth
from databricks.labs.community_connector.sources.mongodb.mongodb import MongodbLakeflowConnect
from tests.unit.sources.mongodb.fake_mongo import FakeMongoClient
from tests.unit.sources.test_suite import LakeflowConnectTests


def _create_with_auth(self):
    # Same as MongoClientFactory.create, but returns the fake client: the
    # keyword arguments (including temporary AWS keys fetched over HTTP)
    # are still built, so the auth path runs through the simulator.
    kwargs = self.build_kwargs()
    if self.auth_mode == "aws_service_credential":
        assert kwargs["authMechanism"] == "MONGODB-AWS" and kwargs["username"]
    return FakeMongoClient(self.uri)


def _mode() -> str:
    return os.environ.get("CONNECTOR_TEST_MODE", "").strip().lower()


class TestMongodbConnector(LakeflowConnectTests):
    connector_class = MongodbLakeflowConnect
    simulator_source = "mongodb"
    sample_records = 50
    replay_config = {
        "connection_uri": "fake://lakeflow-suite",
        "database": "suite",
        "auth_mode": "aws_service_credential",
        "aws_service_credential": "simulated_atlas_role",
        "databricks_host": "https://simulated.cloud.databricks.com",
        "databricks_client_id": "00000000-0000-0000-0000-000000000000",
        "databricks_client_secret": "simulator-fake-client-secret",
        "aws_refresh_margin_seconds": "600",
        "unicode_decode_error_handler": "strict",
    }
    table_configs = {
        t: {"max_records_per_batch": "20", "snapshot_batch_size": "20"}
        for t in ("注文", "events")
    }

    @classmethod
    def setup_class(cls):
        has_config = (os.environ.get("CONNECTOR_TEST_CONFIG_JSON")
                      or os.environ.get("CONNECTOR_TEST_CONFIG_PATH"))
        if _mode() in ("live", "record") and not has_config:
            pytest.skip("live/record mode needs a workspace config (CONNECTOR_TEST_CONFIG_JSON)")
        orig_create = mongodb_auth.MongoClientFactory.create
        mongodb_auth.MongoClientFactory.create = _create_with_auth
        try:
            cls.config = cls._load_config()
            db = FakeMongoClient(cls.config["connection_uri"])[cls.config["database"]]
            if not db.list_collection_names_for_test():
                db.create_collection("注文").insert_many(
                    [{"_id": ObjectId(), "顧客": f"顧客{i}🍣", "金額": i} for i in range(30)])
                db.create_collection("events").insert_many(
                    [{"_id": f"evt-{i}"} for i in range(30)])
            super().setup_class()
        except BaseException:
            # pytest does not run teardown_class after a failed setup_class.
            mongodb_auth.MongoClientFactory.create = orig_create
            raise
        cls._orig_create = orig_create

    @classmethod
    def teardown_class(cls):
        if not hasattr(cls, "_orig_create"):
            return
        try:
            super().teardown_class()
        finally:
            mongodb_auth.MongoClientFactory.create = cls._orig_create
            # Live and record runs store the live responses in the raw
            # cassette, and here those are an OAuth token and temporary AWS
            # keys. Only the validation and coverage reports are kept as
            # evidence; replaying this suite needs the spec and corpus, not
            # the cassette.
            if _mode() in ("live", "record"):
                cls._cassette_path().unlink(missing_ok=True)
