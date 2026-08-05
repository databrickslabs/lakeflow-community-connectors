"""Tests for the OData LakeflowConnect connector.

Two layers:

* The class-based ``TestODataConnector`` runs the shared
  ``LakeflowConnectTests`` contract suite against the simulator at
  ``source_simulator/specs/odata/`` (Northwind-shaped Customers + Orders).
  This is what CI runs.

* The module-level ``@responses.activate`` tests below exercise narrow
  invariants of the connector that the contract suite doesn't cover:
  literal escaping, ``@odata.nextLink`` resolution edge cases, boundary
  trim shapes, auth wiring, and multi-schema disambiguation. They mock
  HTTP with ``responses`` and run independently of the simulator.
"""


import json
import logging
import os
import re
import time

import pytest
import requests
import responses

from databricks.labs.community_connector.sources.odata import ODataLakeflowConnect
from databricks.labs.community_connector.sources.odata.odata import _odata_literal
from pyspark.sql.types import DecimalType, IntegerType, StringType, TimestampType
from tests.unit.sources.test_suite import LakeflowConnectTests
from tests.unit.sources.test_partition_suite import SupportsPartitionedStreamTests




class TestODataConnector(LakeflowConnectTests, SupportsPartitionedStreamTests):
    """Contract test suite for the OData connector against the simulator.

    The simulator stands up a Northwind-shaped service at
    ``/odata/`` with a fixed ``$metadata`` document and Customers /
    Orders entity sets seeded from the JSON corpus. Connector reads
    flow through the simulator's custom OData handler (entity_set.py)
    which implements just enough ``$top``/``$skip``/``$filter``/
    ``$orderby``/``@odata.nextLink`` semantics to drive the suite.

    ``SupportsPartitionedStreamTests`` is mounted because the connector
    implements ``SupportsPartitionedStream`` (``PartitionMixin``). Its
    partitioned-table contract tests ``skip`` here — the connector only
    partitions *contained* N+1 snapshot paths (``Parent__Child``), and the
    flat Northwind corpus (Customers/Orders) has no partitionable table — so
    ``test_is_partitioned`` runs against the simulator while the contained
    partitioning behaviour is covered by the bespoke ``test_partition_*``
    tests below (which build nested ``$metadata`` fixtures the flat corpus
    can't express).
    """

    connector_class = ODataLakeflowConnect
    simulator_source = "odata"
    sample_records = 50
    # The simulator never validates these — they only need to satisfy
    # ``__init__`` so a session is built. The actual HTTP traffic is
    # intercepted before it leaves the connector.
    replay_config = {
        "service_url": "https://services.odata.org/V4/Northwind/Northwind.svc/",
        "auth_type": "bearer",
        "token": "simulator-fake-token",
    }
    # Orders is the only CDC-shaped table in the corpus. The cursor
    # field has duplicate values (multiple OrderIDs per OrderDate), so
    # this configuration also exercises the boundary trim.
    table_configs = {
        "Orders": {"cursor_field": "OrderDate"},
    }
