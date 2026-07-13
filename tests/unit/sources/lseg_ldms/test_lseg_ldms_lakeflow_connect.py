"""Tests for the LSEG LDMS (RDMS) LakeflowConnect connector.

Runs against the in-process source simulator described by
``source_simulator/specs/lseg_ldms/``. The connector exposes three logical
tables:

  * ``curve_values``   — a partitioned stream (SupportsPartitionedStream),
    read via ``POST /api/v1/CurveValuesBatch`` (custom handler).
  * ``curve_metadata`` — snapshot catalog via ``GET /api/v1/Metadata/Search``
    (custom handler).
  * ``tabular_data``   — snapshot provider datasets via
    ``GET /api/v1/TabularData/Data/{DataType}`` (declarative offset paging).

Stand-in credentials below are values of the right shape; the simulator does
not validate them. ``tabular_data`` requires a ``data_type`` table option, so
one is supplied via ``table_configs``.
"""

from __future__ import annotations

from databricks.labs.community_connector.sources.lseg_ldms.lseg_ldms import (
    LSEGLDMSLakeflowConnect,
)
from tests.unit.sources.test_partition_suite import (
    SupportsPartitionedStreamTests,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestLSEGLDMSConnector(LakeflowConnectTests, SupportsPartitionedStreamTests):
    connector_class = LSEGLDMSLakeflowConnect
    simulator_source = "lseg_ldms"

    # Stand-in credentials. The simulator never validates these — any values
    # of the right shape work. ``api_key`` carries its ``apikey-v1 `` prefix
    # verbatim (the connector sends it as the Authorization header as-is).
    replay_config = {
        "base_url": "https://oilprod1.rdms.refinitiv.example.com",
        "api_key": "apikey-v1 simulator-fake-key",
    }

    # ``tabular_data`` requires a provider dataset type; without it the read
    # path raises. JODI is the canonical documented example.
    table_configs = {
        "tabular_data": {"data_type": "JODI"},
    }
