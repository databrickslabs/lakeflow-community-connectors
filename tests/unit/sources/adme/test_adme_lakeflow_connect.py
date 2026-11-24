"""Tests for the ADME (OSDU) LakeflowConnect connector.

Runs against the in-process source simulator described by
``source_simulator/specs/adme/``. The connector hits two hosts:

  * ``login.microsoftonline.com/<tenant>/oauth2/token`` — stub Bearer
    token (handler ``oauth_token``).
  * ``<instance_url>/api/search/v2/query_with_cursor`` — OSDU Search
    Service (handler ``query_with_cursor``).

Stand-in credentials below are values of the right shape; the simulator
does not validate them.
"""

from __future__ import annotations

from databricks.labs.community_connector.sources.adme.adme import (
    ADMELakeflowConnect,
)
from tests.unit.sources.test_partition_suite import (
    SupportsPartitionedStreamTests,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestADMEConnector(LakeflowConnectTests, SupportsPartitionedStreamTests):
    connector_class = ADMELakeflowConnect
    simulator_source = "adme"
    sample_records = 50

    # Stand-in credentials. The simulator never validates these — any
    # values of the right shape work. Picked plausible-looking strings so
    # the OAuth handler URL match (which uses ``{tenant_id}``) and the
    # connector's instance-url composition both work.
    replay_config = {
        "tenant_id": "00000000-0000-0000-0000-000000000000",
        "client_id": "11111111-1111-1111-1111-111111111111",
        "client_secret": "simulator-fake-client-secret",
        "instance_url": "https://admetest.energy.azure.com",
        "data_partition_id": "opendes",
    }


_BASE_OPTS = {
    "auth_mode": "static_token",
    "access_token": "fake-token-for-unit-test",
    "base_url": "https://admetest.energy.azure.com",
    "data_partition_id": "opendes",
}


def _make_connector() -> ADMELakeflowConnect:
    return ADMELakeflowConnect(dict(_BASE_OPTS))


def test_resolve_kind_query_default():
    """No override: falls back to the static TABLE_TO_KIND_QUERY entry."""
    from databricks.labs.community_connector.sources.adme.adme_schemas import (
        TABLE_TO_KIND_QUERY,
    )

    connector = _make_connector()
    assert (
        connector._resolve_kind_query("Rock_and_Fluid", {})
        == TABLE_TO_KIND_QUERY["Rock_and_Fluid"]
    )
    assert (
        connector._resolve_kind_query("Wellbore", {})
        == TABLE_TO_KIND_QUERY["Wellbore"]
    )


def test_resolve_kind_query_override_rock_and_fluid():
    """Per-table override via table_options replaces the default kind."""
    connector = _make_connector()
    custom = "osdu:wks:work-product-component--RockSampleAnalysis:*"
    assert (
        connector._resolve_kind_query(
            "Rock_and_Fluid", {"kind_query_rock_and_fluid": custom}
        )
        == custom
    )


def test_resolve_kind_query_override_wellbore():
    connector = _make_connector()
    custom = "osdu:wks:master-data--Wellbore:1.0.0"
    assert (
        connector._resolve_kind_query("Wellbore", {"kind_query_wellbore": custom})
        == custom
    )


def test_resolve_kind_query_blank_override_falls_back():
    """A whitespace-only override is ignored — falls back to the default."""
    from databricks.labs.community_connector.sources.adme.adme_schemas import (
        TABLE_TO_KIND_QUERY,
    )

    connector = _make_connector()
    assert (
        connector._resolve_kind_query(
            "Rock_and_Fluid", {"kind_query_rock_and_fluid": "  "}
        )
        == TABLE_TO_KIND_QUERY["Rock_and_Fluid"]
    )
