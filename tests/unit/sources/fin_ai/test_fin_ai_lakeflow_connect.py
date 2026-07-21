"""Tests for the Fin.ai (Intercom) LakeflowConnect connector.

Runs against the in-process source simulator described by
``source_simulator/specs/fin_ai/``. Fin.ai is an Intercom REST API
connector, so the simulator stands in for ``api.intercom.io``:

  * ``POST /conversations/search`` / ``/contacts/search`` / ``/tickets/search``
    — the incremental Search-API tables (custom ``search`` handler; the
    ``updated_at`` filter, sort and ``starting_after`` pagination live in a
    JSON POST body). These are the three partitioned (``SupportsPartitionedStream``)
    tables, exercised by the partition suite.
  * ``GET /companies/scroll`` — the companies snapshot via the Scroll API
    (custom ``companies_scroll`` handler with an opaque ``scroll_param``).
  * ``GET /admins`` / ``/tags`` / ``/segments`` / ``/data_attributes`` /
    ``/teams`` — single-page GET snapshots (declarative).

Stand-in credentials below are values of the right shape; the simulator
does not validate them (auth headers — Bearer token, Intercom-Version —
are ignored).
"""

from __future__ import annotations

from databricks.labs.community_connector.sources.fin_ai.fin_ai import (
    FinAiLakeflowConnect,
)
from tests.unit.sources.test_partition_suite import (
    SupportsPartitionedStreamTests,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestFinAiConnector(LakeflowConnectTests, SupportsPartitionedStreamTests):
    connector_class = FinAiLakeflowConnect
    simulator_source = "fin_ai"

    # Stand-in credentials — the simulator never validates these; any
    # values of the right shape work. ``access_token`` is the only required
    # option (an Intercom private-app Access Token); region defaults to
    # ``us`` -> ``https://api.intercom.io``.
    replay_config = {
        "access_token": "simulator-fake-access-token",
        "region": "us",
    }
