"""Harness tests for FhirLakeflowConnect (LakeflowConnectTests).

Default runs use the in-process simulator (source_simulator/specs/fhir/), which
is shaped like public HAPI R4. A live run validates that spec against HAPI and
writes cassettes/*.validation.json and *.coverage.json next to this file:

    CONNECTOR_TEST_MODE=live \\
    CONNECTOR_TEST_CONFIG_JSON='{"base_url": "https://hapi.fhir.org/baseR4",
                                 "auth_type": "none"}' \\
    .venv/bin/python -m pytest tests/unit/sources/fhir/test_fhir_lakeflow_connect.py

Budget about 30 minutes. The run drains all 14 resource types once, and the
two bulk classes run real $export jobs (HAPI answers each status poll with
Retry-After: 120). The classes below exist so the run touches every endpoint
in the spec: REST search and paging, bulk export at system and Patient scope,
type-level _history, and SMART discovery.
"""

from datetime import datetime, timedelta, timezone

import pytest

from databricks.labs.community_connector.source_simulator import MODE_SIMULATE
from databricks.labs.community_connector.sources.fhir.fhir import FhirLakeflowConnect
from databricks.labs.community_connector.sources.fhir.fhir_utils import discover_token_url
from tests.unit.sources.test_suite import (
    LakeflowConnectTests,
    _resolve_env_mode_for_simulator,
)


class _FhirTests(LakeflowConnectTests):
    """Shared settings; `extra_options` are layered over the stand-in or live config."""

    connector_class = FhirLakeflowConnect
    simulator_source = "fhir"
    sample_records = 5  # HAPI FHIR is a public server, be considerate.
    replay_config = {
        "base_url": "https://simulator-fhir.example.com/baseR4",
        "auth_type": "none",
    }
    extra_options: dict = {}

    @classmethod
    def _load_config(cls) -> dict:
        return {**super()._load_config(), **cls.extra_options}

    def _simulated(self) -> bool:
        return _resolve_env_mode_for_simulator(self.simulator_source) == MODE_SIMULATE


class TestFhirConnector(_FhirTests):
    """REST search over the 14 default resource types."""

    # The simulator corpus holds only Patient and Goal; the other types are
    # empty there (they are populated on HAPI).
    allow_empty_first_read = frozenset(
        {
            "AllergyIntolerance",
            "CarePlan",
            "Condition",
            "Coverage",
            "Device",
            "DiagnosticReport",
            "DocumentReference",
            "Encounter",
            "Immunization",
            "MedicationRequest",
            "Observation",
            "Procedure",
        }
    )

    def test_smart_discovery_reflects_the_server(self):
        """HAPI publishes no SMART configuration, so discovery reports the 404."""
        with pytest.raises(RuntimeError, match="404"):
            discover_token_url(self.config["base_url"])


class TestFhirConnectorBulk(_FhirTests):
    """Bulk $export at system scope. Goal keeps live exports small (118 on HAPI)."""

    extra_options = {
        "resource_types": "Goal",
        "bulk_export_enabled": "true",
        "bulk_export_poll_interval": "1",
    }


class TestFhirConnectorBulkPatientScope(_FhirTests):
    """Bulk $export at Patient-compartment scope (GET Patient/$export)."""

    extra_options = {
        "resource_types": "Goal",
        "bulk_export_enabled": "true",
        "bulk_export_scope": "patient",
        "bulk_export_poll_interval": "1",
    }


class TestFhirConnectorDeletes(_FhirTests):
    """track_deletes=true: cdc_with_deletes, deletions read from _history."""

    extra_options = {"resource_types": "Goal", "track_deletes": "true"}

    def test_history_deletes_are_read(self):
        """A later trigger reads GET Goal/_history and emits well-formed deletes.

        The harness's own deletes test only makes a first call, which by design
        reads nothing; this drives the second-trigger path that hits _history.
        """
        since = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
        records, _ = self.connector.read_table_deletes(
            "Goal", {"cursor": since.replace("+00:00", "Z"), "run_ts": "previous"}, {}
        )
        records = list(records)
        assert all(
            r["resource_id"] and r["last_updated"] and r["resource"] is None for r in records
        )
        if self._simulated():
            # corpus/history.json: goal-retired's newest entry is a DELETE.
            assert [r["resource_id"] for r in records] == ["goal-retired"]
