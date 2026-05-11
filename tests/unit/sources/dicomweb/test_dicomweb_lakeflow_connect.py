"""
Tests for DICOMwebLakeflowConnect.

TestDICOMwebConnector       — real-server tests (requires configs/dev_config.json)
TestDICOMwebConnectorMocked — mock-based suite tests using fixture data

Pure unit tests (parser, schemas, diagnostics, utilities) are in test_dicomweb_libs.py.
"""

from __future__ import annotations

import json
import pathlib
import shutil
import tempfile
from unittest.mock import MagicMock

from databricks.labs.community_connector.sources.dicomweb.dicomweb import (
    DICOMwebLakeflowConnect,
)
from tests.unit.sources.test_partition_suite import SupportsPartitionedStreamTests
from tests.unit.sources.test_suite import LakeflowConnectTests


# ---------------------------------------------------------------------------
# Real-server tests (requires configs/dev_config.json)
# ---------------------------------------------------------------------------


class TestDICOMwebConnector(LakeflowConnectTests, SupportsPartitionedStreamTests):
    connector_class = DICOMwebLakeflowConnect
    simulator_source = "dicomweb"
    sample_records = 10
    replay_config = {"base_url": "https://simulator.example.com"}

    @classmethod
    def setup_class(cls):
        cls._tmp_dir = tempfile.mkdtemp(prefix="dicomweb_test_")
        super().setup_class()
        # Inject temp volume path for WADO-RS download tests
        if "instances" in cls.table_configs:
            cls.table_configs["instances"]["dicom_volume_path"] = cls._tmp_dir

    @classmethod
    def teardown_class(cls):
        shutil.rmtree(cls._tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Mock-based suite tests using fixture data
# ---------------------------------------------------------------------------

FIXTURES_DIR = pathlib.Path(__file__).parent / "fixtures"


def _load_fixture(name: str) -> list[dict]:
    return json.loads((FIXTURES_DIR / name).read_text())


class TestDICOMwebConnectorMocked(LakeflowConnectTests, SupportsPartitionedStreamTests):
    """Runs the full test suite against mocked DICOMweb client responses.

    Uses fixture data from fixtures/ so no real server is needed.
    Dates are set near 'today' so micro-batch convergence tests resolve quickly.
    """

    connector_class = DICOMwebLakeflowConnect
    config = {"base_url": "https://dicomweb.example.com"}
    sample_records = 10000  # We allow more records for mock tests

    @classmethod
    def setup_class(cls):
        cls._tmp_dir = tempfile.mkdtemp(prefix="dicomweb_test_")
        cls.table_configs = {
            "studies": {"starting_date": "20260301", "window_days": "30"},
            "series": {"starting_date": "20260301", "window_days": "30"},
            "instances": {
                "starting_date": "20260301",
                "window_days": "30",
                "num_partitions": "2",
                "fetch_dicom_files": "true",
                "dicom_volume_path": cls._tmp_dir,
                "wado_mode": "full",
                "fetch_metadata": "true",
            },
            "diagnostics": {},
        }
        super().setup_class()
        cls._studies = _load_fixture("studies.json")
        cls._series = _load_fixture("series.json")
        cls._instances = _load_fixture("instances.json")
        cls._install_mocks()

    @classmethod
    def teardown_class(cls):
        shutil.rmtree(cls._tmp_dir, ignore_errors=True)

    @classmethod
    def _install_mocks(cls):
        c = cls.connector._client
        c.query_studies = MagicMock(side_effect=cls._mock_query_studies)
        c.query_series_for_study = MagicMock(return_value=cls._series)
        c.query_instances_for_series = MagicMock(return_value=cls._instances)
        c.retrieve_instance = MagicMock(return_value=b"FAKEDICMDATA")
        c.retrieve_series_metadata = MagicMock(side_effect=cls._mock_series_metadata)
        c.probe_endpoint = MagicMock(
            return_value={
                "status_code": 200,
                "content_type": "application/dicom+json",
                "latency_ms": 10,
                "error": None,
            }
        )

    @classmethod
    def _mock_query_studies(cls, date_range, limit=100, offset=0):
        """Return fixture studies on first page, empty on subsequent pages."""
        if offset == 0:
            return cls._studies[:limit]
        return []

    @classmethod
    def _mock_series_metadata(cls, study_uid, series_uid):
        """Return metadata keyed by SOP UIDs from the instances fixture."""
        return [
            {
                "00080018": inst.get("00080018"),
                "00080060": {"vr": "CS", "Value": ["CT"]},
            }
            for inst in cls._instances
            if inst.get("00080018")
        ]
