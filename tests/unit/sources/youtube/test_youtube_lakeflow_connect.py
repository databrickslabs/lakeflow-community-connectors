"""Lakeflow Connect test suite for the YouTube connector."""

import sys
from pathlib import Path

import pytest

# Ensure project root is on path for CI (e.g. when run from tests/ or repo root).
_ROOT = Path(__file__).resolve().parents[4]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from databricks.labs.community_connector.sources.youtube.youtube import YouTubeLakeflowConnect
from tests.unit.sources import test_suite
from tests.unit.sources.test_suite import LakeflowConnectTester
from tests.unit.sources.test_utils import load_config


def _load_table_config(config_dir: Path) -> dict:
    """Load table config from dev_table_config.json if it exists, else return default for tests."""
    path = config_dir / "dev_table_config.json"
    if path.exists():
        return load_config(path)
    # Default: tables that work with public API (api_key) and need no OAuth.
    return {
        "channels": {"channel_ids": "UC_x5XG1OV2P6uZZ5FSM9Ttw"},
        "video_categories": {"region_code": "US"},
    }


def test_youtube_connector():
    """Run the Lakeflow Connect test suite for the YouTube connector.

    Uses dev_config.json for auth: either api_key (public data) or OAuth
    (client_id, client_secret, refresh_token). Uses dev_table_config.json for
    per-table options; only tables with config are exercised so the suite can
    pass with a subset (e.g. channels, video_categories). Tables that require
    options not in dev_table_config are excluded via a wrapper that restricts
    list_tables to configured tables.
    """
    config_dir = Path(__file__).parent / "configs"
    config_path = config_dir / "dev_config.json"

    if not config_path.exists():
        pytest.skip(
            "YouTube connector tests require dev_config.json with api_key or OAuth "
            "(client_id, client_secret, refresh_token). Create "
            "tests/unit/sources/youtube/configs/dev_config.json to run these tests."
        )

    init_options = load_config(config_path)
    table_config = _load_table_config(config_dir)

    class _YouTubeConnectorWrapper(YouTubeLakeflowConnect):
        """Restricts list_tables to tables that have options in table_config."""

        def list_tables(self):
            all_tables = super().list_tables()
            if not table_config:
                return all_tables
            return [t for t in all_tables if t in table_config]

    test_suite.LakeflowConnect = _YouTubeConnectorWrapper
    tester = LakeflowConnectTester(init_options, table_config, sample_records=10)
    report = tester.run_all_tests()
    tester.print_report(report, show_details=True)

    assert report.passed_tests == report.total_tests, (
        f"Test suite had failures: {report.failed_tests} failed, {report.error_tests} errors"
    )
