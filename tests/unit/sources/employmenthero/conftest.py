# Shared fixtures and constants for Employment Hero unit tests.
# Pytest auto-discovers fixtures here for tests in this directory and subdirectories.

from pathlib import Path
from unittest.mock import patch

import pytest

from tests.unit.sources.test_utils import load_config

# Shared paths (import in test modules if needed: from ...conftest import CONFIGS_DIR, DATA_PATH)
CONFIGS_DIR = Path(__file__).resolve().parent / "configs"
DATA_PATH = CONFIGS_DIR / "dev_table_data.json"


@pytest.fixture(autouse=True)
def _patch_time_sleep():
    """Patch time.sleep in the client so retry tests run instantly."""
    with patch(
        "databricks.labs.community_connector.sources.employmenthero.employmenthero_client.time.sleep",
    ):
        yield


@pytest.fixture
def employees_data() -> dict:
    """Load mock employee data from config JSON (pagination + pages). Shared by mock and tests."""
    return load_config(DATA_PATH)["employees"]
