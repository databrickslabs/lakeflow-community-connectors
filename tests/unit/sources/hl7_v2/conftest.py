"""Pytest conftest for HL7 v2 tests — shared fixtures."""
from __future__ import annotations

from pathlib import Path

import pytest

SAMPLES_DIR = Path(__file__).parent / "samples"


@pytest.fixture(scope="session")
def sample_files() -> list[str]:
    """List of all .hl7 sample filenames."""
    return sorted(f.name for f in SAMPLES_DIR.glob("*.hl7"))
