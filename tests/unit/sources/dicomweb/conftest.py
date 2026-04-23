"""
Shared pytest fixtures for the DICOMweb connector test suite.
"""

import json
import pathlib

import pytest

FIXTURES_DIR = pathlib.Path(__file__).parent / "fixtures"


@pytest.fixture()
def studies_response() -> list[dict]:
    return json.loads((FIXTURES_DIR / "studies.json").read_text())


@pytest.fixture()
def series_response() -> list[dict]:
    return json.loads((FIXTURES_DIR / "series.json").read_text())


@pytest.fixture()
def instances_response() -> list[dict]:
    return json.loads((FIXTURES_DIR / "instances.json").read_text())


@pytest.fixture()
def dicomweb_options() -> dict[str, str]:
    return {
        "base_url": "https://dicomweb.example.com",
    }
