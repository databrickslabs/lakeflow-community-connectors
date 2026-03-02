"""
Auth verification test for DICOMwebLakeflowConnect.

Connects to the real DICOMweb server configured in dev_config.json,
calls list_tables() to confirm reachability, then calls read_table()
on "studies" to confirm that records can be fetched end-to-end.

Run with:
    .venv/bin/python -m pytest tests/unit/sources/dicomweb/test_auth_verify.py -v
"""

from __future__ import annotations

import pathlib

from databricks.labs.community_connector.sources.dicomweb.dicomweb import (
    DICOMwebLakeflowConnect,
)
from tests.unit.sources.test_utils import load_config

CONFIG_PATH = pathlib.Path(__file__).parent / "configs" / "dev_config.json"


def _make_connector() -> DICOMwebLakeflowConnect:
    """Load dev_config.json and return an instantiated connector."""
    config = load_config(CONFIG_PATH)
    # auth_type defaults to "none" when not present in the config file
    return DICOMwebLakeflowConnect(config)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_auth_list_tables():
    """Connector can be instantiated and list_tables() returns expected table names."""
    connector = _make_connector()
    tables = connector.list_tables()

    print(f"\n[auth-check] list_tables() returned: {tables}")

    assert isinstance(tables, list), "list_tables() must return a list"
    assert len(tables) > 0, "list_tables() returned an empty list — no tables available"
    assert "studies" in tables, f"Expected 'studies' in tables, got: {tables}"


def test_auth_read_studies():
    """
    read_table('studies') fetches at least one record from the live server.

    Uses page_size=5 to keep the request small and avoid hammering the demo
    server while still verifying end-to-end connectivity.
    """
    connector = _make_connector()

    records_iter, next_offset = connector.read_table(
        table_name="studies",
        start_offset={},
        table_options={"page_size": "5"},
    )

    # Materialise only the first batch — stop after 5 records to stay light.
    records: list[dict] = []
    for record in records_iter:
        records.append(record)
        if len(records) >= 5:
            break

    print(f"\n[auth-check] read_table('studies') returned {len(records)} record(s)")
    if records:
        sample_keys = list(records[0].keys())
        print(f"[auth-check] Sample record keys: {sample_keys}")
        print(f"[auth-check] StudyInstanceUID: {records[0].get('StudyInstanceUID')}")
        print(f"[auth-check] StudyDate:        {records[0].get('StudyDate')}")
        print(f"[auth-check] connection_name:  {records[0].get('connection_name')}")

    assert len(records) > 0, (
        "read_table('studies') returned no records. "
        "Check that the DICOMweb server is reachable and the credentials in dev_config.json are correct."
    )

    # Every record must carry a StudyInstanceUID (the primary key)
    for rec in records:
        assert rec.get("StudyInstanceUID"), (
            f"Record is missing StudyInstanceUID: {rec}"
        )

    # next_offset must be a dict with at least a study_date key
    assert isinstance(next_offset, dict), f"Expected dict next_offset, got {type(next_offset)}"
    assert "study_date" in next_offset, f"next_offset missing 'study_date': {next_offset}"

    print(f"[auth-check] next_offset: {next_offset}")
    print("[auth-check] Auth verification PASSED.")
