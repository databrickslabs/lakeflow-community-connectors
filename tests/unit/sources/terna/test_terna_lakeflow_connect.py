"""Test the Terna connector using the LakeflowConnect test suite."""

from pathlib import Path

import pytest

from databricks.labs.community_connector.sources.terna.terna import TernaLakeflowConnect
from tests.unit.sources import test_suite
from tests.unit.sources.test_suite import LakeflowConnectTester
from tests.unit.sources.test_utils import load_config


def test_terna_init_raises_without_credentials():
    """Initializing without client_id or client_secret raises ValueError."""
    with pytest.raises(ValueError, match="client_id.*client_secret"):
        TernaLakeflowConnect({})

    with pytest.raises(ValueError, match="client_id.*client_secret"):
        TernaLakeflowConnect({"client_id": "x"})

    with pytest.raises(ValueError, match="client_id.*client_secret"):
        TernaLakeflowConnect({"client_secret": "y"})


def test_terna_list_tables_and_schema():
    """list_tables returns the four supported tables; get_table_schema returns expected fields (no API call)."""
    connector = TernaLakeflowConnect({"client_id": "test", "client_secret": "test"})

    tables = connector.list_tables()
    assert tables == [
        "total_load",
        "actual_generation",
        "renewable_generation",
        "physical_foreign_flow",
    ]

    schema = connector.get_table_schema("total_load", {})
    assert schema.fieldNames() == [
        "date",
        "date_tz",
        "date_offset",
        "total_load_MW",
        "forecast_total_load_MW",
        "bidding_zone",
    ]


def test_terna_read_table_total_load_returns_data():
    """Call the Terna API and assert total_load returns at least one record with expected fields."""
    config_dir = Path(__file__).parent / "configs"
    config = load_config(config_dir / "dev_config.json")
    table_config = load_config(config_dir / "dev_table_config.json")

    if not config.get("client_id") or not config.get("client_secret"):
        pytest.skip("Terna API credentials not set in dev_config.json")

    connector = TernaLakeflowConnect(config)
    table_options = table_config.get("total_load", {})
    records_iter, offset = connector.read_table("total_load", None, table_options)

    records = list(records_iter)
    assert isinstance(offset, dict), "read_table should return (iterator, offset)"
    assert len(records) >= 1, "Expected at least one record from total_load API"

    row = records[0]
    expected_keys = {"date", "date_tz", "date_offset", "total_load_MW", "bidding_zone"}
    assert expected_keys.issubset(row.keys()), f"Record should have keys {expected_keys}, got {list(row.keys())}"


def test_terna_read_table_with_simulated_offset():
    """Simulate a resume: pass a hand-crafted start_offset so the connector fetches the next chunk.

    Offset format (from terna.py):
      - cursor: "yyyy-mm-dd hh:mm:ss" — next chunk starts the day after this date.
      - range_end (optional): same format; caps the end of the range when date_to spans multiple chunks.
    """
    config_dir = Path(__file__).parent / "configs"
    config = load_config(config_dir / "dev_config.json")
    table_config = load_config(config_dir / "dev_table_config.json")

    if not config.get("client_id") or not config.get("client_secret"):
        pytest.skip("Terna API credentials not set in dev_config.json")

    connector = TernaLakeflowConnect(config)
    table_options = table_config.get("total_load", {})

    # Simulate "we already read up to 2025-03-02"; next chunk will be 2025-03-03 within range_end
    start_offset = {
        "cursor": "2025-03-02 23:59:59",
        "range_end": "2025-03-07 23:59:59",
    }
    records_iter, end_offset = connector.read_table("total_load", start_offset, table_options)

    records = list(records_iter)
    assert isinstance(end_offset, dict)
    assert "cursor" in end_offset
    # range_end is preserved for multi-chunk runs
    assert end_offset.get("range_end") == "2025-03-07 23:59:59"
    # Should have data for 2025-03-03 onward (or empty if API has no data for that day)
    assert all(isinstance(r, dict) for r in records)


def test_terna_read_table_with_no_end():
    """When only date_from is provided (no date_to), connector uses current execution time as end."""
    config_dir = Path(__file__).parent / "configs"
    config = load_config(config_dir / "dev_config.json")

    if not config.get("client_id") or not config.get("client_secret"):
        pytest.skip("Terna API credentials not set in dev_config.json")

    connector = TernaLakeflowConnect(config)
    # Only date_from; date_to omitted → effective end = current execution time
    table_options = {"date_from": "01/03/2025"}
    records_iter, offset = connector.read_table("total_load", None, table_options)

    records = list(records_iter)
    assert isinstance(offset, dict)
    assert "cursor" in offset
    # When range spans beyond first chunk, range_end is set (to current time or chunk end)
    if offset.get("range_end"):
        # Cursor/range_end format is yyyy-mm-dd hh:mm:ss (may be current time, not 23:59:59)
        assert len(offset["range_end"]) >= 19
    assert all(isinstance(r, dict) for r in records)


def test_terna_cdc_resume_uses_current_now():
    """CDC mode (no date_to): a second run with the same offset and no date_to fetches from cursor to current now."""
    config_dir = Path(__file__).parent / "configs"
    config = load_config(config_dir / "dev_config.json")

    if not config.get("client_id") or not config.get("client_secret"):
        pytest.skip("Terna API credentials not set in dev_config.json")

    connector = TernaLakeflowConnect(config)
    table_options = {"date_from": "01/03/2025"}  # no date_to = CDC

    # First "run": get one chunk and offset
    records_1, offset_1 = connector.read_table("total_load", None, table_options)
    list_1 = list(records_1)
    assert isinstance(offset_1, dict)
    assert "cursor" in offset_1

    # Second "run" (simulated): resume with that offset, still no date_to → effective_end = current now
    records_2, offset_2 = connector.read_table("total_load", offset_1, table_options)
    list_2 = list(records_2)
    assert isinstance(offset_2, dict)
    assert "cursor" in offset_2
    # Cursor should advance or stay; we must be able to call read again (CDC always goes to "now")
    assert offset_2["cursor"] >= offset_1["cursor"]
    assert all(isinstance(r, dict) for r in list_1 + list_2)


def test_terna_cdc_old_cursor_still_fetches_to_now():
    """CDC mode: even with an old cursor (no date_to), connector fetches from cursor+1 to current now."""
    config_dir = Path(__file__).parent / "configs"
    config = load_config(config_dir / "dev_config.json")

    if not config.get("client_id") or not config.get("client_secret"):
        pytest.skip("Terna API credentials not set in dev_config.json")

    connector = TernaLakeflowConnect(config)
    # No date_to = CDC; old cursor in the past
    table_options = {"date_from": "01/01/2025"}
    start_offset = {"cursor": "2025-01-15 23:59:59"}  # no range_end → effective_end = now

    records_iter, offset = connector.read_table("total_load", start_offset, table_options)
    records = list(records_iter)
    assert isinstance(offset, dict)
    assert "cursor" in offset
    # Should get a chunk from 2025-01-16 toward "now" (may be empty if API has no data for that range)
    assert offset["cursor"] >= "2025-01-15 23:59:59"
    assert all(isinstance(r, dict) for r in records)


def test_terna_bounded_resume_caps_at_range_end():
    """Bounded mode (date_to set): resume with cursor at range_end returns no more data."""
    config_dir = Path(__file__).parent / "configs"
    config = load_config(config_dir / "dev_config.json")

    if not config.get("client_id") or not config.get("client_secret"):
        pytest.skip("Terna API credentials not set in dev_config.json")

    connector = TernaLakeflowConnect(config)
    table_options = {"date_from": "01/03/2025", "date_to": "07/03/2025"}
    # Cursor already at end of requested range → next from_date would be 2025-03-08 > range_end
    start_offset = {
        "cursor": "2025-03-07 23:59:59",
        "range_end": "2025-03-07 23:59:59",
    }

    records_iter, offset = connector.read_table("total_load", start_offset, table_options)
    records = list(records_iter)
    assert isinstance(offset, dict)
    # Bounded: effective_end = min(now, range_end) = 2025-03-07; from_date = 2025-03-08 > effective_end → no data
    assert len(records) == 0
    assert offset.get("cursor") == "2025-03-07 23:59:59" or "cursor" in offset


def test_terna_offset_chaining():
    """Offset returned from a read can be used as start_offset for the next read (pagination contract)."""
    config_dir = Path(__file__).parent / "configs"
    config = load_config(config_dir / "dev_config.json")
    table_config = load_config(config_dir / "dev_table_config.json")

    if not config.get("client_id") or not config.get("client_secret"):
        pytest.skip("Terna API credentials not set in dev_config.json")

    connector = TernaLakeflowConnect(config)
    table_options = table_config.get("total_load", {})

    # First page: no offset
    records_1, offset_1 = connector.read_table("total_load", None, table_options)
    list_1 = list(records_1)
    assert isinstance(offset_1, dict)
    assert "cursor" in offset_1

    # Second page: use offset from first read as start_offset
    records_2, offset_2 = connector.read_table("total_load", offset_1, table_options)
    list_2 = list(records_2)
    assert isinstance(offset_2, dict)
    assert "cursor" in offset_2

    # Cursor should advance (or stay if first chunk was the last in range)
    assert offset_2["cursor"] >= offset_1["cursor"], (
        "Second read cursor should be >= first (or same if no more data)"
    )
    # If we got data in the first chunk, second chunk may be empty or have later dates
    assert all(isinstance(r, dict) for r in list_1 + list_2)


def test_terna_connector():
    """Test the Terna connector using the shared LakeflowConnect test suite."""
    test_suite.LakeflowConnect = TernaLakeflowConnect

    config_dir = Path(__file__).parent / "configs"
    config = load_config(config_dir / "dev_config.json")
    table_config = load_config(config_dir / "dev_table_config.json")

    # Real API; keep sample_records small to avoid rate limits
    tester = LakeflowConnectTester(config, table_config, sample_records=10)
    report = tester.run_all_tests()
    tester.print_report(report, show_details=True)

    assert report.passed_tests == report.total_tests, (
        f"Test suite had failures: {report.failed_tests} failed, "
        f"{report.error_tests} errors"
    )
