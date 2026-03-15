"""Test the Terna connector using the LakeflowConnect test suite."""

import re
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from databricks.labs.community_connector.sources.terna.terna import TernaLakeflowConnect
from tests.unit.sources import test_suite
from tests.unit.sources.test_suite import LakeflowConnectTester
from tests.unit.sources.test_utils import load_config

import logging

logger = logging.getLogger(__name__)


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

def test_terna_full_missing_date_from():
    """When date_from is missing, connector raises ValueError (API requires it)."""
    config_dir = Path(__file__).parent / "configs"
    config = load_config(config_dir / "dev_config.json")

    if not config.get("client_id") or not config.get("client_secret"):
        pytest.skip("Terna API credentials not set in dev_config.json")

    connector = TernaLakeflowConnect(config)
    table_options = {"date_to": "29/02/2024", "bidding_zone": "Italy"}
    start_offset = None

    with pytest.raises(ValueError, match="total_load requires 'date_from'"):
        records_iter, offset = connector.read_table("total_load", start_offset, table_options)
        records = list(records_iter)


def test_terna_beyond_five_years_limit():
    """When date_from is missing, connector raises ValueError (API requires it)."""
    config_dir = Path(__file__).parent / "configs"
    config = load_config(config_dir / "dev_config.json")

    if not config.get("client_id") or not config.get("client_secret"):
        pytest.skip("Terna API credentials not set in dev_config.json")

    connector = TernaLakeflowConnect(config)
    table_options = {"date_from": "31/12/2020", "bidding_zone": "Italy"}
    start_offset = None

    with pytest.raises(ValueError, match="Terna connector: 'date_from' must be within the last 5 solar years, not sooner than 01/01/2021"):
        connector.read_table("total_load", start_offset, table_options)


def test_terna_cdc_more_than_sixty_days_multiple_chunks():
    """When date_from is missing, connector raises ValueError (API requires it)."""
    config_dir = Path(__file__).parent / "configs"
    config = load_config(config_dir / "dev_config.json")

    if not config.get("client_id") or not config.get("client_secret"):
        pytest.skip("Terna API credentials not set in dev_config.json")

    connector = TernaLakeflowConnect(config)

    date_to = datetime.now()
    date_from = (date_to - timedelta(days=1)).strftime("%d/%m/%Y")
    table_options = {"date_from": date_from, "bidding_zone": "Italy"}
    start_offset = None

    records_iter, offset = connector.read_table("total_load", start_offset, table_options)
    records = list(records_iter)

    assert offset.get("cursor") == date_to.strftime("%d/%m/%Y")


def test_terna_cdc_more_than_sixty_days_single_chunk():
    """When date_from is missing, connector raises ValueError (API requires it)."""
    config_dir = Path(__file__).parent / "configs"
    config = load_config(config_dir / "dev_config.json")

    if not config.get("client_id") or not config.get("client_secret"):
        pytest.skip("Terna API credentials not set in dev_config.json")

    connector = TernaLakeflowConnect(config)

    date_to = datetime.now()
    date_from = (date_to - timedelta(days=20)).strftime("%d/%m/%Y")
    table_options = {"date_from": date_from, "bidding_zone": "Italy"}
    start_offset = None

    records_iter, offset = connector.read_table("total_load", start_offset, table_options)
    records = list(records_iter)

    assert len(records) == 4 * 24 * 20
    assert offset.get("cursor") == date_to.strftime("%d/%m/%Y")


def test_terna_cdc():
    """When date_from is missing, connector raises ValueError (API requires it)."""
    config_dir = Path(__file__).parent / "configs"
    config = load_config(config_dir / "dev_config.json")

    if not config.get("client_id") or not config.get("client_secret"):
        pytest.skip("Terna API credentials not set in dev_config.json")

    connector = TernaLakeflowConnect(config)

    date_from = (datetime.now() - timedelta(days=20)).strftime("%d/%m/%Y")
    table_options = {"date_from": date_from, "bidding_zone": "Italy"}
    start_offset = None

    records_iter, offset = connector.read_table("total_load", start_offset, table_options)
    records = list(records_iter)

    assert len(records) == 4 * 24 * 20
    assert offset.get("cursor") == datetime.now().strftime("%d/%m/%Y")

def test_terna_cdc_empty_because_same_date():
    """When date_from is missing, connector raises ValueError (API requires it)."""
    config_dir = Path(__file__).parent / "configs"
    config = load_config(config_dir / "dev_config.json")

    if not config.get("client_id") or not config.get("client_secret"):
        pytest.skip("Terna API credentials not set in dev_config.json")

    connector = TernaLakeflowConnect(config)

    date_from = datetime.now().strftime("%d/%m/%Y")

    table_options = {"date_from": date_from, "bidding_zone": "Italy"}
    start_offset = None

    records_iter, offset = connector.read_table("total_load", start_offset, table_options)
    records = list(records_iter)

    logger.info(records)
    logger.info(offset)

    assert len(records) == 0
    assert offset.get("cursor") == date_from

def test_terna_full_start_end_dates():
    config_dir = Path(__file__).parent / "configs"
    config = load_config(config_dir / "dev_config.json")

    if not config.get("client_id") or not config.get("client_secret"):
        pytest.skip("Terna API credentials not set in dev_config.json")

    connector = TernaLakeflowConnect(config)
    table_options = {"date_from": "01/02/2024", "date_to": "29/02/2024", "bidding_zone": "Italy"}
    # Full refresh, cursor is None
    start_offset = None

    records_iter, offset = connector.read_table("total_load", start_offset, table_options)
    records = list(records_iter)
    
    assert isinstance(offset, dict)
    assert len(records) == 4 * 24 * 29 
    assert offset.get("cursor") == "29/02/2024"


def test_terna_not_full_start_end_dates():
    config_dir = Path(__file__).parent / "configs"
    config = load_config(config_dir / "dev_config.json")

    if not config.get("client_id") or not config.get("client_secret"):
        pytest.skip("Terna API credentials not set in dev_config.json")

    connector = TernaLakeflowConnect(config)
    table_options = {"date_from": "28/02/2024", "date_to": "29/02/2024", "bidding_zone": "Italy"}
    # Full refresh, cursor is None
    start_offset = {"cursor": "29/02/2024"}

    records_iter, offset = connector.read_table("total_load", start_offset, table_options)
    records = list(records_iter)
    
    assert isinstance(offset, dict)
    assert len(records) == 0 
    assert offset.get("cursor") == "29/02/2024"

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
