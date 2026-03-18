"""Test the Terna connector using the LakeflowConnect test suite."""

import re
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from databricks.labs.community_connector.sources.terna.terna import TernaLakeflowConnect
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


def test_terna_full_missing_date_from():
    """When date_from is missing, connector raises ValueError (API requires it)."""
    config_dir = Path(__file__).parent / "configs"
    config = load_config(config_dir / "dev_config.json")

    if not config.get("client_id") or not config.get("client_secret"):
        pytest.skip("Terna API credentials not set in dev_config.json")

    connector = TernaLakeflowConnect(config)
    table_options = {"date_to": "29/02/2024", "bidding_zones": "Italy"}
    start_offset = None

    with pytest.raises(ValueError, match="monthly_index_industrial_electrical_consumption requires 'date_from'"):
        records_iter, offset = connector.read_table("monthly_index_industrial_electrical_consumption", start_offset, table_options)
        records = list(records_iter)


def test_terna_beyond_five_years_limit():
    """When date_from is missing, connector raises ValueError (API requires it)."""
    config_dir = Path(__file__).parent / "configs"
    config = load_config(config_dir / "dev_config.json")

    if not config.get("client_id") or not config.get("client_secret"):
        pytest.skip("Terna API credentials not set in dev_config.json")

    connector = TernaLakeflowConnect(config)
    table_options = {"date_from": "31/12/2020", "bidding_zones": "Italy"}
    start_offset = None

    with pytest.raises(ValueError, match="Terna connector: 'date_from' must be within the last 5 solar years, not sooner than 01/01/2021"):
        connector.read_table("monthly_index_industrial_electrical_consumption", start_offset, table_options)


def test_terna_cdc_more_than_sixty_days_multiple_chunks():
    """When date_from is missing, connector raises ValueError (API requires it)."""
    config_dir = Path(__file__).parent / "configs"
    config = load_config(config_dir / "dev_config.json")

    if not config.get("client_id") or not config.get("client_secret"):
        pytest.skip("Terna API credentials not set in dev_config.json")

    connector = TernaLakeflowConnect(config)

    date_to = datetime.now()
    date_from = (date_to - timedelta(days=1)).strftime("%d/%m/%Y")
    table_options = {"date_from": date_from, "bidding_zones": "Italy"}
    start_offset = None

    records_iter, offset = connector.read_table("monthly_index_industrial_electrical_consumption", start_offset, table_options)
    records = list(records_iter)

    assert offset.get("cursor") == date_to.strftime("%d/%m/%Y")


def test_terna_cdc_more_than_sixty_days_single_chunk():
    """When date_from is missing, connector raises ValueError (API requires it)."""
    config_dir = Path(__file__).parent / "configs"
    config = load_config(config_dir / "dev_config.json")

    if not config.get("client_id") or not config.get("client_secret"):
        pytest.skip("Terna API credentials not set in dev_config.json")

    connector = TernaLakeflowConnect(config)

    days = 80

    date_to = datetime.now()
    date_from = (date_to - timedelta(days=days)).strftime("%d/%m/%Y")
    table_options = {"date_from": date_from, "bidding_zones": "Italy"}
    start_offset = None

    records_iter, offset = connector.read_table("monthly_index_industrial_electrical_consumption", start_offset, table_options)
    records = list(records_iter)

    assert len(records) == 4 * 24 * (days-2)
    assert offset.get("cursor") == date_to.strftime("%d/%m/%Y")


def test_terna_cdc():
    """When date_from is missing, connector raises ValueError (API requires it)."""
    config_dir = Path(__file__).parent / "configs"
    config = load_config(config_dir / "dev_config.json")

    if not config.get("client_id") or not config.get("client_secret"):
        pytest.skip("Terna API credentials not set in dev_config.json")

    connector = TernaLakeflowConnect(config)

    days = 2

    date_from = (datetime.now() - timedelta(days=days)).strftime("%d/%m/%Y")
    table_options = {"date_from": date_from, "bidding_zones": "Italy"}
    start_offset = None

    records_iter, offset = connector.read_table("monthly_index_industrial_electrical_consumption", start_offset, table_options)
    records = list(records_iter)

    assert offset.get("cursor") == datetime.now().strftime("%d/%m/%Y")

def test_terna_cdc_empty_because_same_date():
    """When date_from is missing, connector raises ValueError (API requires it)."""
    config_dir = Path(__file__).parent / "configs"
    config = load_config(config_dir / "dev_config.json")

    if not config.get("client_id") or not config.get("client_secret"):
        pytest.skip("Terna API credentials not set in dev_config.json")

    connector = TernaLakeflowConnect(config)

    date_from = datetime.now().strftime("%d/%m/%Y")

    table_options = {"date_from": date_from, "bidding_zones": "Italy"}
    start_offset = None

    records_iter, offset = connector.read_table("monthly_index_industrial_electrical_consumption", start_offset, table_options)
    records = list(records_iter)

    assert len(records) == 0
    assert offset.get("cursor") == date_from

def test_terna_full_start_end_dates():
    config_dir = Path(__file__).parent / "configs"
    config = load_config(config_dir / "dev_config.json")

    if not config.get("client_id") or not config.get("client_secret"):
        pytest.skip("Terna API credentials not set in dev_config.json")

    connector = TernaLakeflowConnect(config)
    table_options = {"date_from": "01/02/2024", "date_to": "29/02/2024", "bidding_zones": "Italy"}
    # Full refresh, cursor is None
    start_offset = None

    records_iter, offset = connector.read_table("monthly_index_industrial_electrical_consumption", start_offset, table_options)
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
    table_options = {"date_from": "28/02/2024", "date_to": "29/02/2024", "bidding_zones": "Italy"}
    start_offset = {"cursor": "29/02/2024"}

    records_iter, offset = connector.read_table("monthly_index_industrial_electrical_consumption", start_offset, table_options)
    records = list(records_iter)
    
    assert isinstance(offset, dict)
    assert len(records) == 0 
    assert offset.get("cursor") == "29/02/2024"


def test_terna_wrong_bidding_zone():
    config_dir = Path(__file__).parent / "configs"
    config = load_config(config_dir / "dev_config.json")

    if not config.get("client_id") or not config.get("client_secret"):
        pytest.skip("Terna API credentials not set in dev_config.json")

    connector = TernaLakeflowConnect(config)
    table_options = {"date_from": "28/02/2024", "date_to": "29/02/2024", "bidding_zones": "Trento"}
    # Full refresh, cursor is None
    start_offset = {"cursor": "28/02/2024"}

    with pytest.raises(ValueError, match="Terna connector: Invalid biddingZone value Trento. Must be one of North, Centre-North, South, Centre-South, Sardinia, Sicily, Calabria, Italy"):
        records_iter, offset = connector.read_table("monthly_index_industrial_electrical_consumption", start_offset, table_options)
        records = list(records_iter)


def test_terna_multiple_bidding_zones():
    config_dir = Path(__file__).parent / "configs"
    config = load_config(config_dir / "dev_config.json")

    if not config.get("client_id") or not config.get("client_secret"):
        pytest.skip("Terna API credentials not set in dev_config.json")

    connector = TernaLakeflowConnect(config)
    table_options = {"date_from": "28/02/2024", "date_to": "29/02/2024", "bidding_zones": "Italy,North"}
    # Full refresh, cursor is None
    start_offset = {"cursor": "28/02/2024"}

    records_iter, offset = connector.read_table("monthly_index_industrial_electrical_consumption", start_offset, table_options)
    records = list(records_iter)
    
