"""Tests for the monthly_index_industrial_electrical_consumption table reader."""

import calendar
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from databricks.labs.community_connector.sources.terna.terna import TernaLakeflowConnect
from tests.unit.sources.test_utils import load_config

import logging

logger = logging.getLogger(__name__)

TABLE = "monthly_index_industrial_electrical_consumption"


def _get_connector():
    config_dir = Path(__file__).parent / "configs"
    config = load_config(config_dir / "dev_config.json")
    if not config.get("client_id") or not config.get("client_secret"):
        pytest.skip("Terna API credentials not set in dev_config.json")
    return TernaLakeflowConnect(config)


def test_terna_init_raises_without_credentials():
    """Initializing without client_id or client_secret raises ValueError."""
    with pytest.raises(ValueError, match="client_id.*client_secret"):
        TernaLakeflowConnect({})

    with pytest.raises(ValueError, match="client_id.*client_secret"):
        TernaLakeflowConnect({"client_id": "x"})

    with pytest.raises(ValueError, match="client_id.*client_secret"):
        TernaLakeflowConnect({"client_secret": "y"})


def test_terna_full_missing_date_from():
    """When date_from is missing, connector raises ValueError."""
    connector = _get_connector()
    table_options = {"date_to": "29/02/2024"}

    with pytest.raises(ValueError, match="monthly_index_industrial_electrical_consumption requires 'date_from'"):
        connector.read_table(TABLE, None, table_options)


def test_terna_beyond_five_years_limit():
    """date_from older than 5 solar years raises ValueError."""
    connector = _get_connector()
    table_options = {"date_from": "31/12/2020"}

    with pytest.raises(ValueError, match="Terna connector: 'date_from' must be within the last 5 solar years, not sooner than 01/01/2021"):
        connector.read_table(TABLE, None, table_options)


def test_terna_invalid_sector():
    """An invalid sector value raises ValueError."""
    connector = _get_connector()
    table_options = {"date_from": "01/02/2024", "date_to": "28/02/2024", "sectors": "INVALID_SECTOR"}

    with pytest.raises(ValueError, match="Invalid sector value INVALID_SECTOR"):
        connector.read_table(TABLE, None, table_options)


def test_terna_invalid_tension_type():
    """An invalid tension_type value raises ValueError."""
    connector = _get_connector()
    table_options = {"date_from": "01/02/2024", "date_to": "28/02/2024", "tension_types": "BT"}

    with pytest.raises(ValueError, match="Invalid tension_type value BT"):
        connector.read_table(TABLE, None, table_options)


def test_terna_cdc_empty_because_same_date():
    """When date_from == date_to, returns empty with cursor == date_from."""
    connector = _get_connector()
    date_from = datetime.now().strftime("%d/%m/%Y")
    table_options = {"date_from": date_from}

    records_iter, offset = connector.read_table(TABLE, None, table_options)
    records = list(records_iter)

    assert len(records) == 0
    assert offset.get("cursor") == date_from


def test_terna_full_single_month():
    """Full read of a single known historical month returns data."""
    connector = _get_connector()
    table_options = {"date_from": "01/11/2024", "date_to": "30/11/2024"}

    records_iter, offset = connector.read_table(TABLE, None, table_options)
    records = list(records_iter)

    assert isinstance(offset, dict)
    assert len(records) > 0
    assert offset.get("cursor") == "30/11/2024"


def test_terna_full_multi_month():
    """Reading a range spanning multiple months returns data and cursor at end of last month."""
    connector = _get_connector()
    table_options = {"date_from": "01/10/2024", "date_to": "15/12/2024"}

    records_iter, offset = connector.read_table(TABLE, None, table_options)
    records = list(records_iter)

    assert isinstance(offset, dict)
    assert len(records) > 0
    assert offset.get("cursor") == "31/12/2024"


def test_terna_not_full_cursor_at_date_to():
    """When cursor == date_to, returns empty (already fully read)."""
    connector = _get_connector()
    table_options = {"date_from": "01/11/2024", "date_to": "30/11/2024"}
    start_offset = {"cursor": "30/11/2024"}

    records_iter, offset = connector.read_table(TABLE, start_offset, table_options)
    records = list(records_iter)

    assert isinstance(offset, dict)
    assert len(records) == 0
    assert offset.get("cursor") == "30/11/2024"


def test_terna_cdc_default_date_to():
    """When date_to is omitted, defaults to now; cursor set to end of current month."""
    connector = _get_connector()
    now = datetime.now()
    date_from = (now - timedelta(days=2)).strftime("%d/%m/%Y")
    table_options = {"date_from": date_from}

    records_iter, offset = connector.read_table(TABLE, None, table_options)
    records = list(records_iter)

    last_day = calendar.monthrange(now.year, now.month)[1]
    expected_cursor = f"{last_day:02d}/{now.month:02d}/{now.year}"
    assert offset.get("cursor") == expected_cursor


def test_terna_with_sector_filter():
    """Reading with a sector filter returns data."""
    connector = _get_connector()
    table_options = {
        "date_from": "01/11/2024",
        "date_to": "30/11/2024",
        "sectors": "ALIMENTARE",
    }

    records_iter, offset = connector.read_table(TABLE, None, table_options)
    records = list(records_iter)

    assert isinstance(offset, dict)
    assert len(records) > 0
    for record in records:
        assert record.get("sector") == "ALIMENTARE"


def test_terna_with_tension_type_filter():
    """Reading with a tension_type filter returns data."""
    connector = _get_connector()
    table_options = {
        "date_from": "01/11/2024",
        "date_to": "30/11/2024",
        "tension_types": "AT",
    }

    records_iter, offset = connector.read_table(TABLE, None, table_options)
    records = list(records_iter)

    assert isinstance(offset, dict)
    assert len(records) > 0
    for record in records:
        assert record.get("tension_type") == "AT"


def test_terna_with_multiple_sectors():
    """Reading with multiple comma-separated sectors completes without error."""
    connector = _get_connector()
    table_options = {
        "date_from": "01/11/2024",
        "date_to": "30/11/2024",
        "sectors": "ALIMENTARE,CHIMICA",
    }

    records_iter, offset = connector.read_table(TABLE, None, table_options)
    records = list(records_iter)

    assert isinstance(offset, dict)
    assert offset.get("cursor") == "30/11/2024"
