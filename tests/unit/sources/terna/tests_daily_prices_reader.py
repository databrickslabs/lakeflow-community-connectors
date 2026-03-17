"""Tests for the Terna connector daily_prices table (DailyPricesReader)."""

from datetime import datetime
from pathlib import Path

import pytest
import requests

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


def test_terna_list_tables_includes_daily_prices():
    """list_tables includes daily_prices; get_table_schema returns daily_prices schema (no API call)."""
    connector = TernaLakeflowConnect({"client_id": "test", "client_secret": "test"})

    tables = connector.list_tables()
    assert "daily_prices" in tables
    assert tables == ["total_load", "market_load", "daily_prices"]

    schema = connector.get_table_schema("daily_prices", {})
    assert schema.fieldNames() == [
        "publication_date",
        "reference_date",
        "data_type",
        "date_tz",
        "macrozone",
        "base_price_EURxMWh",
        "incentive_component_EURxMWh",
        "unbalance_price_EURxMWh",
    ]


def test_terna_daily_prices_missing_date_from():
    """When date_from is missing for daily_prices, connector raises ValueError."""
    config_dir = Path(__file__).parent / "configs"
    config = load_config(config_dir / "dev_config.json")

    if not config.get("client_id") or not config.get("client_secret"):
        pytest.skip("Terna API credentials not set in dev_config.json")

    connector = TernaLakeflowConnect(config)
    table_options = {"date_to": "29/02/2024"}
    start_offset = None

    with pytest.raises(ValueError, match="daily_prices requires 'date_from'"):
        records_iter, _ = connector.read_table("daily_prices", start_offset, table_options)
        list(records_iter)


def test_terna_daily_prices_beyond_five_years_limit():
    """When date_from is older than 5 solar years, connector raises ValueError."""
    config_dir = Path(__file__).parent / "configs"
    config = load_config(config_dir / "dev_config.json")

    if not config.get("client_id") or not config.get("client_secret"):
        pytest.skip("Terna API credentials not set in dev_config.json")

    connector = TernaLakeflowConnect(config)
    table_options = {"date_from": "31/12/2020", "date_to": "01/01/2021"}
    start_offset = None

    with pytest.raises(
        ValueError,
        match="Terna connector: 'date_from' must be within the last 5 solar years",
    ):
        connector.read_table("daily_prices", start_offset, table_options)


def test_terna_daily_prices_invalid_data_type():
    """When data_types contains an invalid value, connector raises ValueError."""
    config_dir = Path(__file__).parent / "configs"
    config = load_config(config_dir / "dev_config.json")

    if not config.get("client_id") or not config.get("client_secret"):
        pytest.skip("Terna API credentials not set in dev_config.json")

    connector = TernaLakeflowConnect(config)
    table_options = {
        "date_from": "01/02/2024",
        "date_to": "02/02/2024",
        "data_types": "InvalidType",
    }
    start_offset = None

    with pytest.raises(
        ValueError,
        match="Terna connector: Invalid dataType value InvalidType. Must be one of Orario, Quarto Orario",
    ):
        connector.read_table("daily_prices", start_offset, table_options)


def test_terna_daily_prices_valid_data_type_orario():
    """Read daily_prices with data_types=Orario (no error; may skip if API unavailable)."""
    config_dir = Path(__file__).parent / "configs"
    config = load_config(config_dir / "dev_config.json")

    if not config.get("client_id") or not config.get("client_secret"):
        pytest.skip("Terna API credentials not set in dev_config.json")

    connector = TernaLakeflowConnect(config)
    table_options = {
        "date_from": "01/03/2024",
        "date_to": "17/03/2024",
        "data_types": "Orario",
    }
    start_offset = None

    try:
        records_iter, offset = connector.read_table("daily_prices", start_offset, table_options)
        records = list(records_iter)
    except (requests.RequestException, OSError) as e:
        pytest.skip(f"Terna API unreachable: {e}")

    assert isinstance(offset, dict)
    assert "cursor" in offset
    assert len(records) > 0
    assert offset["cursor"] == "17/03/2024"


def test_terna_daily_prices_valid_data_type_quarto_orario():
    """Read daily_prices with data_types=Quarto Orario (no error)."""
    config_dir = Path(__file__).parent / "configs"
    config = load_config(config_dir / "dev_config.json")

    if not config.get("client_id") or not config.get("client_secret"):
        pytest.skip("Terna API credentials not set in dev_config.json")

    connector = TernaLakeflowConnect(config)
    table_options = {
        "date_from": "01/02/2024",
        "date_to": "02/02/2024",
        "data_types": "Quarto Orario",
    }
    start_offset = None

    try:
        records_iter, offset = connector.read_table("daily_prices", start_offset, table_options)
        records = list(records_iter)
    except (requests.RequestException, OSError) as e:
        pytest.skip(f"Terna API unreachable: {e}")

    assert isinstance(offset, dict)
    assert offset.get("cursor") == "02/02/2024"


def test_terna_daily_prices_cdc_empty_because_same_date():
    """When date_from equals date_to, connector returns empty iterator and cursor."""
    config_dir = Path(__file__).parent / "configs"
    config = load_config(config_dir / "dev_config.json")

    if not config.get("client_id") or not config.get("client_secret"):
        pytest.skip("Terna API credentials not set in dev_config.json")

    connector = TernaLakeflowConnect(config)
    date_from = datetime.now().strftime("%d/%m/%Y")
    table_options = {"date_from": date_from}
    start_offset = None

    records_iter, offset = connector.read_table("daily_prices", start_offset, table_options)
    records = list(records_iter)

    assert len(records) == 0
    assert offset.get("cursor") == date_from


def test_terna_daily_prices_date_from_no_end():
    """When date_from equals date_to, connector returns empty iterator and cursor."""
    config_dir = Path(__file__).parent / "configs"
    config = load_config(config_dir / "dev_config.json")

    if not config.get("client_id") or not config.get("client_secret"):
        pytest.skip("Terna API credentials not set in dev_config.json")

    connector = TernaLakeflowConnect(config)
    date_from = datetime.now().strftime("%d/%m/%Y")
    table_options = {"date_from": "01/03/2026", "date_to": "17/03/2026", "data_types": "Quarto Orario"}
    start_offset = None

    records_iter, offset = connector.read_table("daily_prices", start_offset, table_options)
    records = list(records_iter)

    logger.info(f"Records: {len(records)}")

    #assert len(records) == 0
    assert offset.get("cursor") == date_from


def test_terna_daily_prices_full_start_end_dates():
    """Read daily_prices with date_from and date_to; cursor is date_to."""
    config_dir = Path(__file__).parent / "configs"
    config = load_config(config_dir / "dev_config.json")

    if not config.get("client_id") or not config.get("client_secret"):
        pytest.skip("Terna API credentials not set in dev_config.json")

    connector = TernaLakeflowConnect(config)
    table_options = {"date_from": "01/03/2026", "date_to": "17/03/2026", "data_types": "Quarto Orario"}
    start_offset = None

    try:
        records_iter, offset = connector.read_table("daily_prices", start_offset, table_options)
        records = list(records_iter)
    except (requests.RequestException, OSError) as e:
        pytest.skip(f"Terna API unreachable: {e}")

    assert isinstance(offset, dict)
    assert offset.get("cursor") == "17/03/2026"


def test_terna_daily_prices_cursor_resume():
    """Read daily_prices with start_offset cursor; resume from cursor."""
    config_dir = Path(__file__).parent / "configs"
    config = load_config(config_dir / "dev_config.json")

    if not config.get("client_id") or not config.get("client_secret"):
        pytest.skip("Terna API credentials not set in dev_config.json")

    connector = TernaLakeflowConnect(config)
    table_options = {"date_from": "28/02/2024", "date_to": "29/02/2024"}
    start_offset = {"cursor": "29/02/2024"}

    records_iter, offset = connector.read_table("daily_prices", start_offset, table_options)
    records = list(records_iter)

    assert isinstance(offset, dict)
    assert offset.get("cursor") == "29/02/2024"
    assert len(records) == 0


def test_terna_daily_prices_read_table_metadata():
    """read_table_metadata for daily_prices returns primary_keys, cursor_field, ingestion_type."""
    connector = TernaLakeflowConnect({"client_id": "test", "client_secret": "test"})

    metadata = connector.read_table_metadata("daily_prices", {})

    assert metadata["primary_keys"] == ["reference_date", "macrozone"]
    assert metadata["cursor_field"] == "reference_date"
    assert metadata["ingestion_type"] == "append"
