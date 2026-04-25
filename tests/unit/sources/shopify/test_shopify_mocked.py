"""Mock-based unit tests for ShopifyLakeflowConnect.

Stubs ``requests.Session`` so tests run without credentials in CI.
Live-API integration tests live in ``test_shopify_lakeflow_connect.py``
and are excluded from CI via test_exclude.txt.
"""

from unittest.mock import MagicMock

import pytest

from databricks.labs.community_connector.sources.shopify.shopify import (
    DEFAULT_API_VERSION,
    ShopifyLakeflowConnect,
)


def _response(status_code: int = 200, json_body=None, headers=None):
    """Build a mock ``requests.Response``-like object."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_body if json_body is not None else {}
    resp.headers = headers if headers is not None else {}
    resp.text = str(json_body) if json_body is not None else ""
    return resp


@pytest.fixture
def conn():
    """ShopifyLakeflowConnect with a stubbed ``requests.Session``."""
    c = ShopifyLakeflowConnect(
        {
            "shop": "lakeflow-test-store",
            "access_token": "shpat_fake",
        }
    )
    c._session = MagicMock()
    return c


# ---------------------------------------------------------------------------
# Constructor / connection-level concerns
# ---------------------------------------------------------------------------


def test_init_requires_shop():
    with pytest.raises(ValueError, match="shop"):
        ShopifyLakeflowConnect({"access_token": "shpat_x"})


def test_init_requires_access_token():
    with pytest.raises(ValueError, match="access_token"):
        ShopifyLakeflowConnect({"shop": "x"})


def test_init_default_api_version_applied(conn):
    assert conn.api_version == DEFAULT_API_VERSION
    assert "/admin/api/" + DEFAULT_API_VERSION in conn.base_url


def test_init_api_version_override():
    c = ShopifyLakeflowConnect(
        {
            "shop": "x",
            "access_token": "shpat_y",
            "api_version": "2025-10",
        }
    )
    assert c.api_version == "2025-10"
    assert "/2025-10" in c.base_url


def test_init_uses_shopify_token_header():
    c = ShopifyLakeflowConnect({"shop": "x", "access_token": "shpat_y"})
    assert c._session.headers["X-Shopify-Access-Token"] == "shpat_y"
    # Shopify uses its own header, NOT Authorization
    assert "Authorization" not in c._session.headers


# ---------------------------------------------------------------------------
# Interface compliance
# ---------------------------------------------------------------------------


def test_list_tables_returns_supported(conn):
    tables = conn.list_tables()
    assert "locations" in tables


def test_get_table_schema_returns_struct(conn):
    schema = conn.get_table_schema("locations", {})
    assert any(f.name == "id" for f in schema.fields)
    assert any(f.name == "shop" for f in schema.fields)


def test_get_table_schema_unknown_raises(conn):
    with pytest.raises(ValueError, match="Unsupported"):
        conn.get_table_schema("does_not_exist", {})


def test_read_table_metadata_returns_dict(conn):
    md = conn.read_table_metadata("locations", {})
    assert md["primary_keys"] == ["id"]
    assert md["ingestion_type"] == "snapshot"


def test_read_table_dispatches_unknown_raises(conn):
    with pytest.raises(ValueError, match="Unsupported"):
        conn.read_table("does_not_exist", {}, {})


# ---------------------------------------------------------------------------
# _read_locations
# ---------------------------------------------------------------------------


def test_read_locations_returns_records_with_shop_field(conn):
    """Records must be enriched with the shop subdomain."""
    conn._session.get.return_value = _response(
        json_body={
            "locations": [
                {"id": 1, "name": "HQ", "active": True},
                {"id": 2, "name": "Warehouse", "active": True},
            ]
        }
    )

    records, offset = conn._read_locations({}, {})
    records = list(records)

    assert len(records) == 2
    assert records[0]["id"] == 1
    assert records[0]["name"] == "HQ"
    assert records[0]["shop"] == "lakeflow-test-store"
    assert records[1]["shop"] == "lakeflow-test-store"
    # Snapshot tables return an empty offset (no incremental state)
    assert offset == {}


def test_read_locations_empty_response(conn):
    """No locations returned ⇒ empty iterator + empty offset."""
    conn._session.get.return_value = _response(json_body={"locations": []})

    records, offset = conn._read_locations({}, {})
    assert list(records) == []
    assert offset == {}


def test_read_locations_calls_correct_endpoint(conn):
    conn._session.get.return_value = _response(json_body={"locations": []})
    conn._read_locations({}, {})

    called_url = conn._session.get.call_args[0][0]
    assert called_url == (
        f"https://lakeflow-test-store.myshopify.com"
        f"/admin/api/{DEFAULT_API_VERSION}/locations.json"
    )


def test_read_locations_propagates_api_error(conn):
    conn._session.get.return_value = _response(
        status_code=403, json_body={}, headers={}
    )
    # api_get raises RuntimeError on non-200
    with pytest.raises(RuntimeError, match="locations"):
        conn._read_locations({}, {})
