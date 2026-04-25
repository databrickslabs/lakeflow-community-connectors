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


# ---------------------------------------------------------------------------
# Generic CDC reader (customers / products / orders share this code path)
# ---------------------------------------------------------------------------


def test_cdc_initial_fetch_sets_watermark_from_max_updated_at(conn):
    """First run (no offset) returns records and a watermark."""
    conn._session.get.return_value = _response(
        json_body={
            "customers": [
                {"id": 1, "updated_at": "2026-04-20T10:00:00Z"},
                {"id": 2, "updated_at": "2026-04-21T11:00:00Z"},
            ]
        }
    )

    records, offset = conn._read_customers({}, {})
    records = list(records)
    assert len(records) == 2
    assert records[0]["shop"] == "lakeflow-test-store"
    # Watermark = max updated_at across batch
    assert offset == {"updated_at": "2026-04-21T11:00:00Z"}


def test_cdc_incremental_passes_updated_at_min(conn):
    """Subsequent run with prior offset must send updated_at_min filter."""
    conn._session.get.return_value = _response(
        json_body={"customers": []}
    )
    conn._read_customers(
        {"updated_at": "2026-04-19T00:00:00Z"}, {}
    )

    params = conn._session.get.call_args.kwargs["params"]
    assert params["updated_at_min"] == "2026-04-19T00:00:00Z"
    assert params["limit"] == "250"


def test_cdc_strict_dedup_drops_boundary_records(conn):
    """Inclusive server filter + client-side > = strict overall.
    Records exactly at the watermark must be dropped (Shopify's
    updated_at_min is inclusive and would otherwise re-emit them)."""
    conn._session.get.return_value = _response(
        json_body={
            "customers": [
                # At watermark — must be filtered out client-side
                {"id": 1, "updated_at": "2026-04-20T00:00:00Z"},
                # After watermark — keep
                {"id": 2, "updated_at": "2026-04-21T00:00:00Z"},
            ]
        }
    )

    prior = {"updated_at": "2026-04-20T00:00:00Z"}
    records, offset = conn._read_customers(prior, {})
    records = list(records)

    assert len(records) == 1
    assert records[0]["id"] == 2
    assert offset == {"updated_at": "2026-04-21T00:00:00Z"}


def test_cdc_stable_offset_when_no_changes(conn):
    """No new records ⇒ end_offset == start_offset (terminates)."""
    conn._session.get.return_value = _response(
        json_body={"customers": []}
    )

    prior = {"updated_at": "2026-04-21T00:00:00Z"}
    records, offset = conn._read_customers(prior, {})
    assert list(records) == []
    assert offset == prior


def test_cdc_link_header_pagination_follows_next(conn):
    """When the Link header contains rel=next, the helper follows it
    until exhausted — even though the URL params differ on subsequent
    calls (page_info cursor encodes original filters)."""
    next_url = (
        "https://lakeflow-test-store.myshopify.com/admin/api/2026-04"
        "/customers.json?page_info=cursor2&limit=250"
    )
    conn._session.get.side_effect = [
        _response(
            json_body={
                "customers": [
                    {"id": 1, "updated_at": "2026-04-20T10:00:00Z"}
                ]
            },
            headers={"Link": f'<{next_url}>; rel="next"'},
        ),
        _response(
            json_body={
                "customers": [
                    {"id": 2, "updated_at": "2026-04-21T11:00:00Z"}
                ]
            },
            headers={},  # no next, drained
        ),
    ]

    records, offset = conn._read_customers({}, {})
    records = list(records)

    assert [r["id"] for r in records] == [1, 2]
    # Two HTTP calls: initial + follow-link
    assert conn._session.get.call_count == 2
    # Second call uses the URL from Link header verbatim, no params
    second_call = conn._session.get.call_args_list[1]
    assert second_call.args[0] == next_url
    assert second_call.kwargs.get("params") is None


def test_orders_passes_status_any(conn):
    """orders endpoint defaults to status=open and silently drops
    cancelled/closed; we must explicitly pass status=any."""
    conn._session.get.return_value = _response(
        json_body={"orders": []}
    )
    conn._read_orders({}, {})

    params = conn._session.get.call_args.kwargs["params"]
    assert params.get("status") == "any"


def test_orders_records_enriched_with_shop(conn):
    conn._session.get.return_value = _response(
        json_body={
            "orders": [
                {
                    "id": 1001,
                    "order_number": 1001,
                    "updated_at": "2026-04-21T11:00:00Z",
                    "financial_status": "paid",
                }
            ]
        }
    )
    records, _ = conn._read_orders({}, {})
    records = list(records)
    assert records[0]["shop"] == "lakeflow-test-store"
    assert records[0]["financial_status"] == "paid"


def test_products_uses_correct_endpoint(conn):
    conn._session.get.return_value = _response(
        json_body={"products": []}
    )
    conn._read_products({}, {})
    called_url = conn._session.get.call_args[0][0]
    assert called_url.endswith("/products.json")
