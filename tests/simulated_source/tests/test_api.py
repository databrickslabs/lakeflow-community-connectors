"""Unit tests for the simulated source API.

Covers every feature documented in the module docstring of api.py:
  - Authentication
  - Table discovery and hidden tables
  - Schema and metadata endpoints
  - Per-table record reads with param validation
  - Filters (category, user_id, status)
  - Cursor-based reads (since, until, limit)
  - Full-refresh (products)
  - Deleted records endpoint (orders only)
  - DELETE endpoint (orders only)
  - POST insert / upsert
  - Nullable seed data
  - Retriable errors
  - JSON response structure
"""

from __future__ import annotations

import pytest

from tests.simulated_source.api import (
    API_CONFIG,
    SimulatedSourceAPI,
    TABLE_API_CONFIG,
)


@pytest.fixture()
def api():
    saved_error_rate = API_CONFIG["error_rate"]
    API_CONFIG["error_rate"] = 0
    try:
        yield SimulatedSourceAPI("test_user", "test_pass")
    finally:
        API_CONFIG["error_rate"] = saved_error_rate


# ── authentication ────────────────────────────────────────────────────


class TestAuthentication:
    def test_valid_credentials(self):
        api = SimulatedSourceAPI("user", "pass")
        assert api.get("/tables").status_code == 200

    def test_empty_username_raises(self):
        with pytest.raises(ValueError, match="username"):
            SimulatedSourceAPI("", "pass")

    def test_whitespace_username_raises(self):
        with pytest.raises(ValueError, match="username"):
            SimulatedSourceAPI("   ", "pass")

    def test_empty_password_raises(self):
        with pytest.raises(ValueError, match="password"):
            SimulatedSourceAPI("user", "")

    def test_whitespace_password_raises(self):
        with pytest.raises(ValueError, match="password"):
            SimulatedSourceAPI("user", "  ")


# ── table discovery ───────────────────────────────────────────────────


class TestTableDiscovery:
    def test_list_tables_returns_visible_tables(self, api):
        resp = api.get("/tables")
        assert resp.status_code == 200
        tables = resp.json()["tables"]
        assert "products" in tables
        assert "events" in tables
        assert "users" in tables
        assert "orders" in tables

    def test_list_tables_excludes_hidden(self, api):
        resp = api.get("/tables")
        tables = resp.json()["tables"]
        assert "metrics" not in tables

    def test_hidden_table_schema_returns_404(self, api):
        resp = api.get("/tables/metrics/schema")
        assert resp.status_code == 404

    def test_hidden_table_metadata_returns_404(self, api):
        resp = api.get("/tables/metrics/metadata")
        assert resp.status_code == 404

    def test_hidden_table_records_still_accessible(self, api):
        resp = api.get("/tables/metrics/records")
        assert resp.status_code == 200
        assert "records" in resp.json()


# ── schema and metadata ──────────────────────────────────────────────


class TestSchemaAndMetadata:
    @pytest.mark.parametrize("table", ["products", "events", "users", "orders"])
    def test_schema_returns_field_list(self, api, table):
        resp = api.get(f"/tables/{table}/schema")
        assert resp.status_code == 200
        schema = resp.json()["schema"]
        assert isinstance(schema, list)
        assert len(schema) > 0
        assert all("name" in f and "type" in f for f in schema)

    @pytest.mark.parametrize("table", ["products", "events", "users", "orders"])
    def test_metadata_returns_dict(self, api, table):
        resp = api.get(f"/tables/{table}/metadata")
        assert resp.status_code == 200
        metadata = resp.json()["metadata"]
        assert isinstance(metadata, dict)

    def test_metadata_has_no_ingestion_type(self, api):
        for table in ["products", "events", "users", "orders"]:
            metadata = api.get(f"/tables/{table}/metadata").json()["metadata"]
            assert "ingestion_type" not in metadata

    def test_unknown_table_schema_404(self, api):
        resp = api.get("/tables/nonexistent/schema")
        assert resp.status_code == 404

    def test_unknown_table_metadata_404(self, api):
        resp = api.get("/tables/nonexistent/metadata")
        assert resp.status_code == 404


# ── products (full refresh, category filter) ─────────────────────────


class TestProducts:
    def test_returns_all_records(self, api):
        resp = api.get("/tables/products/records")
        assert resp.status_code == 200
        records = resp.json()["records"]
        assert len(records) == 53

    def test_category_filter(self, api):
        resp = api.get("/tables/products/records", params={"category": "electronics"})
        assert resp.status_code == 200
        records = resp.json()["records"]
        assert len(records) > 0
        assert all(r["category"] == "electronics" for r in records)

    def test_unsupported_param_returns_400(self, api):
        resp = api.get("/tables/products/records", params={"since": "x"})
        assert resp.status_code == 400
        assert "Unsupported" in resp.json()["error"]

    def test_no_cursor_field_in_metadata(self, api):
        metadata = api.get("/tables/products/metadata").json()["metadata"]
        assert "cursor_field" not in metadata


# ── events (append-only, since + limit) ──────────────────────────────


class TestEvents:
    def test_returns_records(self, api):
        resp = api.get("/tables/events/records")
        assert resp.status_code == 200
        records = resp.json()["records"]
        assert len(records) > 0

    def test_limit(self, api):
        resp = api.get("/tables/events/records", params={"limit": "5"})
        assert resp.status_code == 200
        records = resp.json()["records"]
        assert len(records) == 5

    def test_since_filters_records(self, api):
        all_records = api.get("/tables/events/records", params={"limit": "200"}).json()["records"]
        mid = all_records[len(all_records) // 2]["created_at"]
        filtered = api.get("/tables/events/records", params={"since": mid, "limit": "200"}).json()[
            "records"
        ]
        assert len(filtered) < len(all_records)
        assert all(r["created_at"] > mid for r in filtered)

    def test_unsupported_param_returns_400(self, api):
        resp = api.get("/tables/events/records", params={"until": "x"})
        assert resp.status_code == 400

    def test_cursor_field_is_created_at(self, api):
        metadata = api.get("/tables/events/metadata").json()["metadata"]
        assert metadata["cursor_field"] == "created_at"


# ── users (mutable, since, no deletes) ───────────────────────────────


class TestUsers:
    def test_returns_records(self, api):
        resp = api.get("/tables/users/records")
        assert resp.status_code == 200
        records = resp.json()["records"]
        assert len(records) == 37

    def test_since_filters_records(self, api):
        all_records = api.get("/tables/users/records").json()["records"]
        mid = all_records[len(all_records) // 2]["updated_at"]
        filtered = api.get("/tables/users/records", params={"since": mid}).json()["records"]
        assert len(filtered) < len(all_records)
        assert all(r["updated_at"] > mid for r in filtered)

    def test_unsupported_limit_returns_400(self, api):
        resp = api.get("/tables/users/records", params={"limit": "10"})
        assert resp.status_code == 400

    def test_deleted_records_not_supported(self, api):
        resp = api.get("/tables/users/deleted_records")
        assert resp.status_code == 400

    def test_delete_not_supported(self, api):
        resp = api.delete("/tables/users/records/user_0001")
        assert resp.status_code == 400

    def test_upsert_updates_timestamp(self, api):
        resp = api.post(
            "/tables/users/records",
            json={
                "user_id": "user_0001",
                "email": "new@example.com",
                "display_name": "New Name",
                "status": "active",
            },
        )
        assert resp.status_code == 200
        record = resp.json()["record"]
        assert record["email"] == "new@example.com"
        assert "updated_at" in record


# ── orders (mutable, since, filters, deletes) ────────────────────────


class TestOrders:
    def test_returns_records(self, api):
        resp = api.get("/tables/orders/records")
        assert resp.status_code == 200
        records = resp.json()["records"]
        assert len(records) > 0

    def test_since_filters_records(self, api):
        all_records = api.get("/tables/orders/records").json()["records"]
        mid = all_records[len(all_records) // 2]["updated_at"]
        filtered = api.get("/tables/orders/records", params={"since": mid}).json()["records"]
        assert len(filtered) < len(all_records)

    def test_user_id_filter(self, api):
        resp = api.get("/tables/orders/records", params={"user_id": "user_0001"})
        assert resp.status_code == 200
        records = resp.json()["records"]
        assert all(r["user_id"] == "user_0001" for r in records)

    def test_status_filter(self, api):
        resp = api.get("/tables/orders/records", params={"status": "pending"})
        assert resp.status_code == 200
        records = resp.json()["records"]
        assert all(r["status"] == "pending" for r in records)

    def test_combined_filters(self, api):
        resp = api.get(
            "/tables/orders/records", params={"user_id": "user_0001", "status": "pending"}
        )
        assert resp.status_code == 200
        for r in resp.json()["records"]:
            assert r["user_id"] == "user_0001"
            assert r["status"] == "pending"

    def test_unsupported_param_returns_400(self, api):
        resp = api.get("/tables/orders/records", params={"until": "x"})
        assert resp.status_code == 400

    def test_delete_record(self, api):
        resp = api.delete("/tables/orders/records/order_0001")
        assert resp.status_code == 200
        tombstone = resp.json()["record"]
        assert tombstone["order_id"] == "order_0001"

    def test_delete_nonexistent_returns_404(self, api):
        resp = api.delete("/tables/orders/records/no_such_order")
        assert resp.status_code == 404

    def test_deleted_records_endpoint(self, api):
        api.delete("/tables/orders/records/order_0002")
        resp = api.get("/tables/orders/deleted_records")
        assert resp.status_code == 200
        deleted = resp.json()["records"]
        assert any(r["order_id"] == "order_0002" for r in deleted)

    def test_deleted_records_since_filter(self, api):
        api.delete("/tables/orders/records/order_0003")
        all_deleted = api.get("/tables/orders/deleted_records").json()["records"]
        if len(all_deleted) > 1:
            mid = all_deleted[0]["updated_at"]
            filtered = api.get("/tables/orders/deleted_records", params={"since": mid}).json()[
                "records"
            ]
            assert all(r["updated_at"] > mid for r in filtered)

    def test_deleted_records_unsupported_param_returns_400(self, api):
        resp = api.get("/tables/orders/deleted_records", params={"limit": "10"})
        assert resp.status_code == 400

    def test_post_then_since_returns_only_new_record(self, api):
        # Paginate to find the true latest timestamp across all records.
        all_records = []
        since = None
        while True:
            params = {"since": since} if since else {}
            page = api.get("/tables/orders/records", params=params).json()["records"]
            if not page:
                break
            all_records.extend(page)
            since = page[-1]["updated_at"]

        latest_ts = max(r["updated_at"] for r in all_records)

        resp = api.post(
            "/tables/orders/records",
            json={
                "order_id": "order_incr_test",
                "user_id": "user_0001",
                "amount": 123.45,
                "status": "pending",
            },
        )
        assert resp.status_code == 200

        new_records = api.get("/tables/orders/records", params={"since": latest_ts}).json()[
            "records"
        ]
        assert len(new_records) == 1
        assert new_records[0]["order_id"] == "order_incr_test"


# ── metrics (hidden, since + until, struct value) ────────────────────


class TestMetrics:
    def test_not_in_table_list(self, api):
        tables = api.get("/tables").json()["tables"]
        assert "metrics" not in tables

    def test_schema_not_accessible(self, api):
        assert api.get("/tables/metrics/schema").status_code == 404

    def test_metadata_not_accessible(self, api):
        assert api.get("/tables/metrics/metadata").status_code == 404

    def test_records_accessible(self, api):
        resp = api.get("/tables/metrics/records")
        assert resp.status_code == 200
        records = resp.json()["records"]
        assert len(records) > 0

    def test_since_filter(self, api):
        first_page = api.get("/tables/metrics/records").json()["records"]
        early_ts = first_page[5]["updated_at"]
        filtered = api.get("/tables/metrics/records", params={"since": early_ts}).json()["records"]
        assert all(r["updated_at"] > early_ts for r in filtered)

    def test_until_filter(self, api):
        all_records = api.get("/tables/metrics/records").json()["records"]
        mid = all_records[len(all_records) // 2]["updated_at"]
        filtered = api.get("/tables/metrics/records", params={"until": mid}).json()["records"]
        assert len(filtered) < len(all_records)
        assert all(r["updated_at"] <= mid for r in filtered)

    def test_since_and_until_combined(self, api):
        all_records = api.get("/tables/metrics/records").json()["records"]
        lo = all_records[len(all_records) // 4]["updated_at"]
        hi = all_records[3 * len(all_records) // 4]["updated_at"]
        filtered = api.get("/tables/metrics/records", params={"since": lo, "until": hi}).json()[
            "records"
        ]
        assert all(lo < r["updated_at"] <= hi for r in filtered)

    def test_value_is_struct_or_none(self, api):
        records = api.get("/tables/metrics/records").json()["records"]
        for r in records:
            v = r["value"]
            if v is not None:
                assert isinstance(v, dict)
                assert set(v.keys()) == {"count", "label", "measure"}

    def test_unsupported_param_returns_400(self, api):
        resp = api.get("/tables/metrics/records", params={"limit": "10"})
        assert resp.status_code == 400


# ── POST (insert / upsert) ───────────────────────────────────────────


class TestPost:
    def test_insert_into_products(self, api):
        resp = api.post(
            "/tables/products/records",
            json={
                "product_id": "prod_new",
                "name": "New Product",
                "price": 99.99,
                "category": "electronics",
            },
        )
        assert resp.status_code == 201
        assert resp.json()["record"]["product_id"] == "prod_new"

    def test_upsert_into_orders(self, api):
        resp = api.post(
            "/tables/orders/records",
            json={
                "order_id": "order_0001",
                "user_id": "user_0001",
                "amount": 999.99,
                "status": "shipped",
            },
        )
        assert resp.status_code == 200
        record = resp.json()["record"]
        assert record["amount"] == 999.99
        assert "updated_at" in record

    def test_post_unknown_table_returns_404(self, api):
        resp = api.post("/tables/nonexistent/records", json={"id": "1"})
        assert resp.status_code == 404


# ── nullable seed data ───────────────────────────────────────────────


class TestNullableSeedData:
    def test_products_have_some_nulls(self, api):
        records = api.get("/tables/products/records").json()["records"]
        null_count = sum(1 for r in records if r.get("name") is None)
        assert null_count > 0, "Expected some null 'name' values in products"

    def test_primary_keys_never_null(self, api):
        for table in ["products", "events", "users", "orders"]:
            resp = api.get(f"/tables/{table}/records")
            if resp.status_code != 200:
                continue
            records = resp.json()["records"]
            pk_fields = {
                "products": "product_id",
                "events": "event_id",
                "users": "user_id",
                "orders": "order_id",
            }
            pk = pk_fields[table]
            assert all(r[pk] is not None for r in records)

    def test_cursor_fields_never_null(self, api):
        records = api.get("/tables/events/records", params={"limit": "200"}).json()["records"]
        assert all(r["created_at"] is not None for r in records)

        records = api.get("/tables/users/records").json()["records"]
        assert all(r["updated_at"] is not None for r in records)


# ── retriable errors ─────────────────────────────────────────────────


class TestRetriableErrors:
    def test_errors_occur_with_high_rate(self):
        API_CONFIG["error_rate"] = 1.0
        try:
            api = SimulatedSourceAPI("user", "pass")
            resp = api.get("/tables")
            assert resp.status_code in (429, 500, 503)
            assert "error" in resp.json()
        finally:
            API_CONFIG["error_rate"] = 0.03

    def test_no_errors_with_zero_rate(self):
        API_CONFIG["error_rate"] = 0
        try:
            api = SimulatedSourceAPI("user", "pass")
            for _ in range(20):
                resp = api.get("/tables")
                assert resp.status_code == 200
        finally:
            API_CONFIG["error_rate"] = 0.03

    def test_post_can_return_retriable_error(self):
        API_CONFIG["error_rate"] = 1.0
        try:
            api = SimulatedSourceAPI("user", "pass")
            resp = api.post("/tables/orders/records", json={"order_id": "x"})
            assert resp.status_code in (429, 500, 503)
        finally:
            API_CONFIG["error_rate"] = 0.03

    def test_delete_can_return_retriable_error(self):
        API_CONFIG["error_rate"] = 1.0
        try:
            api = SimulatedSourceAPI("user", "pass")
            resp = api.delete("/tables/orders/records/order_0001")
            assert resp.status_code in (429, 500, 503)
        finally:
            API_CONFIG["error_rate"] = 0.03


# ── JSON response structure ──────────────────────────────────────────


class TestJsonResponseStructure:
    def test_list_tables_response(self, api):
        body = api.get("/tables").json()
        assert "tables" in body
        assert isinstance(body["tables"], list)

    def test_schema_response(self, api):
        body = api.get("/tables/products/schema").json()
        assert "schema" in body

    def test_metadata_response(self, api):
        body = api.get("/tables/products/metadata").json()
        assert "metadata" in body

    def test_records_response(self, api):
        body = api.get("/tables/products/records").json()
        assert "records" in body

    def test_post_response(self, api):
        body = api.post(
            "/tables/products/records",
            json={
                "product_id": "prod_json_test",
                "name": "Test",
                "price": 1.0,
                "category": "books",
            },
        ).json()
        assert "record" in body

    def test_delete_response(self, api):
        body = api.delete("/tables/orders/records/order_0010").json()
        assert "record" in body

    def test_error_response(self, api):
        body = api.get("/tables/nonexistent/schema").json()
        assert "error" in body


# ── routing edge cases ───────────────────────────────────────────────


class TestRouting:
    def test_unknown_get_route(self, api):
        resp = api.get("/unknown/path")
        assert resp.status_code == 404

    def test_unknown_post_route(self, api):
        resp = api.post("/unknown/path", json={})
        assert resp.status_code == 404

    def test_unknown_delete_route(self, api):
        resp = api.delete("/unknown/path")
        assert resp.status_code == 404

    def test_delete_not_allowed_on_products(self, api):
        resp = api.delete("/tables/products/records/prod_0001")
        assert resp.status_code == 400

    def test_delete_not_allowed_on_events(self, api):
        resp = api.delete("/tables/events/records/some_id")
        assert resp.status_code == 400

    def test_deleted_records_not_supported_on_products(self, api):
        resp = api.get("/tables/products/deleted_records")
        assert resp.status_code == 400

    def test_deleted_records_not_supported_on_events(self, api):
        resp = api.get("/tables/events/deleted_records")
        assert resp.status_code == 400
