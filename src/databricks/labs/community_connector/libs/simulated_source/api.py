"""Simulated REST API for a data source.

Exposes ``get()``, ``post()``, and ``delete()`` methods that behave like
``requests.get()``, ``requests.post()``, ``requests.delete()`` — the
caller passes a URL path and optional query params or JSON body, and
receives back a response object with ``.status_code`` and ``.json()``.

Authentication
--------------
The API requires a ``username`` and ``password`` to initialise.  Any
non-empty strings are accepted (no specific credentials are enforced).
Pass them when creating the client::

    api = get_api("my_user", "my_pass")

Tables
------
products
    A product catalog.  Every GET returns the full set of records (no
    incremental cursor).  Supports an optional ``category`` filter.

events
    An append-only event log.  New records are timestamped with
    ``created_at``.  Supports ``since`` for cursor-based reads and
    ``limit`` for pagination.  Records are never updated or deleted.

users
    A mutable user directory.  Records carry an ``updated_at`` date that
    advances on every change.  Supports ``since`` for incremental reads.
    No deletes.

orders
    A mutable order ledger with full lifecycle support.  Records carry an
    ``updated_at`` timestamp.  Supports ``since`` for incremental reads,
    plus ``user_id`` and ``status`` filters.  Deleted records are
    accessible via the ``/deleted_records`` endpoint (with ``since``),
    and individual records can be removed via ``DELETE /records/{pk}``.

All tables above are discoverable via ``GET /tables`` and their schema
and metadata are available via the corresponding endpoints.

metrics  *(hidden)*
    A time-series metrics table that cannot be discovered through the API
    — it is excluded from ``GET /tables`` and its schema and metadata are
    not available.  Records can still be read directly via
    ``GET /tables/metrics/records`` if you know the table name.  Supports
    ``since`` and ``until`` for time-range queries.  The ``value`` field
    is a struct with subfields ``count`` (integer), ``label`` (string),
    and ``measure`` (double).

Pagination
----------
All tables support a ``page`` query parameter (integer, 1-based).  When
omitted the request starts from page 1.  Each table has a
``max_page_size`` configured in ``TABLE_API_CONFIG`` that caps the
number of records per page.  The response body always includes a
``next_page`` field: an integer indicating the next page number when
more records are available, or ``null`` when the current page is the
last one.  The ``page`` value is replayable — requesting the same page
number returns the same slice of data (assuming no mutations between
calls).

Route table
-----------
GET    /tables                           → list table names
GET    /tables/{table}/schema            → column descriptors
GET    /tables/{table}/metadata          → table metadata
GET    /tables/{table}/records           → list records (params vary by table)
GET    /tables/{table}/deleted_records   → list deleted tombstones (orders only)
POST   /tables/{table}/records           → insert / upsert a record
DELETE /tables/{table}/records/{pk}      → delete a record (orders only)

Per-table query param support for GET /records is defined in
``TABLE_API_CONFIG``.  Global behaviour knobs (null rate for seed data,
retriable-error rate) live in ``API_CONFIG``.
"""

from __future__ import annotations

import random
import re
import threading
import uuid
from datetime import timedelta
from typing import Optional

from databricks.labs.community_connector.libs.simulated_source.store import Store, _iso, _now


class Response:  # pylint: disable=too-few-public-methods
    """Mimics a ``requests.Response`` with ``.status_code`` and ``.json()``."""

    __slots__ = ("status_code", "_body")

    def __init__(self, status_code: int, body) -> None:
        self.status_code = status_code
        self._body = body

    def json(self):
        """Return the parsed JSON body."""
        return self._body


# ── global API configuration ──────────────────────────────────────────
#
# null_rate     : probability (0.0–1.0) that a nullable field is None
#                 in seed data.  Default 0.2 (20%).
# error_rate    : probability (0.0–1.0) that any API call returns a
#                 retriable error instead of the real response.
#                 Default 0.03 (3%).  Error types are chosen uniformly
#                 from 429 (rate limit), 500 (internal), 503 (unavailable).

API_CONFIG: dict[str, float] = {
    "null_rate": 0.2,
    "error_rate": 0.03,
}

_RETRIABLE_ERRORS = [
    (429, {"error": "Rate limit exceeded. Please retry after a short delay."}),
    (500, {"error": "Internal server error. Please retry."}),
    (503, {"error": "Service temporarily unavailable. Please retry."}),
]


# ── per-table API configuration ──────────────────────────────────────
#
# allowed_record_params : query params accepted by GET /records
# allowed_deleted_params: query params accepted by GET /deleted_records
#                         (absent key means the endpoint is unsupported)
# allow_delete          : whether DELETE /records/{pk} is supported
# filter_params         : subset of allowed_record_params that act as
#                         exact-match filters (passed to store.list_records)
# hidden                : if True, excluded from GET /tables, /schema,
#                         and /metadata — but records endpoint still works
# max_page_size         : max records returned per page.  All tables
#                         support a ``page`` query param (int, 1-based,
#                         defaults to 1).  The response includes a
#                         ``next_page`` field (int or null).

TABLE_API_CONFIG: dict[str, dict] = {
    "products": {
        "allowed_record_params": {"category", "page"},
        "filter_params": {"category"},
        "allow_delete": False,
        "max_page_size": 20,
    },
    "events": {
        "allowed_record_params": {"since", "limit", "page"},
        "allow_delete": False,
        "max_page_size": 50,
    },
    "users": {
        "allowed_record_params": {"since", "page"},
        "allow_delete": False,
        "max_page_size": 15,
    },
    "orders": {
        "allowed_record_params": {"since", "user_id", "status", "page"},
        "filter_params": {"user_id", "status"},
        "allowed_deleted_params": {"since", "page"},
        "allow_delete": True,
        "max_page_size": 40,
    },
    "metrics": {
        "allowed_record_params": {"since", "until", "page"},
        "allow_delete": False,
        "hidden": True,
        "max_page_size": 100,
    },
}


# Pre-compiled route patterns
_ROUTE_TABLES = re.compile(r"^/tables/?$")
_ROUTE_TABLE_SCHEMA = re.compile(r"^/tables/(?P<table>[^/]+)/schema/?$")
_ROUTE_TABLE_METADATA = re.compile(r"^/tables/(?P<table>[^/]+)/metadata/?$")
_ROUTE_TABLE_RECORDS = re.compile(r"^/tables/(?P<table>[^/]+)/records/?$")
_ROUTE_TABLE_RECORD_PK = re.compile(r"^/tables/(?P<table>[^/]+)/records/(?P<pk>[^/]+)/?$")
_ROUTE_TABLE_DELETED = re.compile(r"^/tables/(?P<table>[^/]+)/deleted_records/?$")


class SimulatedSourceAPI:
    """In-memory simulated REST API.

    Usage mirrors the ``requests`` library::

        api = get_api("my_user", "my_pass")
        resp = api.get("/tables")
        tables = resp.json()

        resp = api.get("/tables/users/records", params={"since": ts})
        records = resp.json()

        resp = api.post("/tables/users/records", json={"user_id": "u1", ...})
        created = resp.json()

        resp = api.delete("/tables/orders/records/order_0001")
        tombstone = resp.json()
    """

    def __init__(self, username: str, password: str) -> None:
        if not username or not username.strip():
            raise ValueError("username must be a non-empty string")
        if not password or not password.strip():
            raise ValueError("password must be a non-empty string")
        self._rng = random.Random()
        self._store = Store()
        self._register_tables()
        self._seed()

    def _maybe_error(self) -> Optional[Response]:
        """With probability ``API_CONFIG["error_rate"]``, return a retriable error."""
        if self._rng.random() < API_CONFIG["error_rate"]:
            status, body = self._rng.choice(_RETRIABLE_ERRORS)
            return Response(status, body)
        return None

    def get(self, path: str, *, params: Optional[dict] = None) -> Response:
        """Dispatch a GET request to the matching route handler."""
        err = self._maybe_error()
        if err:
            return err
        params = params or {}

        _routes = [
            (_ROUTE_TABLES, self._handle_list_tables),
            (_ROUTE_TABLE_SCHEMA, lambda m: self._handle_get_schema(m.group("table"))),
            (_ROUTE_TABLE_METADATA, lambda m: self._handle_get_metadata(m.group("table"))),
            (_ROUTE_TABLE_DELETED, lambda m: self._handle_get_deleted(m.group("table"), params)),
            (_ROUTE_TABLE_RECORDS, lambda m: self._handle_get_records(m.group("table"), params)),
        ]
        for pattern, handler in _routes:
            m = pattern.match(path)
            if m:
                return handler(m)

        return Response(404, {"error": f"No route matches GET {path}"})

    def post(self, path: str, *, json: Optional[dict] = None) -> Response:
        """Dispatch a POST request to the matching route handler."""
        err = self._maybe_error()
        if err:
            return err
        json = json or {}

        m = _ROUTE_TABLE_RECORDS.match(path)
        if m:
            return self._handle_post_record(m.group("table"), json)

        return Response(404, {"error": f"No route matches POST {path}"})

    def delete(self, path: str) -> Response:
        """Dispatch a DELETE request to the matching route handler."""
        err = self._maybe_error()
        if err:
            return err
        m = _ROUTE_TABLE_RECORD_PK.match(path)
        if m:
            return self._handle_delete_record(m.group("table"), m.group("pk"))

        return Response(404, {"error": f"No route matches DELETE {path}"})

    # ── route handlers ────────────────────────────────────────────────

    def _handle_list_tables(self, _match) -> Response:
        """Return the list of non-hidden tables."""
        hidden = {t for t, c in TABLE_API_CONFIG.items() if c.get("hidden")}
        tables = [t for t in self._store.list_tables() if t not in hidden]
        return Response(200, {"tables": tables})

    def _handle_get_schema(self, table: str) -> Response:
        if TABLE_API_CONFIG.get(table, {}).get("hidden"):
            return Response(404, {"error": f"Table '{table}' not found"})
        try:
            return Response(200, {"schema": self._store.get_table_schema(table)})
        except ValueError as e:
            return Response(404, {"error": str(e)})

    def _handle_get_metadata(self, table: str) -> Response:
        if TABLE_API_CONFIG.get(table, {}).get("hidden"):
            return Response(404, {"error": f"Table '{table}' not found"})
        try:
            return Response(200, {"metadata": self._store.get_table_metadata(table)})
        except ValueError as e:
            return Response(404, {"error": str(e)})

    def _handle_get_records(self, table: str, params: dict) -> Response:
        try:
            metadata = self._store.get_table_metadata(table)
        except ValueError as e:
            return Response(404, {"error": str(e)})

        cfg = TABLE_API_CONFIG.get(table, {})
        bad = set(params.keys()) - cfg.get("allowed_record_params", set())
        if bad:
            return Response(
                400,
                {"error": f"Unsupported query params for '{table}': {sorted(bad)}"},
            )

        max_page_size = cfg.get("max_page_size", 100)
        page = int(params.get("page", 1))
        if page < 1:
            return Response(400, {"error": "page must be >= 1"})

        filters = {k: params[k] for k in cfg.get("filter_params", set()) if k in params}
        cursor_field = metadata.get("cursor_field")

        if not cursor_field:
            return self._paginate_full_refresh(table, filters, page, max_page_size)

        return self._paginate_cursor(table, params, cursor_field, filters, page, max_page_size)

    def _paginate_full_refresh(self, table, filters, page, max_page_size):
        """Paginate a table that has no cursor field (full-refresh semantics)."""
        records = self._store.get_all_records(table)
        for field, value in filters.items():
            records = [r for r in records if r.get(field) == value]
        records.sort(key=lambda r: r.get(self._store.get_table_pk(table), ""))
        start = (page - 1) * max_page_size
        page_records = records[start : start + max_page_size]
        next_page = page + 1 if start + max_page_size < len(records) else None
        return Response(200, {"records": page_records, "next_page": next_page})

    def _paginate_cursor(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self, table, params, cursor_field, filters, page, max_page_size,
    ):
        """Paginate a table using cursor-field range queries."""
        effective_size = min(int(params.get("limit", max_page_size)), max_page_size)
        try:
            all_records = self._store.list_records(
                table,
                since=params.get("since"),
                until=params.get("until"),
                cursor_field=cursor_field,
                filters=filters or None,
                limit=None,
            )
            start = (page - 1) * effective_size
            page_records = all_records[start : start + effective_size]
            next_page = page + 1 if start + effective_size < len(all_records) else None
            return Response(200, {"records": page_records, "next_page": next_page})
        except ValueError as e:
            return Response(404, {"error": str(e)})

    def _handle_get_deleted(self, table: str, params: dict) -> Response:
        try:
            metadata = self._store.get_table_metadata(table)
        except ValueError as e:
            return Response(404, {"error": str(e)})

        cfg = TABLE_API_CONFIG.get(table, {})
        allowed = cfg.get("allowed_deleted_params")
        if allowed is None:
            return Response(
                400,
                {"error": f"Table '{table}' does not support the deleted_records endpoint"},
            )

        bad = set(params.keys()) - allowed
        if bad:
            return Response(
                400,
                {"error": f"Unsupported query params for '{table}' deleted_records: {sorted(bad)}"},
            )

        max_page_size = cfg.get("max_page_size", 100)
        page = int(params.get("page", 1))
        if page < 1:
            return Response(400, {"error": "page must be >= 1"})

        cursor_field = metadata.get("cursor_field")
        since = params.get("since")

        try:
            all_records = self._store.list_deleted_records(
                table,
                since=since,
                cursor_field=cursor_field,
                limit=None,
            )
            start = (page - 1) * max_page_size
            page_records = all_records[start : start + max_page_size]
            next_page = page + 1 if start + max_page_size < len(all_records) else None
            return Response(200, {"records": page_records, "next_page": next_page})
        except ValueError as e:
            return Response(404, {"error": str(e)})

    def _handle_post_record(self, table: str, body: dict) -> Response:
        try:
            metadata = self._store.get_table_metadata(table)
        except ValueError as e:
            return Response(404, {"error": str(e)})

        cursor_field = metadata.get("cursor_field")

        if not cursor_field:
            created = self._store.insert_record(table, body)
            return Response(201, {"record": created})

        upserted = self._store.upsert_record(table, body, ts_field=cursor_field)
        return Response(200, {"record": upserted})

    def _handle_delete_record(self, table: str, pk_value: str) -> Response:
        try:
            metadata = self._store.get_table_metadata(table)
        except ValueError as e:
            return Response(404, {"error": str(e)})

        cfg = TABLE_API_CONFIG.get(table, {})
        if not cfg.get("allow_delete"):
            return Response(
                400,
                {"error": f"Table '{table}' does not support the DELETE endpoint"},
            )

        cursor_field = metadata.get("cursor_field")
        tombstone = self._store.delete_record(table, pk_value, ts_field=cursor_field)
        if tombstone is None:
            return Response(404, {"error": f"Record with pk '{pk_value}' not found"})
        return Response(200, {"record": tombstone})

    # ── table registration ────────────────────────────────────────────

    def _register_tables(self) -> None:
        self._store.register_table(
            name="products",
            schema_fields=[
                {"name": "product_id", "type": "string", "nullable": False},
                {"name": "name", "type": "string", "nullable": True},
                {"name": "price", "type": "double", "nullable": True},
                {"name": "category", "type": "string", "nullable": True},
            ],
            metadata={
                "primary_keys": ["product_id"],
            },
            pk_field="product_id",
        )

        self._store.register_table(
            name="events",
            schema_fields=[
                {"name": "event_id", "type": "string", "nullable": False},
                {"name": "event_type", "type": "string", "nullable": True},
                {"name": "user_id", "type": "string", "nullable": True},
                {"name": "payload", "type": "string", "nullable": True},
                {"name": "created_at", "type": "timestamp", "nullable": True},
            ],
            metadata={
                "cursor_field": "created_at",
            },
            pk_field="event_id",
        )

        self._store.register_table(
            name="users",
            schema_fields=[
                {"name": "user_id", "type": "string", "nullable": False},
                {"name": "email", "type": "string", "nullable": True},
                {"name": "display_name", "type": "string", "nullable": True},
                {"name": "status", "type": "string", "nullable": True},
                {"name": "updated_at", "type": "timestamp", "nullable": True},
            ],
            metadata={
                "primary_keys": ["user_id"],
                "cursor_field": "updated_at",
            },
            pk_field="user_id",
        )

        self._store.register_table(
            name="orders",
            schema_fields=[
                {"name": "order_id", "type": "string", "nullable": False},
                {"name": "user_id", "type": "string", "nullable": True},
                {"name": "amount", "type": "double", "nullable": True},
                {"name": "status", "type": "string", "nullable": True},
                {"name": "updated_at", "type": "timestamp", "nullable": True},
            ],
            metadata={
                "primary_keys": ["order_id"],
                "cursor_field": "updated_at",
            },
            pk_field="order_id",
        )

        self._store.register_table(
            name="metrics",
            schema_fields=[
                {"name": "metric_id", "type": "string", "nullable": False},
                {"name": "name", "type": "string", "nullable": True},
                {
                    "name": "value",
                    "type": "struct",
                    "nullable": True,
                    "fields": [
                        {"name": "count", "type": "integer", "nullable": True},
                        {"name": "label", "type": "string", "nullable": True},
                        {"name": "measure", "type": "double", "nullable": True},
                    ],
                },
                {"name": "host", "type": "string", "nullable": True},
                {"name": "updated_at", "type": "timestamp", "nullable": True},
            ],
            metadata={
                "primary_keys": ["metric_id"],
                "cursor_field": "updated_at",
            },
            pk_field="metric_id",
        )

    # ── seed data ─────────────────────────────────────────────────────

    def _seed(self) -> None:
        rng = random.Random(42)
        null_rate = API_CONFIG["null_rate"]
        base = _now() - timedelta(hours=1)

        def _maybe(value, *, rng=rng):
            return value if rng.random() >= null_rate else None

        self._store.seed_records(
            "products",
            [
                {
                    "product_id": f"prod_{i:04d}",
                    "name": _maybe(f"Product {i}"),
                    "price": _maybe(round(10.0 + i * 1.5, 2)),
                    "category": _maybe(["electronics", "books", "clothing"][i % 3]),
                }
                for i in range(53)
            ],
        )

        self._store.seed_records(
            "events",
            [
                {
                    "event_id": str(uuid.UUID(int=i)),
                    "event_type": _maybe(["click", "view", "purchase"][i % 3]),
                    "user_id": _maybe(f"user_{i % 5:03d}"),
                    "payload": _maybe(f"payload_{i}"),
                    "created_at": _iso(base + timedelta(seconds=i)),
                }
                for i in range(101)
            ],
        )

        self._store.seed_records(
            "users",
            [
                {
                    "user_id": f"user_{i:04d}",
                    "email": _maybe(f"user{i}@example.com"),
                    "display_name": _maybe(f"User {i}"),
                    "status": _maybe("active"),
                    "updated_at": _iso(base + timedelta(seconds=i * 60)),
                }
                for i in range(37)
            ],
        )

        self._store.seed_records(
            "orders",
            [
                {
                    "order_id": f"order_{i:04d}",
                    "user_id": _maybe(f"user_{i % 10:04d}"),
                    "amount": _maybe(round(50.0 + i * 3.3, 2)),
                    "status": _maybe(["pending", "shipped", "delivered"][i % 3]),
                    "updated_at": _iso(base + timedelta(seconds=i)),
                }
                for i in range(103)
            ],
        )

        self._store.seed_records(
            "metrics",
            [
                {
                    "metric_id": f"metric_{i:04d}",
                    "name": _maybe(f"metric.{['cpu', 'mem', 'disk', 'net'][i % 4]}"),
                    "value": _maybe(
                        {
                            "count": _maybe(i),
                            "label": _maybe(["low", "medium", "high", "critical"][i % 4]),
                            "measure": _maybe(round(i * 1.1, 2)),
                        }
                    ),
                    "host": _maybe(f"host-{i % 3}"),
                    "updated_at": _iso(base + timedelta(seconds=i * 60)),
                }
                for i in range(253)
            ],
        )


# ── singleton management ──────────────────────────────────────────────

_INSTANCE: Optional[SimulatedSourceAPI] = None
_INSTANCE_LOCK = threading.Lock()


def get_api(username: str, password: str) -> SimulatedSourceAPI:
    """Return the singleton API instance, creating it on first call."""
    global _INSTANCE  # noqa: PLW0603
    with _INSTANCE_LOCK:
        if _INSTANCE is None:
            _INSTANCE = SimulatedSourceAPI(username, password)
        return _INSTANCE


def reset_api(username: str, password: str) -> SimulatedSourceAPI:
    """Reset the singleton — call between test runs."""
    global _INSTANCE  # noqa: PLW0603
    with _INSTANCE_LOCK:
        _INSTANCE = SimulatedSourceAPI(username, password)
        return _INSTANCE
