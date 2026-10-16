"""Custom simulator handlers for PayPal endpoints the declarative spec can't model.

* ``oauth_token`` — stub for ``POST /v1/oauth2/token`` (client-credentials).
* ``disputes`` — ``GET /v1/customer/disputes``: ``start_time`` filter on
  ``update_time`` plus opaque ``next_page_token`` pagination surfaced through
  a ``rel=next`` entry in the response ``links`` array.
* ``product_detail`` — ``GET /v1/catalogs/products/{product_id}``: looks the
  product up by id in the ``product_details`` corpus. The ``products`` and
  ``product_details`` corpora are synthesized independently, so ids rarely
  line up; on a miss a deterministic record is cloned with the requested id.
"""

from __future__ import annotations

import copy
import json
import re
import zlib
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import ResponseRecord
from databricks.labs.community_connector.source_simulator.interceptor import (
    response_from_record,
)

_DISPUTES_DEFAULT_PAGE_SIZE = 10
_DISPUTES_MAX_PAGE_SIZE = 50
_TOKEN_PREFIX = "sim-page-"
_PRODUCT_PATH_RE = re.compile(r"/v1/catalogs/products/(?P<product_id>[^/]+)$")
_COMPACT_TZ_RE = re.compile(r"([+-]\d{2})(\d{2})$")
_EPOCH = datetime.min.replace(tzinfo=timezone.utc)


def oauth_token(prep: PreparedRequest, spec: Any, corpus: Any) -> Response:  # noqa: ARG001
    return _json_response(
        prep,
        200,
        {
            "scope": "https://uri.paypal.com/services/invoicing",
            "access_token": "sim-access-token",
            "token_type": "Bearer",
            "app_id": "APP-SIMULATED",
            "expires_in": 32400,
            "nonce": "sim-nonce",
        },
    )


def disputes(prep: PreparedRequest, spec: Any, corpus: Any) -> Response:  # noqa: ARG001
    query = dict(parse_qsl(urlsplit(prep.url or "").query, keep_blank_values=True))

    records = corpus.get("disputes") or []
    if not isinstance(records, list):
        records = []

    start_time = query.get("start_time")
    if start_time:
        lower = _parse_ts(start_time)
        records = [r for r in records if _record_ts(r) >= lower]
    records = sorted(records, key=_record_ts)

    page_size = _bounded_int(
        query.get("page_size"), _DISPUTES_DEFAULT_PAGE_SIZE, _DISPUTES_MAX_PAGE_SIZE
    )
    offset = _decode_token(query.get("next_page_token"))
    page = records[offset : offset + page_size]
    next_offset = offset + len(page)

    links = [{"href": prep.url, "rel": "self", "method": "GET"}]
    if page and next_offset < len(records):
        links.append(
            {
                "href": _with_query(
                    prep.url or "", next_page_token=f"{_TOKEN_PREFIX}{next_offset}"
                ),
                "rel": "next",
                "method": "GET",
            }
        )
    return _json_response(prep, 200, {"items": page, "links": links})


def product_detail(prep: PreparedRequest, spec: Any, corpus: Any) -> Response:  # noqa: ARG001
    match = _PRODUCT_PATH_RE.search(urlsplit(prep.url or "").path)
    product_id = match.group("product_id") if match else ""

    records = corpus.get("product_details") or []
    if not isinstance(records, list) or not records:
        return _not_found(prep, product_id)

    for record in records:
        if record.get("id") == product_id:
            return _json_response(prep, 200, record)

    template = records[zlib.crc32(product_id.encode()) % len(records)]
    clone = copy.deepcopy(template)
    clone["id"] = product_id
    return _json_response(prep, 200, clone)


# ----- helpers ---------------------------------------------------------------


def _not_found(prep: PreparedRequest, product_id: str) -> Response:
    return _json_response(
        prep,
        404,
        {
            "name": "RESOURCE_NOT_FOUND",
            "message": "The specified resource does not exist.",
            "details": [{"issue": "INVALID_RESOURCE_ID", "value": product_id}],
        },
    )


def _record_ts(record: dict) -> datetime:
    value = record.get("update_time")
    if not isinstance(value, str) or not value:
        return _EPOCH
    try:
        return _parse_ts(value)
    except ValueError:
        return _EPOCH


def _parse_ts(value: str) -> datetime:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    if "T" in text:
        text = _COMPACT_TZ_RE.sub(r"\1:\2", text)
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _bounded_int(raw: str | None, default: int, maximum: int) -> int:
    try:
        value = int(raw) if raw else default
    except ValueError:
        value = default
    return max(1, min(value, maximum))


def _decode_token(token: str | None) -> int:
    if not token:
        return 0
    if token.startswith(_TOKEN_PREFIX):
        token = token[len(_TOKEN_PREFIX) :]
    try:
        return max(0, int(token))
    except ValueError:
        return 0


def _with_query(url: str, **overrides: str) -> str:
    parts = urlsplit(url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query.update(overrides)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


def _json_response(prep: PreparedRequest, status: int, payload: Any) -> Response:
    rec = ResponseRecord(
        status_code=status,
        headers={"Content-Type": "application/json"},
        body_text=json.dumps(payload, ensure_ascii=False),
        body_b64=None,
        encoding="utf-8",
        url=prep.url,
    )
    return response_from_record(rec, prep)
