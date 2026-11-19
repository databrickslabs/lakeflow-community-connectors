"""Custom simulator handlers for the Pipedrive API.

Pipedrive's response envelopes don't fit the declarative pagination styles:

* **v2** (``/api/v2/...``): ``{"success": true, "data": [...],
  "additional_data": {"next_cursor": "<opaque>" | null}}`` with ``?cursor=``.
* **v1** (``/v1/...``): ``{"success": true, "data": [...],
  "additional_data": {"pagination": {"start", "limit",
  "more_items_in_collection", "next_start"}}}`` with ``?start=&limit=``.

The handlers below reproduce those envelopes on top of the corpus and model
the query semantics the connector relies on:

* ``updated_since`` / ``updated_until`` — inclusive bounds on ``update_time``
  (falling back to ``add_time``).  Timestamps are compared as datetimes, so
  RFC 3339 and v1's ``YYYY-MM-DD HH:MM:SS`` both work.
* ``sort_by`` + ``sort_direction`` (v2) and ``sort=<field> ASC|DESC`` (v1).
* Deals ``status``: ``deleted`` selects soft-deleted rows (``is_deleted``
  true); ``open`` / ``won`` / ``lost`` select live rows; the default excludes
  deleted rows — this mirrors how Pipedrive exposes deleted deals.
* Projects: ``/projects`` serves rows without ``archive_time``,
  ``/projects/archived`` serves rows with one.
* Deal children (``/api/v2/deals/products``, ``/v1/deals/{id}/flow``) are
  joined on ``deal_id`` / ``item_id``.  A corpus bootstrapped from schemas has
  random foreign keys that never match a deal; in that case child rows are
  spread deterministically over the known deal IDs so the child tables still
  return data in simulate mode.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qsl, urlsplit

from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import (
    ResponseRecord,
)
from databricks.labs.community_connector.source_simulator.interceptor import (
    response_from_record,
)

_V2_DEFAULT_LIMIT = 100
_V2_MAX_LIMIT = 500
_V1_DEFAULT_LIMIT = 100
_V1_MAX_LIMIT = 500
_V1_MAX_LIMIT_BY_PATH = {"/v1/files": 100}
_CURSOR_PREFIX = "sim-offset:"
_DEAL_CORPORA = ("deals", "deals_archived")
_LIVE_DEAL_STATUSES = {"open", "won", "lost"}


# --------------------------------------------------------------------------- #
# Public handlers
# --------------------------------------------------------------------------- #


def serve_v2_list(prep: PreparedRequest, spec: Any, corpus: Any) -> Response:
    """Cursor-paginated v2 list endpoint (deals, persons, pipelines, ...)."""
    query = _query(prep)
    path = urlsplit(prep.url or "").path
    records = _corpus_list(corpus, spec.corpus)

    if spec.corpus in _DEAL_CORPORA:
        records = _filter_deal_status(records, query.get("status"))
    if spec.corpus == "projects":
        archived = path.rstrip("/").endswith("/archived")
        records = [r for r in records if bool(r.get("archive_time")) == archived]

    records = _filter_updated(records, query.get("updated_since"), query.get("updated_until"))
    records = _sort(
        records,
        query.get("sort_by") or "id",
        (query.get("sort_direction") or "asc").lower() == "desc",
    )
    return _v2_page(prep, records, query)


def serve_v1_list(prep: PreparedRequest, spec: Any, corpus: Any) -> Response:
    """Offset-paginated v1 list endpoint (notes, leads, files, *Fields, ...)."""
    query = _query(prep)
    path = urlsplit(prep.url or "").path.rstrip("/")
    records = _corpus_list(corpus, spec.corpus)
    records = _filter_updated(records, query.get("updated_since"), None)
    field, desc = _parse_v1_sort(query.get("sort"))
    records = _sort(records, field, desc)
    max_limit = _V1_MAX_LIMIT_BY_PATH.get(path, _V1_MAX_LIMIT)
    return _v1_page(prep, records, query, max_limit)


def serve_deal_products(prep: PreparedRequest, spec: Any, corpus: Any) -> Response:
    """``GET /api/v2/deals/products?deal_ids=1,2,...`` (max 100 IDs)."""
    query = _query(prep)
    raw_ids = [x.strip() for x in (query.get("deal_ids") or "").split(",") if x.strip()]
    if not raw_ids:
        return _error(prep, 400, "deal_ids is required")
    if len(raw_ids) > 100:
        return _error(prep, 400, "deal_ids accepts at most 100 IDs")

    wanted = set(raw_ids)
    linked = _link_children(corpus, _corpus_list(corpus, spec.corpus), "deal_id")
    records = [r for r in linked if str(r.get("deal_id")) in wanted]
    records = _sort(records, "id", False)
    return _v2_page(prep, records, query)


def serve_deal_flow(prep: PreparedRequest, spec: Any, corpus: Any) -> Response:
    """``GET /v1/deals/{id}/flow`` — change log rows wrapped as flow items."""
    query = _query(prep)
    match = spec.path_regex.match(urlsplit(prep.url or "").path)
    deal_id = (match.groupdict().get("id") if match else None) or ""

    if deal_id not in {str(i) for i in _known_deal_ids(corpus)}:
        return _error(prep, 404, "Deal not found")

    linked = _link_children(corpus, _corpus_list(corpus, spec.corpus), "item_id")
    entries = [r for r in linked if str(r.get("item_id")) == deal_id]
    # Pipedrive returns the flow newest first.
    entries = _sort(entries, "log_time", True)
    items = [
        {"object": "dealChange", "timestamp": e.get("log_time"), "data": e} for e in entries
    ]
    return _v1_page(prep, items, query, _V1_MAX_LIMIT)


# --------------------------------------------------------------------------- #
# Filtering / sorting
# --------------------------------------------------------------------------- #


def _parse_ts(value: Any) -> Optional[datetime]:
    if value is None or value == "":
        return None
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        try:
            dt = datetime.strptime(text[:19], "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _record_ts(record: Dict[str, Any]) -> Optional[datetime]:
    return _parse_ts(record.get("update_time")) or _parse_ts(record.get("add_time"))


def _filter_updated(
    records: List[Dict[str, Any]], since: Optional[str], until: Optional[str]
) -> List[Dict[str, Any]]:
    lo = _parse_ts(since)
    hi = _parse_ts(until)
    if lo is None and hi is None:
        return records
    out = []
    for r in records:
        ts = _record_ts(r)
        if ts is None:
            continue
        if lo is not None and ts < lo:
            continue
        if hi is not None and ts > hi:
            continue
        out.append(r)
    return out


def _filter_deal_status(records: List[Dict[str, Any]], status: Optional[str]) -> List[Dict[str, Any]]:
    wanted = {s.strip().lower() for s in (status or "").split(",") if s.strip()}
    want_deleted = "deleted" in wanted
    want_live = bool(wanted & _LIVE_DEAL_STATUSES) or not wanted
    if want_deleted and want_live:
        return records
    if want_deleted:
        return [r for r in records if r.get("is_deleted")]
    return [r for r in records if not r.get("is_deleted")]


def _sort_key(record: Dict[str, Any], field: str) -> Tuple[int, Any]:
    value = record.get(field)
    if field.endswith("_time") or field.endswith("_date"):
        ts = _parse_ts(value) if field != "update_time" else _record_ts(record)
        if ts is not None:
            return (0, ts.timestamp())
        return (1, 0)
    if value is None:
        return (1, "")
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return (0, value)
    return (0, str(value))


def _sort(records: List[Dict[str, Any]], field: str, desc: bool) -> List[Dict[str, Any]]:
    present = [r for r in records if _sort_key(r, field)[0] == 0]
    missing = [r for r in records if _sort_key(r, field)[0] == 1]
    try:
        present = sorted(
            present,
            key=lambda r: (_sort_key(r, field)[1], _sort_key(r, "id")[1]),
            reverse=desc,
        )
    except TypeError:
        present = sorted(present, key=lambda r: str(r.get(field)), reverse=desc)
    return present + missing


def _parse_v1_sort(raw: Optional[str]) -> Tuple[str, bool]:
    if not raw:
        return "id", False
    first = raw.split(",")[0].strip().split()
    field = first[0] if first else "id"
    desc = len(first) > 1 and first[1].upper() == "DESC"
    return field, desc


# --------------------------------------------------------------------------- #
# Deal-child linking
# --------------------------------------------------------------------------- #


def _known_deal_ids(corpus: Any) -> List[Any]:
    ids: Dict[Any, None] = {}
    for name in _DEAL_CORPORA:
        for r in _corpus_list(corpus, name):
            if r.get("id") is not None:
                ids[r["id"]] = None
    return list(ids)


def _link_children(corpus: Any, records: List[Dict[str, Any]], fk: str) -> List[Dict[str, Any]]:
    """Return child records whose ``fk`` points at a known deal.

    When no child references any known deal (a schema-bootstrapped corpus),
    assign children round-robin to the known deal IDs instead.
    """
    deal_ids = _known_deal_ids(corpus)
    if not deal_ids or not records:
        return records
    known = {str(i) for i in deal_ids}
    if any(str(r.get(fk)) in known for r in records):
        return records
    ordered = sorted(deal_ids, key=lambda x: (str(type(x)), x))
    out = []
    for i, r in enumerate(records):
        clone = dict(r)
        clone[fk] = ordered[i % len(ordered)]
        out.append(clone)
    return out


# --------------------------------------------------------------------------- #
# Envelopes
# --------------------------------------------------------------------------- #


def _int_param(query: Dict[str, str], name: str, default: int, lo: int, hi: int) -> int:
    try:
        value = int(query.get(name, default))
    except (TypeError, ValueError):
        value = default
    return max(lo, min(hi, value))


def _decode_cursor(raw: Optional[str]) -> int:
    if not raw:
        return 0
    text = raw[len(_CURSOR_PREFIX):] if raw.startswith(_CURSOR_PREFIX) else raw
    try:
        return max(0, int(text))
    except ValueError:
        return 0


def _v2_page(prep: PreparedRequest, records: List[Dict[str, Any]], query: Dict[str, str]) -> Response:
    limit = _int_param(query, "limit", _V2_DEFAULT_LIMIT, 1, _V2_MAX_LIMIT)
    offset = _decode_cursor(query.get("cursor"))
    page = records[offset : offset + limit]
    end = offset + len(page)
    next_cursor = f"{_CURSOR_PREFIX}{end}" if page and end < len(records) else None
    return _json(
        prep,
        200,
        {"success": True, "data": page, "additional_data": {"next_cursor": next_cursor}},
    )


def _v1_page(
    prep: PreparedRequest, records: List[Dict[str, Any]], query: Dict[str, str], max_limit: int
) -> Response:
    limit = _int_param(query, "limit", _V1_DEFAULT_LIMIT, 1, max_limit)
    start = _int_param(query, "start", 0, 0, 10**9)
    page = records[start : start + limit]
    more = start + len(page) < len(records) and bool(page)
    pagination: Dict[str, Any] = {
        "start": start,
        "limit": limit,
        "more_items_in_collection": more,
    }
    if more:
        pagination["next_start"] = start + len(page)
    return _json(
        prep,
        200,
        {"success": True, "data": page, "additional_data": {"pagination": pagination}},
    )


def _query(prep: PreparedRequest) -> Dict[str, str]:
    return dict(parse_qsl(urlsplit(prep.url or "").query, keep_blank_values=True))


def _corpus_list(corpus: Any, name: Optional[str]) -> List[Dict[str, Any]]:
    if not name:
        return []
    value = corpus.get(name)
    return list(value) if isinstance(value, list) else []


def _error(prep: PreparedRequest, status: int, message: str) -> Response:
    return _json(prep, status, {"success": False, "error": message, "errorCode": status})


def _json(prep: PreparedRequest, status: int, payload: Dict[str, Any]) -> Response:
    body = json.dumps(payload, ensure_ascii=False)
    rec = ResponseRecord(
        status_code=status,
        headers={"Content-Type": "application/json"},
        body_text=body,
        body_b64=None,
        encoding="utf-8",
        url=prep.url,
    )
    return response_from_record(rec, prep)
