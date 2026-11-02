"""Custom handlers that make the simulator behave like a FHIR R4 server.

Response shapes mirror public HAPI R4 (``https://hapi.fhir.org/baseR4``,
measured 2026-11-02), so a live run validates cleanly against this spec:

- Searchset and history Bundles carry ``resourceType, id, meta, type, link,
  entry``; empty ones carry ``total`` instead of ``entry``/``link``. These
  Bundles include every key so both shapes validate.
- Paging follows ``link[next]`` to the server root (``/baseR4?_getpages=...``).
- ``$export`` answers 202 with a ``Content-Location`` poll URL. The poll
  returns the manifest, files are NDJSON under ``/Binary/{id}``, and the
  cleanup DELETE of a finished job is 404.

FHIR search prefixes (``_lastUpdated=gt2024-01-05T00:00:00Z``) don't fit the
simulator's generic ``params`` filters, and repeated ``_lastUpdated`` values
must be ANDed (the connector sends ``gt<cursor>`` and ``le<init_ts>``). The
shared query parser keeps only the last value of a repeated key, so these
handlers parse the query string themselves.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urlsplit

from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import (
    ResponseRecord,
)
from databricks.labs.community_connector.source_simulator.interceptor import (
    response_from_record,
)

_PREFIXES = ("gt", "ge", "lt", "le", "eq", "ne")
_FHIR_JSON = "application/fhir+json"
_NDJSON = "application/fhir+ndjson"


# ----- helpers ------------------------------------------------------------


def _respond(prep: PreparedRequest, status: int, body: Any = None, headers=None) -> Response:
    if isinstance(body, (dict, list)):
        text = json.dumps(body, ensure_ascii=False)
    else:
        text = body or ""
    rec = ResponseRecord(
        status_code=status,
        headers={"Content-Type": _FHIR_JSON, **(headers or {})},
        body_text=text,
        body_b64=None,
        encoding="utf-8",
        url=prep.url,
    )
    resp = response_from_record(rec, prep)
    # The body is fully in memory. Marking it consumed lets streaming readers
    # (iter_lines, close) use it; the connector streams NDJSON downloads.
    resp._content_consumed = True  # pylint: disable=protected-access
    return resp


def _query(prep: PreparedRequest) -> List[Tuple[str, str]]:
    return parse_qsl(urlsplit(prep.url or "").query, keep_blank_values=True)


def _root(prep: PreparedRequest) -> str:
    """Scheme, host and FHIR base path, e.g. ``https://host/baseR4``."""
    parts = urlsplit(prep.url or "")
    return f"{parts.scheme}://{parts.netloc}/{parts.path.strip('/').split('/')[0]}"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_prefix(raw: str) -> Tuple[str, str]:
    for p in _PREFIXES:
        if raw.startswith(p) and len(raw) > len(p):
            return p, raw[len(p) :]
    return "eq", raw


def _matches(value: Optional[str], prefix: str, target: str) -> bool:
    if value is None:
        return False
    return {
        "eq": value == target,
        "ne": value != target,
        "gt": value > target,
        "ge": value >= target,
        "lt": value < target,
        "le": value <= target,
    }[prefix]


def _last_updated(resource: dict) -> Optional[str]:
    return (resource.get("meta") or {}).get("lastUpdated")


def _bundle(prep: PreparedRequest, bundle_type: str, entries: List[dict]) -> dict:
    return {
        "resourceType": "Bundle",
        "id": "simulator",
        "meta": {"lastUpdated": _now()},
        "type": bundle_type,
        "total": len(entries),
        "link": [{"relation": "self", "url": prep.url or ""}],
        "entry": entries,
    }


def _resource_type(prep: PreparedRequest, offset: int = 1) -> str:
    """Resource type segment of ``/baseR4/{type}`` (offset 1) or deeper paths."""
    return urlsplit(prep.url or "").path.strip("/").split("/")[offset]


# ----- search -------------------------------------------------------------


def serve_search(prep: PreparedRequest, spec, corpus) -> Response:  # noqa: ARG001
    """``GET /baseR4/{type}``: searchset from the type's corpus, if any.

    Honours repeated ``_lastUpdated`` bounds (ANDed) and ``_sort=_lastUpdated``.
    """
    resource_type = _resource_type(prep)
    query = _query(prep)
    bounds = [_parse_prefix(v) for k, v in query if k == "_lastUpdated"]
    records: List[dict] = [
        r
        for r in (corpus.get(resource_type) or [])
        if all(_matches(_last_updated(r), p, t) for p, t in bounds)
    ]
    if ("_sort", "_lastUpdated") in query:
        records.sort(key=lambda r: _last_updated(r) or "")
    root = _root(prep)
    entries = [
        {
            "fullUrl": f"{root}/{resource_type}/{r.get('id')}",
            "resource": r,
            "search": {"mode": "match"},
        }
        for r in records
    ]
    return _respond(prep, 200, _bundle(prep, "searchset", entries))


def serve_page(prep: PreparedRequest, spec, corpus) -> Response:  # noqa: ARG001
    """``GET /baseR4?_getpages=...``: a follow-on page. Simulated searches fit in
    one page, so this is only reached in live runs; it answers an empty page."""
    return _respond(prep, 200, _bundle(prep, "searchset", []))


# ----- history ------------------------------------------------------------


def serve_history(prep: PreparedRequest, spec, corpus) -> Response:  # noqa: ARG001
    """``GET /baseR4/{type}/_history?_since=...``: newest-first history Bundle.

    Entries come from ``corpus/history.json``. DELETE entries carry no
    resource and no timestamp (as on HAPI), so ``_since`` can't filter them
    out; versioned entries are kept when their lastUpdated is at or after it.
    """
    resource_type = _resource_type(prep)
    since = dict(_query(prep)).get("_since")
    entries = [
        e
        for e in (corpus.get("history") or [])
        if (e.get("request") or {}).get("url", "").split("/")[0] == resource_type
        and (since is None or "resource" not in e or (_last_updated(e["resource"]) or "") >= since)
    ]
    return _respond(prep, 200, _bundle(prep, "history", entries))


# ----- bulk $export -------------------------------------------------------


def serve_export_kickoff(prep: PreparedRequest, spec, corpus) -> Response:  # noqa: ARG001
    """``GET /baseR4[/Patient]/$export``: accept the job, point at the poll URL."""
    types = dict(_query(prep)).get("_type", "")
    poll = f"{_root(prep)}/$export-poll-status?" + urlencode(
        {"_jobId": "simulator-job", "_type": types}
    )
    return _respond(prep, 202, "", headers={"Content-Location": poll})


def serve_export_status(prep: PreparedRequest, spec, corpus) -> Response:  # noqa: ARG001
    """``GET /baseR4/$export-poll-status``: the job is already complete."""
    root = _root(prep)
    types = [t for t in dict(_query(prep)).get("_type", "").split(",") if t]
    manifest = {
        "transactionTime": _now(),
        "request": prep.url or "",
        "requiresAccessToken": False,
        "output": [{"type": t, "url": f"{root}/Binary/{t}"} for t in types if corpus.get(t)],
        "error": [],
    }
    return _respond(prep, 200, manifest)


def serve_export_file(prep: PreparedRequest, spec, corpus) -> Response:  # noqa: ARG001
    """``GET /baseR4/Binary/{type}``: the type's corpus as NDJSON."""
    resource_type = _resource_type(prep, offset=2)
    lines = [json.dumps(r, ensure_ascii=False) for r in corpus.get(resource_type) or []]
    return _respond(prep, 200, "\n".join(lines) + "\n", headers={"Content-Type": _NDJSON})


def serve_not_found(prep: PreparedRequest, spec, corpus) -> Response:  # noqa: ARG001
    """404 OperationOutcome: export cleanup of a finished job (HAPI) and SMART
    discovery on a server without SMART auth (HAPI)."""
    outcome: Dict[str, Any] = {
        "resourceType": "OperationOutcome",
        "text": {
            "status": "generated",
            "div": '<div xmlns="http://www.w3.org/1999/xhtml">Not found</div>',
        },
        "issue": [{"severity": "error", "code": "not-found"}],
    }
    return _respond(prep, 404, outcome)
