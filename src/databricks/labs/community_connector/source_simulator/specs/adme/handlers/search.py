"""Custom simulator handler for ADME's POST ``/api/search/v2/query_with_cursor``.

The OSDU Search Service accepts query parameters in the JSON body
(``kind``, ``query``, ``limit``, ``cursor``), not URL params, so the
simulator's declarative param-role pipeline can't match it. This
handler:

  1. Parses the request body.
  2. Picks the corpus based on the ``kind`` string in the body.
  3. Filters by the Lucene ``modifyTime:[since TO until]`` range, when
     present (the connector uses this for incremental sync).
  4. Synthesises a few future ``modifyTime`` records on top of the
     corpus so connector cap behaviour is exercised in tests.
  5. Paginates via an opaque cursor that just encodes the next offset.
"""

from __future__ import annotations

import copy
import json
import re
from datetime import datetime, timedelta, timezone
from typing import Any

from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import (
    ResponseRecord,
)
from databricks.labs.community_connector.source_simulator.interceptor import (
    response_from_record,
)


# Map an OSDU kind string (or wildcard form) to its corpus name. The
# simulator's CorpusStore is keyed by these names (one JSON file each).
_KIND_TO_CORPUS: dict[str, str] = {
    "Wellbore": "Wellbore",
    "Reservoir": "Reservoir",
    "Sample": "Rock_and_Fluid",
}

_RANGE_RE = re.compile(
    r'modifyTime:\[\s*(?P<lo>\*|"[^"]+")\s+TO\s+(?P<hi>\*|"[^"]+")\s*\]'
)

_DEFAULT_LIMIT = 1000
_FUTURE_RECORDS = 3


def query_with_cursor(
    prep: PreparedRequest, spec: Any, corpus: Any
) -> Response:  # noqa: ARG001
    body = _parse_body(prep.body)
    kind = body.get("kind") or ""
    query = body.get("query") or ""
    try:
        limit = int(body.get("limit") or _DEFAULT_LIMIT)
    except (TypeError, ValueError):
        limit = _DEFAULT_LIMIT
    cursor_raw = body.get("cursor")

    corpus_name = _resolve_corpus_name(kind)
    if not corpus_name:
        return _build_response(prep, status=400, payload={"error": f"unknown kind: {kind}"})

    records = corpus.get(corpus_name) or []
    if not isinstance(records, list):
        records = []

    # Append a few future-dated clones so the cap-validation termination
    # tests have something to detect when an uncapped connector leaks
    # records modified after init time. The connector should *not* return
    # these because its ``until`` filter caps at init time.
    records = _augment_with_future(records)

    # Apply Lucene modifyTime range filter, if present.
    lo, hi = _extract_range(query)
    filtered = [
        r for r in records if _modify_time_in_range(r, lo, hi)
    ]

    # Paginate via opaque cursor that just encodes the next offset.
    offset = _decode_cursor(cursor_raw)
    page = filtered[offset : offset + limit]
    new_offset = offset + len(page)
    next_cursor = (
        _encode_cursor(new_offset)
        if new_offset < len(filtered) and page
        else None
    )

    payload: dict[str, Any] = {
        "cursor": next_cursor,
        "results": page,
        "totalCount": len(filtered),
    }
    return _build_response(prep, status=200, payload=payload)


def oauth_token(
    prep: PreparedRequest, spec: Any, corpus: Any
) -> Response:  # noqa: ARG001
    """Stub Azure AD OAuth token endpoint — issues a placeholder JWT.

    The simulator never validates auth headers, but the connector posts
    to ``login.microsoftonline.com/.../oauth2/token`` to fetch a bearer
    before its first data call. We need to return a 200-shaped token
    response so connector ``__init__`` -> first read flow doesn't fail.
    """
    return _build_response(
        prep,
        status=200,
        payload={
            "token_type": "Bearer",
            "expires_in": 3600,
            "ext_expires_in": 3600,
            "access_token": "simulated-access-token",
        },
    )


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _parse_body(body: Any) -> dict[str, Any]:
    if body is None:
        return {}
    if isinstance(body, (bytes, bytearray)):
        try:
            text = body.decode("utf-8")
        except UnicodeDecodeError:
            return {}
    else:
        text = str(body)
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _resolve_corpus_name(kind: str) -> str | None:
    for marker, name in _KIND_TO_CORPUS.items():
        if marker in kind:
            return name
    return None


def _extract_range(query: str) -> tuple[str | None, str | None]:
    if not query:
        return None, None
    match = _RANGE_RE.search(query)
    if not match:
        return None, None
    lo = _strip_lucene_value(match.group("lo"))
    hi = _strip_lucene_value(match.group("hi"))
    return lo, hi


def _strip_lucene_value(token: str) -> str | None:
    if token == "*":
        return None
    if token.startswith('"') and token.endswith('"'):
        return token[1:-1]
    return token


def _modify_time_in_range(
    record: dict[str, Any], lo: str | None, hi: str | None
) -> bool:
    ts = record.get("modifyTime") or record.get("createTime")
    if ts is None:
        # Records without modifyTime fail an explicit upper-bound filter
        # but pass an open one — matches OSDU's "absent => not matched
        # by the range" semantics.
        return lo is None and hi is None
    if lo is not None and ts < lo:
        return False
    if hi is not None and ts > hi:
        return False
    return True


def _augment_with_future(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Append ``_FUTURE_RECORDS`` future-dated clones to the corpus.

    Connectors that correctly cap ``until`` at their init time won't
    return these. Uncapped connectors leak them and the simulate-mode
    termination tests catch the divergence.
    """
    if not records:
        return records
    future = []
    base = datetime.now(timezone.utc) + timedelta(days=365)
    template = records[-1]
    for i in range(_FUTURE_RECORDS):
        clone = copy.deepcopy(template)
        clone["id"] = f"{clone.get('id', 'future')}::future-{i}"
        future_ts = (base + timedelta(hours=i)).strftime(
            "%Y-%m-%dT%H:%M:%S.000Z"
        )
        clone["modifyTime"] = future_ts
        clone["createTime"] = future_ts
        future.append(clone)
    return list(records) + future


_CURSOR_PREFIX = "cursor-"


def _decode_cursor(cursor: Any) -> int:
    if cursor in (None, "", 0):
        return 0
    if isinstance(cursor, int):
        return max(0, cursor)
    cursor_str = str(cursor)
    # Tolerate either the encoded form ("cursor-N") or a bare integer
    # (helpful when developers hand-craft cursors during debugging).
    if cursor_str.startswith(_CURSOR_PREFIX):
        cursor_str = cursor_str[len(_CURSOR_PREFIX):]
    try:
        return max(0, int(cursor_str))
    except (TypeError, ValueError):
        return 0


def _encode_cursor(offset: int) -> str:
    return f"{_CURSOR_PREFIX}{offset}"


def _build_response(
    prep: PreparedRequest, *, status: int, payload: dict[str, Any]
) -> Response:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    rec = ResponseRecord(
        status_code=status,
        headers={"Content-Type": "application/json"},
        body_text=body.decode("utf-8"),
        body_b64=None,
        encoding="utf-8",
        url=prep.url,
    )
    return response_from_record(rec, prep)
