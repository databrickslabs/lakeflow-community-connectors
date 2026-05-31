"""Simulator handler for OData v4 delta-tracking entity-set endpoints.

The OData delta protocol overlays three behaviors onto the same URL:

* **Bootstrap** — ``GET <entitySet>`` with ``Prefer: odata.track-changes``.
  Server responds 200 with the full snapshot, a ``Preference-Applied``
  header echoing the prefer back, and ``@odata.deltaLink`` pointing at
  ``<entitySet>?$deltatoken=<position>``.
* **Resume** — ``GET <delta link>``. Server interprets the
  ``$deltatoken`` query param, returns events since that position
  (regular entities for adds/changes, ``@removed`` blocks for
  deletes), and a fresh ``@odata.deltaLink`` for the next position.
* **Snapshot** — ``GET <entitySet>`` with no prefer / no token. Falls
  back to the plain entity-set handler.

This handler models all three. Corpus layout:

* ``<table>.json`` — initial snapshot (same shape used by the plain
  entity-set handler).
* ``<table>_changes.json`` — optional. Ordered list of event objects::

      [
        {"op": "add",    "after": {"CustomerID": "AAA1", ...}},
        {"op": "change", "after": {"CustomerID": "ALFKI", "City": "Munich"}},
        {"op": "remove", "key": {"CustomerID": "ANATR"}}
      ]

  Events are indexed 0..N-1. ``$deltatoken=K`` resumes from position K,
  inclusive. An empty changes file (or a missing one) is fine — the
  bootstrap deltaLink will still be emitted; subsequent resumes return
  zero events and the same deltaLink.

The handler is intentionally separate from ``entity_set.py`` so existing
non-delta tests continue to behave identically. It's not wired into the
default odata spec — opt-in per endpoint via ``handler:`` reference.

Endpoint-spec directives understood (set in ``endpoints.yaml`` for an
endpoint wired to this handler):

* ``delta_tracking_supported`` (bool, default True) — when False, the
  handler ignores ``Prefer: odata.track-changes`` (responds without
  ``Preference-Applied`` and without ``@odata.deltaLink``) so tests can
  exercise the connector's silent-ignore fallback path.
* ``simulate_410_after`` (int, optional) — return HTTP 410 Gone on the
  Kth delta-link follow-up GET. Used to test the connector's token-
  expiry re-bootstrap path.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlsplit

from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import (
    ResponseRecord,
)
from databricks.labs.community_connector.source_simulator.interceptor import (
    response_from_record,
)
from databricks.labs.community_connector.source_simulator.specs.odata.handlers.entity_set import (
    serve_entity_set,
)


# In-process counter for ``simulate_410_after``. Keyed by (path, table) so
# multiple endpoints don't share state. Reset per simulator instance via
# ``reset_delta_state()`` — tests that exercise this directive call that
# in setup.
_RESUME_CALLS: Dict[tuple, int] = {}


def reset_delta_state() -> None:
    """Clear the delta-resume-call counter. Call from test setup if
    ``simulate_410_after`` is in use."""
    _RESUME_CALLS.clear()


def serve_delta_entity_set(prep: PreparedRequest, spec: Any, corpus: Any) -> Response:
    """Three-way dispatch: bootstrap / resume / regular snapshot."""
    parsed = urlsplit(prep.url or "")
    query = {k: v[-1] for k, v in parse_qs(parsed.query, keep_blank_values=True).items()}

    deltatoken = query.get("$deltatoken")
    prefer = (prep.headers.get("Prefer") or "").lower()
    has_track_changes = "odata.track-changes" in prefer

    supported = _spec_bool(spec, "delta_tracking_supported", True)

    if deltatoken is not None:
        return _serve_resume(prep, spec, corpus, deltatoken, supported)
    if has_track_changes and supported:
        return _serve_bootstrap(prep, spec, corpus)
    # No prefer or server doesn't support it → standard snapshot. Without
    # the ``Preference-Applied`` header on the response the connector's
    # probe treats this as "delta not supported" and falls back.
    return serve_entity_set(prep, spec, corpus)


def _serve_bootstrap(prep: PreparedRequest, spec: Any, corpus: Any) -> Response:
    """Initial snapshot + deltaLink with token=0."""
    # Delegate snapshot generation to the standard handler so paging /
    # filter / orderby semantics stay aligned. Append the OData v4 delta
    # contract: ``Preference-Applied`` header and ``@odata.deltaLink``
    # pointing at ``$deltatoken=0`` for the first event-stream position.
    snapshot = serve_entity_set(prep, spec, corpus)

    body = json.loads(snapshot.text)
    body["@odata.deltaLink"] = _delta_link(prep.url or "", 0)
    new_text = json.dumps(body, ensure_ascii=False)

    headers = dict(snapshot.headers)
    headers["Preference-Applied"] = "odata.track-changes"

    rec = ResponseRecord(
        status_code=200,
        headers=headers,
        body_text=new_text,
        body_b64=None,
        encoding="utf-8",
        url=prep.url,
    )
    return response_from_record(rec, prep)


def _serve_resume(
    prep: PreparedRequest,
    spec: Any,
    corpus: Any,
    deltatoken: str,
    supported: bool,
) -> Response:
    """Replay events since the token's position; emit fresh deltaLink."""
    table = spec.corpus
    state_key = (spec.path, table)
    _RESUME_CALLS[state_key] = _RESUME_CALLS.get(state_key, 0) + 1
    call_count = _RESUME_CALLS[state_key]

    # ``simulate_410_after: K`` triggers a 410 on the Kth resume call —
    # validates the connector's token-expiry re-bootstrap path.
    expire_after = _spec_int(spec, "simulate_410_after", None)
    if expire_after is not None and call_count >= expire_after:
        rec = ResponseRecord(
            status_code=410,
            headers={"Content-Type": "application/json; charset=utf-8"},
            body_text='{"error": {"code": "deltatokenExpired"}}',
            body_b64=None,
            encoding="utf-8",
            url=prep.url,
        )
        return response_from_record(rec, prep)

    if not supported:
        # A server that doesn't actually support delta tracking
        # shouldn't have minted a token in the first place. Treat as
        # 400 to mirror real-world misconfigured services.
        rec = ResponseRecord(
            status_code=400,
            headers={"Content-Type": "application/json; charset=utf-8"},
            body_text='{"error": {"code": "invalidDeltaToken"}}',
            body_b64=None,
            encoding="utf-8",
            url=prep.url,
        )
        return response_from_record(rec, prep)

    try:
        position = int(deltatoken)
    except ValueError:
        position = 0

    changes = _changes_for(corpus, table)
    events = changes[position:]
    next_position = len(changes)

    value: List[Dict[str, Any]] = []
    for event in events:
        op = event.get("op")
        if op == "remove":
            entry: Dict[str, Any] = {"@removed": {"reason": "deleted"}}
            entry.update(event.get("key") or {})
            value.append(entry)
        else:  # "add" or "change"
            entry = dict(event.get("after") or {})
            value.append(entry)

    payload = {
        "@odata.context": _delta_context(prep.url or "", table),
        "value": value,
        "@odata.deltaLink": _delta_link(prep.url or "", next_position),
    }

    rec = ResponseRecord(
        status_code=200,
        headers={"Content-Type": "application/json; charset=utf-8"},
        body_text=json.dumps(payload, ensure_ascii=False),
        body_b64=None,
        encoding="utf-8",
        url=prep.url,
    )
    return response_from_record(rec, prep)


def _changes_for(corpus: Any, table: str) -> List[Dict[str, Any]]:
    """Look up the optional change-event corpus for ``table``."""
    if not table:
        return []
    changes = corpus.get(f"{table}_changes")
    if changes is None:
        return []
    if not isinstance(changes, list):
        raise ValueError(
            f"Corpus {table}_changes must be a list of change events; "
            f"got {type(changes).__name__}"
        )
    return changes


def _delta_link(current_url: str, position: int) -> str:
    """Construct ``<entitySet>?$deltatoken=<position>``.

    Drops any pre-existing ``$deltatoken`` / ``Prefer``-driven query
    params so the next call follows the link cleanly without inheriting
    bootstrap-only state.
    """
    parts = urlsplit(current_url)
    pairs: List[str] = []
    for chunk in parts.query.split("&"):
        if not chunk:
            continue
        if chunk.startswith("$deltatoken="):
            continue
        if chunk.startswith("$top=") or chunk.startswith("$skip="):
            continue
        pairs.append(chunk)
    pairs.append(f"$deltatoken={position}")
    return f"{parts.scheme}://{parts.netloc}{parts.path}?{'&'.join(pairs)}"


def _delta_context(current_url: str, table: str) -> str:
    """Construct ``@odata.context`` for a delta response."""
    parts = urlsplit(current_url)
    path = parts.path
    if path.endswith("/" + table):
        path = path[: -len(table)]
    elif path.endswith(table):
        path = path[: -len(table)]
    return f"{parts.scheme}://{parts.netloc}{path}$metadata#{table}/$delta"


def _spec_bool(spec: Any, attr: str, default: bool) -> bool:
    val = getattr(spec, attr, None)
    if val is None:
        return default
    return bool(val)


def _spec_int(spec: Any, attr: str, default: Optional[int]) -> Optional[int]:
    val = getattr(spec, attr, None)
    if val is None:
        return default
    try:
        return int(val)
    except (TypeError, ValueError):
        return default
