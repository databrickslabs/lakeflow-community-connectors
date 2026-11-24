"""Model HubSpot legacy contact-list ID discovery and mapping offline."""

from __future__ import annotations

import json
from typing import Any
from urllib.parse import parse_qs, urlparse

from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.specs.hubspot.handlers.batch_read import (
    _build_response,
)


def _list_id_pairs(corpus: Any) -> list[tuple[str, str]]:
    """Return distinct synthetic legacy/current IDs for each list fixture."""
    return [
        (str(1_000_000 + index), str(record["listId"]))
        for index, record in enumerate(corpus.get("contact_lists") or [], start=1)
        if isinstance(record, dict) and record.get("listId") is not None
    ]


def serve_legacy_lists(
    prep: PreparedRequest, spec: Any, corpus: Any
) -> Response:  # noqa: ARG001
    """Return an offset-paged v1 catalog backed by contact-list fixtures."""
    query = parse_qs(urlparse(prep.url or "").query)
    start = int(query.get("offset", ["0"])[0])
    count = int(query.get("count", ["20"])[0])
    records = [{"listId": legacy} for legacy, _ in _list_id_pairs(corpus)]
    page = records[start : start + count]
    next_offset = start + len(page)
    has_more = next_offset < len(records)
    payload = {"lists": page, "has-more": has_more}
    if has_more:
        payload["offset"] = next_offset
    return _build_response(prep, status=200, payload=payload)


def serve_list_id_mapping(
    prep: PreparedRequest, spec: Any, corpus: Any
) -> Response:  # noqa: ARG001
    """Map synthetic legacy IDs to current IDs and report unknown values."""
    requested = _parse_id_list(prep.body)
    known = dict(_list_id_pairs(corpus))
    mappings = [
        {"listId": known[value], "legacyListId": value}
        for value in requested
        if value in known
    ]
    missing = [value for value in requested if value not in known]
    return _build_response(
        prep,
        status=200,
        payload={
            "legacyListIdsToIdsMapping": mappings,
            "missingLegacyListIds": missing,
        },
    )


def _parse_id_list(raw: Any) -> list[str]:
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    try:
        data = json.loads(raw)
    except (TypeError, ValueError):
        data = None
    if not isinstance(data, list):
        return []
    return [str(value) for value in data]
