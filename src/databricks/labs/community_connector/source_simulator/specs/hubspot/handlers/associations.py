"""Serve complete association batches from the selected parent corpus."""

import re

from databricks.labs.community_connector.source_simulator.specs.hubspot.handlers.batch_read import (
    _build_response,
    _parse_body,
)


def serve_associations(prep, spec, corpus):  # pylint: disable=unused-argument
    match = re.search(r"/crm/associations/2026-09/([^/]+)/([^/]+)/batch/read", prep.url or "")
    if not match:
        return _build_response(prep, status=404, payload={"message": "unknown path"})
    from_type, to_type = match.groups()
    parents = {str(record["id"]): record for record in corpus.get(from_type) or []}
    results = []
    for item in _parse_body(prep.body).get("inputs", []):
        parent_id = str(item["id"])
        if parent_id not in parents:
            continue
        parent = parents[parent_id]
        links = (parent.get("associations") or {}).get(to_type, parent.get(to_type, []))
        if isinstance(links, dict):
            links = links.get("results", [])
        results.append({
            "from": {"id": parent_id},
            "to": [{"toObjectId": str(link["id"] if isinstance(link, dict) else link)} for link in links],
        })
    return _build_response(prep, status=200, payload={"status": "COMPLETE", "results": results})
