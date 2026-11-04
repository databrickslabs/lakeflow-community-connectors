"""Custom handler for the Azure DevOps reporting ``workitemrevisions`` endpoint.

Models the real API's watermark behaviour, which the generic spec can't express:

* With no ``continuationToken``, return every revision as the last batch. Like
  Microsoft's documented "first batch" example, the token comes back only
  inside ``nextLink`` (``...?continuationToken=<token>``), with no top-level
  ``continuationToken`` field.
* With that token, return an empty last batch: no revisions are newer than
  the watermark.

Without this, every call returned the full corpus regardless of the token, so
the connector could never observe its watermark doing anything.
"""

from __future__ import annotations

import json
from typing import List
from urllib.parse import urlencode

from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import (
    ResponseRecord,
)
from databricks.labs.community_connector.source_simulator.interceptor import (
    request_record_from_prepared,
    response_from_record,
)


def serve_workitem_revisions(prep: PreparedRequest, spec, corpus) -> Response:  # noqa: ARG001
    req = request_record_from_prepared(prep)
    revisions: List[dict] = corpus.get("workitem_revisions") or []
    watermark = f"sim-watermark-{len(revisions)}"

    values = [] if req.query.get("continuationToken") == watermark else revisions
    base = (prep.url or "").split("?", 1)[0]
    body = {
        "values": values,
        "nextLink": f"{base}?{urlencode({'continuationToken': watermark, 'api-version': '7.1'})}",
        "isLastBatch": True,
    }
    rec = ResponseRecord(
        status_code=200,
        headers={"Content-Type": "application/json"},
        body_text=json.dumps(body),
        body_b64=None,
        encoding="utf-8",
        url=prep.url,
    )
    return response_from_record(rec, prep)
