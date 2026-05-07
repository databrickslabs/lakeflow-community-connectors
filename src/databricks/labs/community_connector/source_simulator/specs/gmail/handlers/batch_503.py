"""Force gmail batch endpoint to fall back to sequential.

Gmail's batch API would respond with multipart/mixed containing many JSON
sub-responses. Modeling that is more work than it's worth — the connector
already has a fallback path that fires when the batch response is non-200,
calling each endpoint individually. We return 503 here so the fallback
kicks in; the per-endpoint specs handle the sequential calls.
"""

from __future__ import annotations

from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import ResponseRecord
from databricks.labs.community_connector.source_simulator.interceptor import (
    response_from_record,
)


def respond_503(prep: PreparedRequest, spec, corpus) -> Response:  # noqa: ARG001
    rec = ResponseRecord(
        status_code=503,
        headers={"Content-Type": "text/plain"},
        body_text="batch unavailable in simulator",
        body_b64=None,
        encoding="utf-8",
        url=prep.url,
    )
    return response_from_record(rec, prep)
