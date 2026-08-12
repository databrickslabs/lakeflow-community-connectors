"""Custom handler for the Google OAuth2 token endpoint.

The HL7 v2 GCP connector refreshes its access token in ``_init_gcp`` by
calling ``Credentials.refresh()``, which POSTs to
``https://oauth2.googleapis.com/token``. The simulator intercepts every
outbound HTTP, so we need a stand-in that returns a well-shaped token
response without contacting Google.
"""

from __future__ import annotations

import json

from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import (
    ResponseRecord,
)
from databricks.labs.community_connector.source_simulator.interceptor import (
    response_from_record,
)


def serve_token(prep: PreparedRequest, spec, corpus) -> Response:  # noqa: ARG001
    body = {
        "access_token": "sim-access-token",
        "token_type": "Bearer",
        "expires_in": 3600,
    }
    body_text = json.dumps(body)
    rec = ResponseRecord(
        status_code=200,
        headers={"Content-Type": "application/json"},
        body_text=body_text,
        body_b64=None,
        encoding="utf-8",
        url=prep.url,
    )
    return response_from_record(rec, prep)
