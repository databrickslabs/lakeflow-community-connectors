"""Simulator handlers for the Databricks endpoints the MongoDB connector calls.

Both endpoints return one JSON object (no records array, no pagination).
The corpus holds the response shape; the handlers fill in what depends on
the clock (the credential expiry) and reject requests the real API would
reject (missing credential name, wrong grant type).
"""

from __future__ import annotations

import copy
import json
import time
from typing import Any
from urllib.parse import parse_qs

from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import ResponseRecord
from databricks.labs.community_connector.source_simulator.interceptor import (
    response_from_record,
)

# Temporary keys from Unity Catalog last at most one hour.
_CREDENTIAL_LIFETIME_SECONDS = 3600


def oauth_token(prep: PreparedRequest, spec: Any, corpus: Any) -> Response:
    del spec
    form = parse_qs(_body_text(prep))
    if form.get("grant_type") != ["client_credentials"]:
        return _response(prep, 400, {"error": "invalid_request",
                                     "error_description": "unsupported grant_type"})
    return _response(prep, 200, copy.deepcopy(_single(corpus, "oauth_token")))


def temporary_service_credentials(prep: PreparedRequest, spec: Any, corpus: Any) -> Response:
    del spec
    try:
        body = json.loads(_body_text(prep) or "{}")
    except ValueError:
        body = {}
    if not body.get("credential_name"):
        return _response(prep, 400, {"error_code": "INVALID_PARAMETER_VALUE",
                                     "message": "credential_name is required"})
    payload = copy.deepcopy(_single(corpus, "temporary_service_credentials"))
    payload["expiration_time"] = int((time.time() + _CREDENTIAL_LIFETIME_SECONDS) * 1000)
    return _response(prep, 200, payload)


def _single(corpus: Any, name: str) -> dict:
    records = corpus.get(name)
    return records[0] if isinstance(records, list) else records


def _body_text(prep: PreparedRequest) -> str:
    body = prep.body
    if isinstance(body, bytes):
        return body.decode("utf-8")
    return body or ""


def _response(prep: PreparedRequest, status: int, payload: dict) -> Response:
    text = json.dumps(payload, ensure_ascii=False)
    rec = ResponseRecord(status_code=status, headers={"Content-Type": "application/json"},
                         body_text=text, body_b64=None, encoding="utf-8", url=prep.url)
    return response_from_record(rec, prep)
