"""Custom handler for ``GET /.well-known/databricks-config``.

The ``databricks-sdk`` issues this request on first contact with a
workspace to discover the workspace's OIDC endpoint, account id, and
related metadata.  The HL7 v2 connector itself never reads the body,
but if the call fails the SDK aborts before any SQL statement reaches
the simulator — so we need a stand-in that returns a well-shaped
response without contacting Databricks.

Returning synthetic identifiers (``sim-*``) keeps real workspace /
account ids out of corpus and cassette files.
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


def serve_workspace_config(prep: PreparedRequest, spec, corpus) -> Response:  # noqa: ARG001
    body = {
        "oidc_endpoint": "https://sim-workspace.cloud.databricks.com/oidc",
        "account_id": "sim-account-id",
        "workspace_id": "sim-workspace-id",
        "cloud_provider": "aws",
        "host_type": "workspace",
        "token_federation_default_oidc_audiences": [
            "https://sim-workspace.cloud.databricks.com/oidc/v1/token",
        ],
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
