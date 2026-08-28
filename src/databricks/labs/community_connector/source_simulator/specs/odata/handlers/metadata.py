"""Custom simulator handler for the OData v4 ``$metadata`` endpoint.

The connector queries ``$metadata`` at startup to discover entity sets,
schemas, and primary keys (CSDL XML format). The simulator serves a
fixed Northwind-shaped CSDL document from the corpus directory so
discovery tests have a predictable target.

The CSDL XML is intentionally a separate file (not inlined) so editing
the fixture doesn't require touching Python.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import (
    ResponseRecord,
)
from databricks.labs.community_connector.source_simulator.interceptor import (
    response_from_record,
)


_METADATA_PATH = Path(__file__).resolve().parent.parent / "corpus" / "metadata.xml"


def serve_metadata(
    prep: PreparedRequest,
    spec: Any,
    corpus: Any,  # noqa: ARG001
) -> Response:
    """Return the static CSDL XML document for the simulated Northwind service."""
    xml = _METADATA_PATH.read_text(encoding="utf-8")
    rec = ResponseRecord(
        status_code=200,
        headers={"Content-Type": "application/xml; charset=utf-8"},
        body_text=xml,
        body_b64=None,
        encoding="utf-8",
        url=prep.url,
    )
    return response_from_record(rec, prep)
