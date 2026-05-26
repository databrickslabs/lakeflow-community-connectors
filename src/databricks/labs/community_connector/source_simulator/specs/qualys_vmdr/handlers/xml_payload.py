"""Custom handler to return XML corpus payloads as XML bodies."""

from __future__ import annotations

from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import ResponseRecord
from databricks.labs.community_connector.source_simulator.interceptor import (
    response_from_record,
)


def serve_xml(prep: PreparedRequest, spec, corpus) -> Response:
    value = corpus.get(spec.corpus) if spec.corpus else ""
    if isinstance(value, dict):
        xml_text = value.get("xml", "")
    elif isinstance(value, str):
        xml_text = value
    else:
        xml_text = ""

    rec = ResponseRecord(
        status_code=200,
        headers={"Content-Type": "application/xml"},
        body_text=xml_text,
        body_b64=None,
        encoding="utf-8",
        url=prep.url,
    )
    return response_from_record(rec, prep)
