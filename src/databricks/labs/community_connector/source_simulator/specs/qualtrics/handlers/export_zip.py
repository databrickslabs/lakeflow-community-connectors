"""Custom handler for qualtrics' GET ``/survey-responses/{file_id}/file``.

The connector expects a binary ZIP body containing a JSON file. The
simulator returns an empty-but-valid ZIP so the connector parses it and
yields zero responses — exercising the full export-poll-download path
without needing real survey data.
"""

from __future__ import annotations

import io
import zipfile

from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import ResponseRecord
from databricks.labs.community_connector.source_simulator.interceptor import (
    response_from_record,
)


def _empty_zip() -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("responses.json", '{"responses": []}')
    return buf.getvalue()


_EMPTY_ZIP_BYTES = _empty_zip()


def serve_zip(prep: PreparedRequest, spec, corpus) -> Response:  # noqa: ARG001
    rec = ResponseRecord(
        status_code=200,
        headers={"Content-Type": "application/zip"},
        body_text=None,
        body_b64=None,
        encoding=None,
        url=prep.url,
    )
    resp = response_from_record(rec, prep)
    resp._content = _EMPTY_ZIP_BYTES  # noqa: SLF001
    resp.headers["Content-Length"] = str(len(_EMPTY_ZIP_BYTES))
    return resp
