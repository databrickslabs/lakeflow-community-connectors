"""Serve a real-format jss0 fixture to Databricks' shared HTTP simulator."""

import io
import json
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from requests import PreparedRequest, Response


def _response(request: PreparedRequest, status: int, body: bytes, headers: dict) -> Response:
    response = Response()
    response.status_code = status
    response.headers.update(headers)
    response.raw = io.BytesIO(body)
    response.url = request.url
    response.request = request
    return response


def serve_plan(request: PreparedRequest, _spec, corpus) -> Response:
    """Return one bounded plan over the sealed fixture's sequence interval."""
    archive = corpus.get("archive")
    payload = json.loads(request.body or b"{}")
    after = payload["afterSeq"]
    before = min(payload.get("beforeSeq", archive["maxSeq"]), archive["maxSeq"])
    if after < 0 or before < after:
        return _response(request, 400, b'{"error":"InvalidCursor"}', {})
    entry = {
        "name": archive["name"],
        "index": 0,
        "checksum": archive["checksum"],
        "minSeq": archive["minSeq"],
        "maxSeq": archive["maxSeq"],
        "mode": "segment",
    }
    plan = {
        "sealedTipSeq": before,
        "plannedThroughSeq": before,
        "segments": [entry] if after < before else [],
        "stats": {},
    }
    return _response(request, 200, json.dumps(plan).encode(), {"Content-Type": "application/json"})


def serve_segment(request: PreparedRequest, _spec, corpus) -> Response:
    """Honor Range and If-Match exactly as the archive transport expects."""
    archive = corpus.get("archive")
    query = parse_qs(urlsplit(request.url).query)
    if query.get("name") != [archive["name"]]:
        return _response(request, 404, b"", {})
    if request.headers.get("If-Match") != f'"{archive["checksum"]}"':
        return _response(request, 412, b"", {})
    range_header = request.headers.get("Range", "")
    if not range_header.startswith("bytes="):
        return _response(request, 416, b"", {})
    try:
        first, last = (int(value) for value in range_header[6:].split("-", 1))
    except ValueError:
        return _response(request, 416, b"", {})
    fixture = Path(__file__).resolve().parents[1] / "corpus" / archive["fixture"]
    data = fixture.read_bytes()
    if first < 0 or last < first or last >= len(data):
        return _response(request, 416, b"", {})
    return _response(
        request,
        206,
        data[first : last + 1],
        {
            "ETag": f'"{archive["checksum"]}"',
            "Content-Range": f"bytes {first}-{last}/{len(data)}",
            "Content-Type": "application/octet-stream",
        },
    )
