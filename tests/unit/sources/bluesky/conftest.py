"""Offline HTTP simulator serving actual jss0 bytes through requests.Session.send."""

import io
import json
import struct
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import cbor2
import pytest
import requests
import xxhash
import zstandard


def block_bytes(events):
    payloads = [
        cbor2.dumps(e["payload"], canonical=True) if e["payload"] is not None else b""
        for e in events
    ]
    blobs = [
        [e.get(key, "").encode() for e in events] for key in ("collection", "did", "rkey", "rev")
    ] + [payloads]
    n = len(events)
    body = struct.pack("<I", n)
    for fmt, values in (
        ("Q", [e["seq"] for e in events]),
        ("q", [e.get("witnessed", 1700000000000000) for e in events]),
        ("q", [e.get("indexed", 0) for e in events]),
        ("B", [e["kind"] for e in events]),
    ):
        body += struct.pack(f"<{n}{fmt}", *values)
    for fmt, column in zip(("B", "H", "B", "B", "I"), blobs):
        body += struct.pack(f"<{n}{fmt}", *(len(value) for value in column))
    body += b"".join(value for column in blobs for value in column)
    return zstandard.ZstdCompressor(write_checksum=True).compress(body), len(body)


def segment_bytes(events):
    header = bytearray(256)
    header[:4] = b"jss0"
    struct.pack_into("<H", header, 12, 1)
    # Multiple blocks deliberately share query bounds and false-positive filters.
    blocks = [events[i : i + 2] for i in range(0, len(events), 2)]
    struct.pack_into("<II", header, 14, len(blocks), len(events))
    struct.pack_into("<QQ", header, 26, events[0]["seq"], events[-1]["seq"])
    body = b""
    footer = b""
    for group in blocks:
        frame, size = block_bytes(group)
        footer += struct.pack(
            "<QIIIQQqq",
            256 + len(body),
            len(frame),
            size,
            len(group),
            group[0]["seq"],
            group[-1]["seq"],
            0,
            0,
        )
        body += struct.pack("<Q", len(frame)) + frame
    start = 256 + len(body)
    struct.pack_into("<Q", header, 58, start)
    struct.pack_into("<Q", header, 90, start)
    checksum = xxhash.xxh3_64(bytes(header[12:]) + footer).hexdigest()
    struct.pack_into("<Q", header, 4, int(checksum, 16))
    return bytes(header) + body + footer, checksum


def response(status, body=b"", headers=None):
    result = requests.Response()
    result.status_code = status
    result.headers.update(headers or {})
    result.raw = io.BytesIO(body)
    return result


class ArchiveService:
    """In-memory Jetstream archive server used by offline transport tests.

    Attributes
    ----------
    events : list[dict]
        Synthetic event records encoded in the fixture archive.
    data : bytes
        Encoded segment bytes returned by ranged archive requests.
    checksum : str
        ETag and metadata checksum for the current synthetic segment.
    tip : int
        Current sealed sequence tip.
    page_span : int
        Maximum planner sequence progress returned per request.
    mode : str
        Planner segment mode, either segment or blocks.
    calls : list
        Captured HTTP requests.
    failures : list
        Queued responses or exceptions returned by subsequent requests.
    plan_override : callable or None
        Optional mutation applied to generated planner responses.
    """

    def __init__(self, events):
        self.events = events
        self.data, self.checksum = segment_bytes(events)
        self.tip = events[-1]["seq"]
        self.page_span = 3
        self.mode = "segment"
        self.calls = []
        self.failures = []
        self.plan_override = None

    def send(self, request, **kwargs):
        self.calls.append(request)
        if self.failures:
            failure = self.failures.pop(0)
            if isinstance(failure, Exception):
                raise failure
            return failure
        if request.url.endswith("planSnapshot"):
            query = json.loads(request.body)
            after = query["afterSeq"]
            tip = min(query.get("beforeSeq", self.tip), self.tip)
            through = min(after + self.page_span, tip)
            entry = {
                "name": "seg_0000000000.jss",
                "index": 0,
                "checksum": self.checksum,
                "minSeq": 1,
                "maxSeq": self.tip,
                "mode": self.mode,
            }
            if self.mode == "blocks":
                entry["blocks"] = [{"first": 0, "last": (len(self.events) - 1) // 2}]
            plan = {
                "sealedTipSeq": tip,
                "plannedThroughSeq": through,
                "segments": [entry] if tip > after else [],
                "stats": {},
            }
            if self.plan_override:
                self.plan_override(plan)
            return response(200, json.dumps(plan).encode())
        query = parse_qs(urlsplit(request.url).query)
        assert query["name"] == ["seg_0000000000.jss"]
        if request.headers["If-Match"] != f'"{self.checksum}"':
            return response(412)
        first, last = map(int, request.headers["Range"].removeprefix("bytes=").split("-"))
        return response(
            206,
            self.data[first : last + 1],
            {
                "ETag": f'"{self.checksum}"',
                "Content-Range": f"bytes {first}-{last}/{len(self.data)}",
            },
        )


@pytest.fixture
def events():
    return json.loads((Path(__file__).parent / "fixtures/events.json").read_text())


@pytest.fixture
def service(monkeypatch, events):
    server = ArchiveService(events)
    monkeypatch.setattr(
        requests.Session, "send", lambda self, request, **kw: server.send(request, **kw)
    )
    monkeypatch.setattr("jetstream_lakehouse.transport.time.sleep", lambda _: None)
    return server
