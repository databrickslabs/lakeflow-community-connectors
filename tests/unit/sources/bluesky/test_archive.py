import copy
import json
import struct
from pathlib import Path

import cbor2
import pytest
import zstandard
from conftest import block_bytes

from databricks.labs.community_connector.sources.bluesky.archive import (
    cid,
    decode_block,
    decode_payload,
)
from databricks.labs.community_connector.sources.bluesky.errors import ProtocolError
from databricks.labs.community_connector.sources.bluesky.options import Options


def test_dag_cbor_bytes_links_and_large_integer():
    digest = b"\x01\x71\x12\x20" + bytes(32)
    data = cbor2.dumps(
        {"blob": b"hello", "link": cbor2.CBORTag(42, b"\0" + digest), "large": 9007199254740993}
    )
    decoded = decode_payload(data)
    assert decoded["blob"] == {"$bytes": "aGVsbG8"}
    assert decoded["link"]["$link"].startswith("bafy")
    assert decoded["large"] == 9007199254740993


@pytest.mark.parametrize(
    "payload",
    [
        b"",
        b"\xff",
        cbor2.dumps([1]),
        cbor2.dumps({1: "value"}),
        cbor2.dumps({"x": 1.5}),
        cbor2.dumps({}) + b"x",
    ],
)
def test_malformed_payload(payload):
    with pytest.raises(ProtocolError):
        decode_payload(payload)


@pytest.mark.parametrize(
    "change",
    [
        {"kind": 99},
        {"seq": 0},
        {"did": ""},
        {"payload": None},
        {"collection": ""},
    ],
)
def test_unknown_or_malformed_events(events, change):
    events = copy.deepcopy(events[:1])
    events[0].update(change)
    frame, size = block_bytes(events)
    with pytest.raises(ProtocolError):
        list(decode_block(frame, size, True))


def test_corrupt_frame(events):
    frame, size = block_bytes(events)
    frame = frame[:-1] + bytes([frame[-1] ^ 1])
    with pytest.raises(ProtocolError):
        list(decode_block(frame, size, True))


def test_truncated_columns(events):
    frame, _ = block_bytes(events)
    body = zstandard.ZstdDecompressor().decompress(frame)[:-1]
    damaged = zstandard.ZstdCompressor(write_checksum=True).compress(body)
    with pytest.raises(ProtocolError):
        list(decode_block(damaged, len(body), True))


def test_record_json_and_cid(events):
    frame, size = block_bytes(events[:1])
    row = next(decode_block(frame, size, True))
    assert json.loads(row["record"]) == events[0]["payload"]
    assert row["cid"] == cid(cbor2.dumps(events[0]["payload"], canonical=True))


def test_official_writer_archive_through_real_http_reader(service):
    from databricks.labs.community_connector.sources.bluesky import BlueskyLakeflowConnect

    service.data = (Path(__file__).parent / "fixtures/native.jss").read_bytes()
    service.checksum = f"{struct.unpack_from('<Q', service.data, 4)[0]:016x}"
    service.tip = 3
    records, offset = BlueskyLakeflowConnect(Options.parse({}, {})).read_table("events", {}, {})
    rows = list(records)
    assert [r["operation"] for r in rows] == ["create", "update", "delete"]
    assert [r["seq"] for r in rows] == [1, 2, 3]
    assert json.loads(rows[0]["record"]) == {"text": "hello"}
    assert offset["seq"] == 3
