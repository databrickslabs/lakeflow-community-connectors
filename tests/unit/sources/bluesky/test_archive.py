import copy
import json
import struct
from pathlib import Path

import cbor2
import pytest
import xxhash
import zstandard
from conftest import block_bytes
from jetstream_lakehouse.archive import cid, decode_block, decode_payload, segment_events
from jetstream_lakehouse.errors import ProtocolError
from jetstream_lakehouse.transport import Transport

from databricks.labs.community_connector.sources.bluesky.options import Options


def reseal(service, change):
    """Keep the fixture's checksum valid so corrupt metadata reaches the decoder."""
    data = bytearray(service.data)
    footer_start = struct.unpack_from("<Q", data, 58)[0]
    change(data)
    service.checksum = xxhash.xxh3_64(data[12:256] + data[footer_start:]).hexdigest()
    struct.pack_into("<Q", data, 4, int(service.checksum, 16))
    service.data = bytes(data)


def read_segment(service, after=0, through=7, blocks=None):
    entry = {
        "name": "seg_0000000000.jss",
        "checksum": service.checksum,
        "mode": "blocks" if blocks is not None else "segment",
        "blocks": blocks or [],
    }
    with Transport(Options.parse({}, {})) as transport:
        return list(segment_events(transport, entry, after, through, False))


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


def test_block_requires_zstd_content_checksum(events):
    frame, size = block_bytes(events[:1])
    body = zstandard.ZstdDecompressor().decompress(frame)
    unchecked = zstandard.ZstdCompressor(write_checksum=False).compress(body)
    with pytest.raises(ProtocolError, match="content checksum"):
        list(decode_block(unchecked, size, False))


def test_truncated_columns(events):
    frame, _ = block_bytes(events)
    body = zstandard.ZstdDecompressor().decompress(frame)[:-1]
    damaged = zstandard.ZstdCompressor(write_checksum=True).compress(body)
    with pytest.raises(ProtocolError):
        list(decode_block(damaged, len(body), True))


def test_trailing_block_bytes(events):
    frame, _ = block_bytes(events[:1])
    body = zstandard.ZstdDecompressor().decompress(frame) + b"extra"
    damaged = zstandard.ZstdCompressor(write_checksum=True).compress(body)
    with pytest.raises(ProtocolError, match="Trailing bytes"):
        list(decode_block(damaged, len(body), False))


def test_segment_reads_only_selected_block(service):
    rows = read_segment(service, after=2, through=4, blocks=[{"first": 1, "last": 1}])
    assert [row["seq"] for row in rows] == [3, 4]
    # Header + footer + one frame, rather than downloading every block.
    assert len(service.calls) == 3
    assert service.calls[-1].headers["Range"].startswith("bytes=")


def test_invalid_footer_offset_fails_before_fetching_footer(service):
    reseal(service, lambda data: struct.pack_into("<Q", data, 90, len(data)))
    with pytest.raises(ProtocolError, match="footer offsets"):
        read_segment(service)
    assert len(service.calls) == 1


def test_block_index_must_match_decoded_rows(service):
    footer_start = struct.unpack_from("<Q", service.data, 58)[0]
    reseal(service, lambda data: struct.pack_into("<I", data, footer_start + 16, 3))
    with pytest.raises(ProtocolError, match="disagree with sealed index"):
        read_segment(service)


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
