"""Jetstream jss0/v1 decoder, based on the published columnar format.

Read header/footer and selected compressed blocks using exact HTTP ranges.
Never fetch a full ~256 MB segment to retrieve a small sequence window.
"""

import base64
import hashlib
import io
import json
import struct
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator, Mapping

import cbor2
import xxhash
import zstandard

from databricks.labs.community_connector.sources.bluesky.errors import ProtocolError
from databricks.labs.community_connector.sources.bluesky.transport import Transport

MAX_BLOCK = 32 * 1024 * 1024
KINDS = {
    1: ("commit", "create"),
    2: ("commit", "update"),
    3: ("commit", "delete"),
    4: ("identity", None),
    5: ("account", None),
    6: ("sync", None),
    7: ("commit", "create"),
}


def cid(data: bytes) -> str:
    # CIDv1 + dag-cbor (0x71) + sha2-256 (0x12, digest length 0x20).
    return "b" + base64.b32encode(
        b"\x01\x71\x12\x20" + hashlib.sha256(data).digest()
    ).decode().lower().rstrip("=")


def json_shape(value: Any) -> Any:
    if isinstance(value, cbor2.CBORTag):
        if value.tag != 42 or not isinstance(value.value, bytes) or value.value[:1] != b"\0":
            raise ProtocolError("Unsupported DAG-CBOR tag")
        return {"$link": "b" + base64.b32encode(value.value[1:]).decode().lower().rstrip("=")}
    if isinstance(value, bytes):
        return {"$bytes": base64.b64encode(value).decode().rstrip("=")}
    if isinstance(value, dict):
        if any(not isinstance(k, str) for k in value):
            raise ProtocolError("DAG-CBOR object contains a non-string key")
        return {k: json_shape(v) for k, v in value.items()}
    if isinstance(value, list):
        return [json_shape(v) for v in value]
    if value is None or isinstance(value, (str, bool, int)):
        return value
    raise ProtocolError("Unsupported DAG-CBOR value")


def decode_payload(payload: bytes) -> dict[str, Any]:
    try:
        stream = io.BytesIO(payload)
        value = cbor2.CBORDecoder(stream).decode()
        if stream.read(1) or not isinstance(value, dict):
            raise ProtocolError("Event payload must contain one DAG-CBOR object")
        return json_shape(value)
    except (ValueError, EOFError, RecursionError, cbor2.CBORDecodeError):
        raise ProtocolError("Malformed DAG-CBOR event payload") from None


def timestamp(microseconds: int) -> str:
    try:
        return (
            datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=microseconds)
        ).isoformat()
    except (ValueError, OverflowError):
        raise ProtocolError("Event timestamp outside supported range") from None


def decode_block(frame: bytes, expected_size: int, include_raw: bool) -> Iterator[dict[str, Any]]:
    if expected_size > MAX_BLOCK:
        raise ProtocolError("Decompressed block exceeds safety limit")
    try:
        params = zstandard.get_frame_parameters(frame)
        if not params.has_checksum:
            raise ProtocolError("Archive block lacks a content checksum")
        # A declared frame size must not override our allocation cap.
        if params.content_size not in (expected_size, zstandard.CONTENTSIZE_UNKNOWN):
            raise ProtocolError("Unexpected Zstandard content size")
        data = zstandard.ZstdDecompressor(max_window_size=MAX_BLOCK // 1024).decompress(
            frame, max_output_size=MAX_BLOCK, allow_extra_data=False
        )
    except zstandard.ZstdError:
        raise ProtocolError("Corrupt or unsupported Zstandard frame") from None
    if len(data) != expected_size or len(data) < 4:
        raise ProtocolError("Invalid block length")
    count = struct.unpack_from("<I", data)[0]
    if count > 262144 or 4 + 34 * count > len(data):
        raise ProtocolError("Invalid block event count")
    position = 4

    def column(fmt: str) -> tuple[int, ...]:
        nonlocal position
        values = struct.unpack_from(f"<{count}{fmt}", data, position)
        position += count * struct.calcsize(fmt)
        return values

    sequences, witnessed, indexed = column("Q"), column("q"), column("q")
    kinds = column("B")
    lengths = [column(fmt) for fmt in ("B", "H", "B", "B", "I")]
    columns = []
    for sizes in lengths:
        values = []
        for size in sizes:
            if position + size > len(data):
                raise ProtocolError("Truncated variable-length block column")
            values.append(data[position : position + size])
            position += size
        columns.append(values)
    if position != len(data):
        raise ProtocolError("Trailing bytes in archive block")
    previous = 0
    for i in range(count):
        seq = sequences[i]
        if not previous < seq <= (1 << 63) - 1:
            raise ProtocolError("Non-increasing or invalid sequence in block")
        previous = seq
        if kinds[i] not in KINDS:
            raise ProtocolError("Unknown archive event kind; upgrade the decoder")
        kind, operation = KINDS[kinds[i]]
        try:
            collection, did, rkey, rev = [c[i].decode("utf-8") for c in columns[:4]]
        except UnicodeError:
            raise ProtocolError("Invalid UTF-8 metadata") from None
        if not did.startswith("did:") or (kind == "commit" and (not collection or not rkey)):
            raise ProtocolError("Missing event identity fields")
        payload = columns[4][i]
        decoded = decode_payload(payload) if payload else None
        if (kind == "commit" and operation != "delete") or kind != "commit":
            if decoded is None:
                raise ProtocolError("Missing event payload")
        event = {
            "seq": seq,
            "event_time": timestamp(indexed[i] or witnessed[i]),
            "witnessed_at": timestamp(witnessed[i]),
            "did": did,
            "kind": kind,
            "operation": operation,
            "collection": collection or None,
            "rkey": rkey or None,
            "cid": cid(payload) if kind == "commit" and operation != "delete" else None,
            "rev": rev or (decoded or {}).get("rev"),
            "is_resync": kinds[i] == 7,
            "record": json.dumps(decoded, separators=(",", ":"))
            if kind == "commit" and operation != "delete"
            else None,
            "event_payload": json.dumps(decoded, separators=(",", ":")),
            "raw_payload": base64.b64encode(payload).decode() if include_raw else None,
        }
        yield event


def segment_events(
    transport: "Transport",
    entry: Mapping[str, Any],
    after: int,
    through: int,
    include_raw: bool,
) -> Iterator[dict[str, Any]]:
    name, checksum = entry["name"], entry["checksum"]
    header, total = transport.range(name, checksum, 0, 255)
    if (
        header[:4] != b"jss0"
        or struct.unpack_from("<H", header, 12)[0] != 1
        or f"{struct.unpack_from('<Q', header, 4)[0]:016x}" != checksum
    ):
        raise ProtocolError("Unsupported or inconsistent sealed segment header")
    count = struct.unpack_from("<I", header, 14)[0]
    footer_start = struct.unpack_from("<Q", header, 58)[0]
    index_start = struct.unpack_from("<Q", header, 90)[0]
    if not 256 <= footer_start <= index_start <= total - count * 52:
        raise ProtocolError("Invalid archive footer offsets")
    footer, _ = transport.range(name, checksum, footer_start, total - 1)
    if xxhash.xxh3_64(header[12:] + footer).hexdigest() != checksum:
        raise ProtocolError("Archive metadata checksum mismatch")
    ranges = entry.get("blocks", [])
    if any(r["last"] >= count for r in ranges):
        raise ProtocolError("Planned block range exceeds segment block count")
    for index in range(count):
        if entry["mode"] == "blocks" and not any(r["first"] <= index <= r["last"] for r in ranges):
            continue
        transport.remaining()
        offset, compressed, uncompressed, events, low, high, _, _ = struct.unpack_from(
            "<QIIIQQqq", footer, index_start - footer_start + index * 52
        )
        if high <= after or low > through or events == 0:
            continue
        if (
            offset < 256
            or compressed == 0
            or offset + 8 + compressed > footer_start
            or uncompressed > MAX_BLOCK
            or events > 262144
        ):
            raise ProtocolError("Invalid archive block index")
        frame, _ = transport.range(name, checksum, offset + 8, offset + 7 + compressed)
        rows = list(decode_block(frame, uncompressed, include_raw))
        if len(rows) != events or rows[0]["seq"] != low or rows[-1]["seq"] != high:
            raise ProtocolError("Block rows disagree with sealed index")
        for event in rows:
            if after < event["seq"] <= through:
                yield event
