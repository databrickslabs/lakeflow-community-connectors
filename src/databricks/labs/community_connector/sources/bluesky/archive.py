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
from typing import Any, Iterator, Mapping, NamedTuple

import cbor2
import xxhash
import zstandard

from databricks.labs.community_connector.sources.bluesky.errors import ProtocolError
from databricks.labs.community_connector.sources.bluesky.transport import Transport

MAX_BLOCK = 32 * 1024 * 1024
HEADER_SIZE = 256
BLOCK_INDEX_ENTRY = struct.Struct("<QIIIQQqq")
MAX_BLOCK_EVENTS = 262144
# Per event: three 64-bit numbers, one kind byte, and five column lengths.
FIXED_EVENT_BYTES = 34
KINDS = {
    1: ("commit", "create"),
    2: ("commit", "update"),
    3: ("commit", "delete"),
    4: ("identity", None),
    5: ("account", None),
    6: ("sync", None),
    7: ("commit", "create"),
}


class BlockColumns(NamedTuple):
    """Fixed-width metadata and raw variable-width values from one decoded block.

    Attributes
    ----------
    sequences : tuple[int, ...]
        Event sequence numbers in archive order.
    witnessed : tuple[int, ...]
        Times Jetstream first saw each event, in Unix microseconds.
    indexed : tuple[int, ...]
        Display times in Unix microseconds; zero means use ``witnessed``.
    kinds : tuple[int, ...]
        Numeric event-kind codes from the block.
    collections : list[bytes]
        UTF-8 collection names, empty for events without a collection.
    dids : list[bytes]
        UTF-8 decentralized identifiers.
    rkeys : list[bytes]
        UTF-8 record keys, empty for non-record events.
    revs : list[bytes]
        UTF-8 revision identifiers when present.
    payloads : list[bytes]
        Raw DAG-CBOR event payloads; deletion events may have no payload.
    """

    sequences: tuple[int, ...]
    witnessed: tuple[int, ...]
    indexed: tuple[int, ...]
    kinds: tuple[int, ...]
    collections: list[bytes]
    dids: list[bytes]
    rkeys: list[bytes]
    revs: list[bytes]
    payloads: list[bytes]


class SegmentMetadata(NamedTuple):
    """Verified footer metadata for a sealed Jetstream segment.

    Attributes
    ----------
    count : int
        Number of block-index entries in the segment.
    footer_start : int
        Byte offset where the footer begins in the segment file.
    index_start : int
        Byte offset where the block index begins in the segment file.
    footer : bytes
        Footer bytes fetched from ``footer_start`` through the end of the file.
    """

    count: int
    footer_start: int
    index_start: int
    footer: bytes


class BlockLocation(NamedTuple):
    """Location and sequence bounds of one block in the sealed index.

    Attributes
    ----------
    offset : int
        Byte offset of the block's eight-byte compressed-length prefix.
    compressed : int
        Size of the following Zstandard frame in bytes.
    uncompressed : int
        Expected size of the decoded block in bytes.
    events : int
        Number of events declared by the block index.
    low : int
        First event sequence number in the block, inclusive.
    high : int
        Last event sequence number in the block, inclusive.
    """

    offset: int
    compressed: int
    uncompressed: int
    events: int
    low: int
    high: int


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


def _decompress_block(frame: bytes, expected_size: int) -> bytes:
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
    return data


def _read_variable_columns(
    data: bytes, position: int, lengths: list[tuple[int, ...]]
) -> list[list[bytes]]:
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
    return columns


def _read_block_columns(data: bytes) -> BlockColumns:
    count = struct.unpack_from("<I", data)[0]
    if count > MAX_BLOCK_EVENTS or 4 + FIXED_EVENT_BYTES * count > len(data):
        raise ProtocolError("Invalid block event count")
    position = 4

    def fixed_column(fmt: str) -> tuple[int, ...]:
        nonlocal position
        values = struct.unpack_from(f"<{count}{fmt}", data, position)
        position += count * struct.calcsize(fmt)
        return values

    # The block stores complete fixed-width columns first, then five length
    # columns, then the variable-width values grouped by column.
    sequences, witnessed, indexed = fixed_column("Q"), fixed_column("q"), fixed_column("q")
    kinds = fixed_column("B")
    # Length widths match collection, DID, rkey, rev, and payload respectively.
    lengths = [fixed_column(fmt) for fmt in ("B", "H", "B", "B", "I")]
    columns = _read_variable_columns(data, position, lengths)
    return BlockColumns(
        sequences,
        witnessed,
        indexed,
        kinds,
        columns[0],
        columns[1],
        columns[2],
        columns[3],
        columns[4],
    )


def _decode_event(columns: BlockColumns, index: int, include_raw: bool) -> dict[str, Any]:
    kind_code = columns.kinds[index]
    if kind_code not in KINDS:
        raise ProtocolError("Unknown archive event kind; upgrade the decoder")
    kind, operation = KINDS[kind_code]
    try:
        collection = columns.collections[index].decode("utf-8")
        did = columns.dids[index].decode("utf-8")
        rkey = columns.rkeys[index].decode("utf-8")
        rev = columns.revs[index].decode("utf-8")
    except UnicodeError:
        raise ProtocolError("Invalid UTF-8 metadata") from None
    if not did.startswith("did:") or (kind == "commit" and (not collection or not rkey)):
        raise ProtocolError("Missing event identity fields")
    payload = columns.payloads[index]
    decoded = decode_payload(payload) if payload else None
    if (kind != "commit" or operation != "delete") and decoded is None:
        raise ProtocolError("Missing event payload")
    record = kind == "commit" and operation != "delete"
    return {
        "seq": columns.sequences[index],
        "event_time": timestamp(columns.indexed[index] or columns.witnessed[index]),
        "witnessed_at": timestamp(columns.witnessed[index]),
        "did": did,
        "kind": kind,
        "operation": operation,
        "collection": collection or None,
        "rkey": rkey or None,
        "cid": cid(payload) if record else None,
        "rev": rev or (decoded or {}).get("rev"),
        "is_resync": kind_code == 7,
        "record": json.dumps(decoded, separators=(",", ":")) if record else None,
        "event_payload": json.dumps(decoded, separators=(",", ":")),
        "raw_payload": base64.b64encode(payload).decode() if include_raw else None,
    }


def decode_block(frame: bytes, expected_size: int, include_raw: bool) -> Iterator[dict[str, Any]]:
    columns = _read_block_columns(_decompress_block(frame, expected_size))
    previous = 0
    for index, seq in enumerate(columns.sequences):
        if not previous < seq <= (1 << 63) - 1:
            raise ProtocolError("Non-increasing or invalid sequence in block")
        previous = seq
        yield _decode_event(columns, index, include_raw)


def _read_segment_metadata(transport: Transport, name: str, checksum: str) -> SegmentMetadata:
    header, total = transport.range(name, checksum, 0, HEADER_SIZE - 1)
    if (
        header[:4] != b"jss0"
        or struct.unpack_from("<H", header, 12)[0] != 1
        or f"{struct.unpack_from('<Q', header, 4)[0]:016x}" != checksum
    ):
        raise ProtocolError("Unsupported or inconsistent sealed segment header")
    # jss0/v1 keeps these fields at fixed offsets in its 256-byte header.
    count = struct.unpack_from("<I", header, 14)[0]
    footer_start = struct.unpack_from("<Q", header, 58)[0]
    index_start = struct.unpack_from("<Q", header, 90)[0]
    if not HEADER_SIZE <= footer_start <= index_start <= total - count * BLOCK_INDEX_ENTRY.size:
        raise ProtocolError("Invalid archive footer offsets")
    footer, _ = transport.range(name, checksum, footer_start, total - 1)
    if xxhash.xxh3_64(header[12:] + footer).hexdigest() != checksum:
        raise ProtocolError("Archive metadata checksum mismatch")
    return SegmentMetadata(count, footer_start, index_start, footer)


def _block_location(metadata: SegmentMetadata, index: int) -> BlockLocation:
    # Each 52-byte footer entry points to a frame preceded by an 8-byte length.
    offset, compressed, uncompressed, events, low, high, _, _ = BLOCK_INDEX_ENTRY.unpack_from(
        metadata.footer,
        metadata.index_start - metadata.footer_start + index * BLOCK_INDEX_ENTRY.size,
    )
    return BlockLocation(offset, compressed, uncompressed, events, low, high)


def _read_block_rows(
    transport: Transport,
    entry: Mapping[str, Any],
    location: BlockLocation,
    metadata: SegmentMetadata,
    include_raw: bool,
) -> list[dict[str, Any]]:
    if (
        location.offset < HEADER_SIZE
        or location.compressed == 0
        or location.offset + 8 + location.compressed > metadata.footer_start
        or location.uncompressed > MAX_BLOCK
        or location.events > MAX_BLOCK_EVENTS
    ):
        raise ProtocolError("Invalid archive block index")
    frame, _ = transport.range(
        entry["name"],
        entry["checksum"],
        location.offset + 8,
        location.offset + 7 + location.compressed,
    )
    rows = list(decode_block(frame, location.uncompressed, include_raw))
    if (
        len(rows) != location.events
        or rows[0]["seq"] != location.low
        or rows[-1]["seq"] != location.high
    ):
        raise ProtocolError("Block rows disagree with sealed index")
    return rows


def segment_events(
    transport: "Transport",
    entry: Mapping[str, Any],
    after: int,
    through: int,
    include_raw: bool,
) -> Iterator[dict[str, Any]]:
    name, checksum = entry["name"], entry["checksum"]
    metadata = _read_segment_metadata(transport, name, checksum)
    ranges = entry.get("blocks", [])
    if any(r["last"] >= metadata.count for r in ranges):
        raise ProtocolError("Planned block range exceeds segment block count")
    for index in range(metadata.count):
        if entry["mode"] == "blocks" and not any(r["first"] <= index <= r["last"] for r in ranges):
            continue
        transport.remaining()
        location = _block_location(metadata, index)
        # The checkpoint window is (after, through]; the sealed block bounds
        # let us skip frames that cannot contain an event in that window.
        if location.high <= after or location.low > through or location.events == 0:
            continue
        for event in _read_block_rows(transport, entry, location, metadata, include_raw):
            if after < event["seq"] <= through:
                yield event
