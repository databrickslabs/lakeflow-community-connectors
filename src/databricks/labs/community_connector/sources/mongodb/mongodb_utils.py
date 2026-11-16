"""Helpers for the MongoDB connector: value encoding, option parsing, offsets."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from bson.timestamp import Timestamp

# bson and pymongo are imported inside the functions that use them, never at
# module level. The merged single-file source is pickled by value, and in
# managed ingestion it is unpickled in a Python worker that has only the
# serverless base packages, not the pipeline's dependencies: a module-level
# reference to a bson name would fail there with "No module named 'bson'".
# See tests/unit/sources/mongodb/test_mongodb_no_driver_worker.py.

# Canonical Extended JSON keeps every BSON type distinguishable (Int64 vs
# Int32, Decimal128, Binary subtype, ...). It is used for the primary key and
# for offsets, where an exact, reversible encoding matters.
CANONICAL = "canonical"
# Relaxed Extended JSON reads naturally (numbers stay numbers, dates become
# {"$date": "2024-01-01T00:00:00Z"}) and is what we put into the document column.
RELAXED = "relaxed"

OFFSET_VERSION = 1

_JSON_OPTIONS: dict = {}


def _json_options(mode: str):
    options = _JSON_OPTIONS.get(mode)
    if options is None:
        from bson.json_util import JSONMode, JSONOptions

        json_mode = JSONMode.CANONICAL if mode == CANONICAL else JSONMode.RELAXED
        options = JSONOptions(json_mode=json_mode, tz_aware=True, tzinfo=timezone.utc)
        _JSON_OPTIONS[mode] = options
    return options


def bson_timestamp(time_: int, inc: int) -> Timestamp:
    """A BSON ``Timestamp`` (cluster time)."""
    from bson.timestamp import Timestamp as _Timestamp

    return _Timestamp(time_, inc)


def decode_errors() -> tuple:
    """Exceptions PyMongo raises for undecodable BSON or invalid UTF-8."""
    from bson.errors import InvalidBSON

    return (InvalidBSON, UnicodeDecodeError)


def dumps(value: Any, mode: str = RELAXED) -> str:
    """Serialize a BSON value to Extended JSON without escaping non-ASCII text.

    ``ensure_ascii=False`` keeps multibyte text (Japanese, emoji, combining
    characters) as literal UTF-8 instead of ``\\uXXXX`` escapes, so the
    string column is readable and byte-for-byte equal to the source text.
    """
    from bson import json_util

    return json_util.dumps(value, json_options=_json_options(mode), ensure_ascii=False,
                           separators=(",", ":"))


def loads(text: str, mode: str = CANONICAL) -> Any:
    """Parse Extended JSON produced by :func:`dumps`."""
    from bson import json_util

    return json_util.loads(text, json_options=_json_options(mode))


def encode_id(value: Any) -> str:
    """Encode a document ``_id`` into a string primary key.

    - ``ObjectId`` (the default ``_id`` type) becomes its 24-char hex string.
    - Every other type becomes canonical Extended JSON. A string ``_id`` is
      therefore JSON-quoted (``"abc"``), so it can never collide with an
      ObjectId hex string. Numbers keep their BSON type
      (``{"$numberInt": "7"}``), which is stable because MongoDB returns an
      ``_id`` with the type it was stored with.
    """
    from bson import ObjectId

    if isinstance(value, ObjectId):
        return str(value)
    return dumps(value, CANONICAL)


def decode_id(text: str) -> Any:
    """Inverse of :func:`encode_id`."""
    from bson import ObjectId

    if len(text) == 24 and ObjectId.is_valid(text) and not text.startswith('"'):
        return ObjectId(text)
    return loads(text, CANONICAL)


def timestamp_to_datetime(ts: Optional[Timestamp]) -> Optional[datetime]:
    """Convert a BSON ``Timestamp`` (cluster time) to an aware UTC datetime."""
    if ts is None:
        return None
    return datetime.fromtimestamp(ts.time, tz=timezone.utc)


def to_utc(dt: Optional[datetime]) -> Optional[datetime]:
    """Normalize a datetime returned by PyMongo to aware UTC."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def token_data(token: Optional[dict]) -> Optional[str]:
    """Return the hex ``_data`` of a resume token (comparable as a string)."""
    if not token:
        return None
    data = token.get("_data")
    if not isinstance(data, str):
        raise ValueError(f"unsupported resume token format: {token!r}")
    return data


def parse_bool(value: Optional[str], default: bool) -> bool:
    if value is None or value == "":
        return default
    lowered = str(value).strip().lower()
    if lowered in ("true", "1", "yes", "y"):
        return True
    if lowered in ("false", "0", "no", "n"):
        return False
    raise ValueError(f"expected a boolean, got {value!r}")


def parse_int(value: Optional[str], default: int, name: str, minimum: int = 1) -> int:
    if value is None or value == "":
        return default
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"option '{name}' must be an integer, got {value!r}") from exc
    if parsed < minimum:
        raise ValueError(f"option '{name}' must be >= {minimum}, got {parsed}")
    return parsed


def parse_choice(value: Optional[str], default: str, name: str, choices: tuple[str, ...]) -> str:
    if value is None or value == "":
        return default
    if value not in choices:
        raise ValueError(f"option '{name}' must be one of {', '.join(choices)}; got {value!r}")
    return value
