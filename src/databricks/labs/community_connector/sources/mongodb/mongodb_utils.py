"""Helpers for the MongoDB connector: value encoding, option parsing, offsets."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from bson import ObjectId, json_util
from bson.json_util import JSONOptions, JSONMode
from bson.timestamp import Timestamp

# Canonical Extended JSON keeps every BSON type distinguishable (Int64 vs
# Int32, Decimal128, Binary subtype, ...). It is used for the primary key and
# for offsets, where an exact, reversible encoding matters.
CANONICAL = JSONOptions(json_mode=JSONMode.CANONICAL, tz_aware=True, tzinfo=timezone.utc)
# Relaxed Extended JSON reads naturally (numbers stay numbers, dates become
# {"$date": "2024-01-01T00:00:00Z"}) and is what we put into the document column.
RELAXED = JSONOptions(json_mode=JSONMode.RELAXED, tz_aware=True, tzinfo=timezone.utc)

OFFSET_VERSION = 1


def dumps(value: Any, options: JSONOptions = RELAXED) -> str:
    """Serialize a BSON value to Extended JSON without escaping non-ASCII text.

    ``ensure_ascii=False`` keeps multibyte text (Japanese, emoji, combining
    characters) as literal UTF-8 instead of ``\\uXXXX`` escapes, so the
    string column is readable and byte-for-byte equal to the source text.
    """
    return json_util.dumps(value, json_options=options, ensure_ascii=False, separators=(",", ":"))


def loads(text: str, options: JSONOptions = CANONICAL) -> Any:
    """Parse Extended JSON produced by :func:`dumps`."""
    return json_util.loads(text, json_options=options)


def encode_id(value: Any) -> str:
    """Encode a document ``_id`` into a string primary key.

    - ``ObjectId`` (the default ``_id`` type) becomes its 24-char hex string.
    - Every other type becomes canonical Extended JSON. A string ``_id`` is
      therefore JSON-quoted (``"abc"``), so it can never collide with an
      ObjectId hex string. Numbers keep their BSON type
      (``{"$numberInt": "7"}``), which is stable because MongoDB returns an
      ``_id`` with the type it was stored with.
    """
    if isinstance(value, ObjectId):
        return str(value)
    return dumps(value, CANONICAL)


def decode_id(text: str) -> Any:
    """Inverse of :func:`encode_id`."""
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
