"""Expand a small sample of records into a larger, varied set at replay time.

Works off type-aware scalar variation — no schema required. Integers shift by
random amounts, ISO-8601 timestamps shift forward, hex/UUID strings get
randomized, other strings get an index suffix. Nested dicts and lists recurse.

Primary keys and cursor fields are inferred by inspecting variation **across
the provided samples**: fields whose values differ between samples are treated
as varying; fields that stay constant are preserved as-is.
"""

from __future__ import annotations

import copy
import hashlib
import json
import random
import re
from datetime import datetime, timedelta, timezone
from typing import Any, List, Optional, Tuple

from tests.unit.sources.mock_framework.sampler import (
    RECORDS_KEY_HINTS,
    _looks_like_records_list,
)

_ISO_TIMESTAMP_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}[Tt ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?$"
)
_HEX_ID_RE = re.compile(r"^[0-9a-fA-F]{20,}$")
_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)


def synthesize_body(
    body_text: Optional[str],
    *,
    target_count: int,
    seed: int = 0,
    records_key_hints: Tuple[str, ...] = RECORDS_KEY_HINTS,
) -> Optional[str]:
    """Return a new body JSON with records extended to ``target_count``.

    If the body isn't JSON, doesn't contain a records list, or already has
    >= ``target_count`` records, returns the body unchanged.
    """
    if body_text is None or not body_text:
        return body_text
    try:
        payload = json.loads(body_text)
    except (ValueError, TypeError):
        return body_text

    records, setter = _locate_records(payload, records_key_hints)
    if records is None or not records:
        return body_text
    if len(records) >= target_count:
        return body_text

    expanded = _expand_records(records, target_count=target_count, seed=seed)
    setter(expanded)
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False)


# ----- locate records in payload ----------------------------------------


def _locate_records(payload: Any, records_key_hints: Tuple[str, ...]):
    """Return ``(records_list, setter_fn)`` — ``setter_fn(new_list)`` replaces it in place."""
    if isinstance(payload, list) and _looks_like_records_list(payload):
        # Top-level list: re-build the outer wrapper by mutating slice.
        def _setter(new: list) -> None:
            payload[:] = new

        return payload, _setter

    if isinstance(payload, dict):
        for hint in records_key_hints:
            if hint in payload and _looks_like_records_list(payload[hint]):
                key = hint

                def _setter(new: list, _key=key) -> None:
                    payload[_key] = new

                return payload[key], _setter

        # Fallback: largest list-of-dicts field.
        candidates = [
            (k, v) for k, v in payload.items() if _looks_like_records_list(v)
        ]
        if candidates:
            candidates.sort(key=lambda kv: len(kv[1]), reverse=True)
            key, value = candidates[0]

            def _setter(new: list, _key=key) -> None:
                payload[_key] = new

            return value, _setter

    return None, lambda _new: None


# ----- record expansion -------------------------------------------------


def _expand_records(
    samples: List[dict], *, target_count: int, seed: int
) -> List[dict]:
    """Return samples + synthesized records up to target_count.

    Deep-copies everything so that nested dicts aren't shared between
    synthesized records (which would cause accumulating mutations) or
    with the caller's input samples (which the caller doesn't expect
    to be mutated).
    """
    rng = random.Random(seed)
    varying_fields = _detect_varying_fields(samples)

    out: List[dict] = [copy.deepcopy(s) for s in samples]
    for i in range(target_count - len(samples)):
        template = copy.deepcopy(samples[-1])
        _mutate(template, varying_fields, rng=rng, index=i + 1)
        out.append(template)
    return out


def _detect_varying_fields(samples: List[dict]) -> set:
    """Field paths whose values differ across at least two samples."""
    varying: set = set()
    if len(samples) < 2:
        return varying

    def walk(prefix: str, objs: List[Any]) -> None:
        first = objs[0]
        if isinstance(first, dict):
            for k in first.keys():
                child_values = [
                    (obj.get(k) if isinstance(obj, dict) else None) for obj in objs
                ]
                if any(cv != child_values[0] for cv in child_values[1:]):
                    varying.add(f"{prefix}.{k}" if prefix else k)
                walk(f"{prefix}.{k}" if prefix else k, child_values)

    walk("", samples)
    return varying


def _mutate(
    obj: Any, varying: set, *, rng: random.Random, index: int, prefix: str = ""
) -> None:
    if isinstance(obj, dict):
        for k, v in list(obj.items()):
            path = f"{prefix}.{k}" if prefix else k
            if isinstance(v, (dict, list)):
                _mutate(v, varying, rng=rng, index=index, prefix=path)
            elif path in varying:
                obj[k] = _vary_scalar(v, rng=rng, index=index)
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            if isinstance(item, (dict, list)):
                _mutate(item, varying, rng=rng, index=index, prefix=f"{prefix}[{i}]")


def _vary_scalar(value: Any, *, rng: random.Random, index: int) -> Any:
    if value is None:
        return None
    if isinstance(value, bool):
        return value  # don't flip booleans — they're usually status flags
    if isinstance(value, int):
        return value + rng.randint(1, 10_000) + index
    if isinstance(value, float):
        return value + rng.random() * 1000.0 + index
    if isinstance(value, str):
        if _ISO_TIMESTAMP_RE.match(value):
            return _shift_timestamp(value, rng=rng, index=index)
        if _UUID_RE.match(value):
            return _random_uuid(rng)
        if _HEX_ID_RE.match(value):
            return _random_hex(len(value), rng=rng)
        return f"{value}-syn{index}"
    return value  # unknown type — leave alone


def _shift_timestamp(ts: str, *, rng: random.Random, index: int) -> str:
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return ts
    shifted = dt + timedelta(seconds=rng.randint(1, 3600) + index * 60)
    # Preserve the original format: separator (T vs space) and Z suffix.
    separator = " " if len(ts) >= 11 and ts[10] == " " else "T"
    if ts.endswith("Z") or ts.endswith("z"):
        return shifted.astimezone(timezone.utc).strftime(f"%Y-%m-%d{separator}%H:%M:%SZ")
    iso = shifted.isoformat()
    if separator == " ":
        iso = iso.replace("T", " ", 1)
    return iso


def _random_hex(length: int, *, rng: random.Random) -> str:
    return "".join(rng.choice("0123456789abcdef") for _ in range(length))


def _random_uuid(rng: random.Random) -> str:
    hx = _random_hex(32, rng=rng)
    return f"{hx[0:8]}-{hx[8:12]}-{hx[12:16]}-{hx[16:20]}-{hx[20:32]}"


def stable_seed(*parts: str) -> int:
    """A small, deterministic int derived from string parts."""
    h = hashlib.sha256("||".join(parts).encode("utf-8")).digest()
    return int.from_bytes(h[:4], "big")
