"""Corpus store: load record arrays + filter / sort / paginate them.

A corpus is a directory of JSON files, one per "table". Each file contains
either an array of dicts (for endpoints that return a list) or a single dict
(for endpoints with ``single_entity: true``).

This module owns the in-memory view: load on demand, apply filters and
sorting per request, hand off to a pagination renderer for slicing. Field
lookup uses dotted paths for nested records (``commit.author.date``).
"""

from __future__ import annotations

import copy
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from databricks.labs.community_connector.source_simulator.endpoint_spec import (
    EndpointSpec,
    FilterOp,
    FilterParam,
)


# A corpus entry is either a list of dicts (endpoint returns an array) or
# a single dict (single_entity).
CorpusValue = Union[List[Dict[str, Any]], Dict[str, Any]]


@dataclass
class CorpusStore:
    """In-memory corpus, loaded from a directory of JSON files."""

    tables: Dict[str, CorpusValue] = field(default_factory=dict)

    @classmethod
    def load(cls, corpus_dir: Path) -> "CorpusStore":
        """Read every ``*.json`` file in ``corpus_dir``; key each by stem."""
        corpus_dir = Path(corpus_dir)
        if not corpus_dir.exists():
            raise FileNotFoundError(f"Corpus directory not found: {corpus_dir}")
        store = cls()
        for path in sorted(corpus_dir.glob("*.json")):
            with open(path, "r", encoding="utf-8") as f:
                store.tables[path.stem] = json.load(f)
        return store

    def get(self, table: str) -> Optional[CorpusValue]:
        return self.tables.get(table)


# ----- query pipeline ----------------------------------------------------


def get_field(record: Dict[str, Any], path: str) -> Any:
    """Look up a dotted-path field on a record; return None if any part missing."""
    cur: Any = record
    for part in path.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return None
    return cur


def set_field(record: Dict[str, Any], path: str, value: Any) -> None:
    """Write a dotted-path field on a record, creating intermediate dicts if missing."""
    cur: Any = record
    parts = path.split(".")
    for part in parts[:-1]:
        nxt = cur.get(part)
        if not isinstance(nxt, dict):
            nxt = {}
            cur[part] = nxt
        cur = nxt
    cur[parts[-1]] = value


def inject_future_records(corpus: "CorpusStore", specs: List[EndpointSpec]) -> None:
    """For every endpoint spec with a ``synthesize_future_records`` directive,
    clone records into the target corpus with cursor fields rewritten to
    timestamps strictly past wall-clock ``now()``. A correctly-capped
    connector will exclude these via its ``until=<init_time>`` filter,
    making the termination test detect cap bugs.

    Idempotent per ``(corpus_key, cursor_field)``: a directive is a no-op
    if at least one existing record already has the cursor field set to a
    future timestamp (e.g. when called twice).
    """
    now = datetime.now(timezone.utc)
    for spec in specs:
        directive = spec.synthesize_future_records
        if directive is None or spec.corpus is None:
            continue
        records = corpus.get(spec.corpus)
        if not isinstance(records, list) or not records:
            # Need at least one template record to clone.
            continue
        # Idempotency check: skip if any existing record already has a
        # post-now timestamp at the cursor field.
        already_seeded = any(
            _is_future_iso(get_field(r, directive.cursor_field), now) for r in records
        )
        if already_seeded:
            continue
        template = records[-1]
        for i in range(directive.count):
            future_ts = (now + timedelta(hours=i + 1)).strftime("%Y-%m-%dT%H:%M:%SZ")
            clone = copy.deepcopy(template)
            set_field(clone, directive.cursor_field, future_ts)
            records.append(clone)


def _is_future_iso(value: Any, now: datetime) -> bool:
    if not isinstance(value, str):
        return False
    try:
        # Accept "2026-05-05T12:00:00Z" and "2026-05-05T12:00:00+00:00".
        ts = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts > now


def apply_filters(
    records: List[Dict[str, Any]],
    filter_specs: List[FilterParam],
    query: Dict[str, str],
) -> List[Dict[str, Any]]:
    """Subset records by every filter param present in ``query``."""
    out = list(records)
    for fp in filter_specs:
        if fp.name not in query:
            continue
        raw = query[fp.name]
        out = [r for r in out if _matches(get_field(r, fp.field), raw, fp.op)]
    return out


def apply_sort(
    records: List[Dict[str, Any]],
    *,
    sort_field: Optional[str],
    sort_order: str = "asc",
) -> List[Dict[str, Any]]:
    """Stable sort by a dotted-path field; missing values sort last in asc, first in desc."""
    if not sort_field:
        return records
    reverse = sort_order.lower() == "desc"

    def key(r: Dict[str, Any]) -> Tuple[bool, Any]:
        v = get_field(r, sort_field)
        # (is_none, value) so None values group together; for asc they come last.
        return (v is None, v if v is not None else "")

    return sorted(records, key=key, reverse=reverse)


def slice_page(
    records: List[Dict[str, Any]], offset: int, limit: int
) -> List[Dict[str, Any]]:
    """Slice ``records`` into a page. Out-of-range offset returns an empty list."""
    offset = max(offset, 0)
    if limit <= 0:
        return []
    return records[offset : offset + limit]


# ----- internals ---------------------------------------------------------


def _matches(value: Any, raw_query_value: str, op: FilterOp) -> bool:
    """Compare ``value`` (from the record) to ``raw_query_value`` using ``op``."""
    if value is None:
        # NE matches when the field is missing; everything else fails.
        return op == FilterOp.NE

    if op == FilterOp.IN:
        choices = [c.strip() for c in raw_query_value.split(",")]
        return str(value) in choices

    coerced = _coerce_to_value_type(raw_query_value, value)
    if coerced is None and not isinstance(value, str):
        # Couldn't coerce to the field's type; comparison is undefined.
        return False

    try:
        if op == FilterOp.EQ:
            return value == coerced
        if op == FilterOp.NE:
            return value != coerced
        if op == FilterOp.GT:
            return value > coerced
        if op == FilterOp.GTE:
            return value >= coerced
        if op == FilterOp.LT:
            return value < coerced
        if op == FilterOp.LTE:
            return value <= coerced
    except TypeError:
        return False

    return False


def _coerce_to_value_type(raw: str, value: Any) -> Any:
    """Best-effort coerce ``raw`` (a string from the query) to the type of ``value``."""
    if isinstance(value, bool):
        return raw.lower() in ("true", "1", "yes")
    if isinstance(value, int) and not isinstance(value, bool):
        try:
            return int(raw)
        except ValueError:
            return None
    if isinstance(value, float):
        try:
            return float(raw)
        except ValueError:
            return None
    # Strings (including ISO timestamps) compare lexicographically — adequate
    # for ISO-8601 timestamps which sort correctly as strings.
    return raw
