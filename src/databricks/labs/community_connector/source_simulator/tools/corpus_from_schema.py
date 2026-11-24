"""Generate a synthetic corpus from a connector's Spark StructType schemas.

When a connector ships without an existing cassette (no live credentials
available to record against), we still want a corpus to drive simulate-mode
tests. This tool walks the connector's ``TABLE_SCHEMAS`` (or any callable
that returns a StructType per table) and synthesizes N records per table
that match the schema.

The generated records are *plausible enough* to pass shape-level
LakeflowConnectTests assertions (correct types, non-null fields populated,
primary keys unique, cursor fields monotonic). They are not real data and
should never be presented to a user as such.

Usage::

    from databricks.labs.community_connector.source_simulator.tools.corpus_from_schema import (
        write_corpus_from_schemas,
    )
    from databricks.labs.community_connector.sources.zendesk.zendesk import (
        ZendeskLakeflowConnect,
    )

    connector = ZendeskLakeflowConnect({"subdomain": "x", "email": "x", "api_token": "x"})
    write_corpus_from_schemas(
        connector=connector,
        output_dir=Path(".../source_simulator/specs/zendesk/corpus/"),
        records_per_table=5,
    )
"""

from __future__ import annotations

import hashlib
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DataType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def write_corpus(
    schemas: Dict[str, StructType],
    output_dir: Path,
    *,
    records_per_table: int = 5,
    cursor_fields: Optional[Dict[str, str]] = None,
    primary_keys: Optional[Dict[str, List[str]]] = None,
    seed: int = 42,
) -> List[Path]:
    """Generate one corpus file per table from a ``{name: StructType}`` dict.

    Doesn't require a connector instance — bypasses connector ``__init__``
    side-effects (credential validation, etc.). Use when the connector
    validates credentials at construction time.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    cursor_fields = cursor_fields or {}
    primary_keys = primary_keys or {}

    written: List[Path] = []
    for table, schema in schemas.items():
        records = generate_records(
            schema=schema,
            count=records_per_table,
            cursor_field=cursor_fields.get(table),
            primary_key_fields=primary_keys.get(table) or [],
            seed=seed + _str_seed(table),
        )
        out_path = output_dir / f"{table}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(records, f, indent=2, ensure_ascii=False)
            f.write("\n")
        written.append(out_path)
        print(f"  wrote {out_path.name}  ({len(records)} record{'s' if len(records) != 1 else ''})")
    return written


def write_corpus_from_schemas(
    connector: Any,
    output_dir: Path,
    *,
    records_per_table: int = 5,
    table_options: Optional[Dict[str, Dict[str, str]]] = None,
    cursor_field_overrides: Optional[Dict[str, str]] = None,
    primary_key_overrides: Optional[Dict[str, List[str]]] = None,
    seed: int = 42,
) -> List[Path]:
    """Generate one corpus file per table the connector lists.

    Args:
        connector: a LakeflowConnect instance — used to call
            ``list_tables`` and ``get_table_schema``.
        output_dir: where to write ``<table>.json`` files.
        records_per_table: how many records to synthesize per table.
        table_options: per-table options to pass to ``get_table_schema``.
        cursor_field_overrides: per-table cursor field name (overrides
            the connector's metadata).
        primary_key_overrides: per-table list of PK fields (overrides
            metadata).
        seed: random seed for determinism across runs.

    Returns the list of files written.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    table_options = table_options or {}
    cursor_field_overrides = cursor_field_overrides or {}
    primary_key_overrides = primary_key_overrides or {}

    written: List[Path] = []
    for table in connector.list_tables():
        opts = table_options.get(table, {})
        try:
            schema = connector.get_table_schema(table, opts)
        except Exception as e:
            # Skip tables whose schema lookup fails (e.g. requires options
            # we don't have). The corpus generator runs offline and
            # without real config; this is best-effort.
            print(f"  skip {table}: get_table_schema raised {type(e).__name__}: {e}")
            continue

        cursor_field = cursor_field_overrides.get(table)
        pk_fields = primary_key_overrides.get(table)
        if cursor_field is None or pk_fields is None:
            try:
                metadata = connector.read_table_metadata(table, opts)
                cursor_field = cursor_field or metadata.get("cursor_field")
                pk_fields = pk_fields or metadata.get("primary_keys") or []
            except Exception:
                pass

        records = generate_records(
            schema=schema,
            count=records_per_table,
            cursor_field=cursor_field,
            primary_key_fields=pk_fields or [],
            seed=seed + _str_seed(table),
        )
        out_path = output_dir / f"{table}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(records, f, indent=2, ensure_ascii=False)
            f.write("\n")
        written.append(out_path)
        print(f"  wrote {out_path.name}  ({len(records)} record{'s' if len(records) != 1 else ''})")
    return written


def generate_records(
    schema: StructType,
    *,
    count: int,
    cursor_field: Optional[str] = None,
    primary_key_fields: Sequence[str] = (),
    seed: int = 0,
) -> List[Dict[str, Any]]:
    """Synthesize ``count`` records matching ``schema``.

    Cursor fields advance monotonically across records (so cursor-based
    pagination terminates in a finite number of steps). Primary key fields
    are unique across the generated records.
    """
    base_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
    cursor_dt = _resolve_field_type(schema, cursor_field) if cursor_field else None

    records: List[Dict[str, Any]] = []
    for i in range(count):
        record_rng = random.Random(seed + i)
        record = _gen_struct(schema, rng=record_rng, depth=0)
        # Force cursor field to advance monotonically. Type depends on the
        # schema's declared type for that field — Long → unix-seconds int,
        # everything else → ISO timestamp string.
        if cursor_field:
            if isinstance(cursor_dt, (LongType, IntegerType, ShortType)):
                _set_field(record, cursor_field, int((base_time + timedelta(hours=i)).timestamp()))
            else:
                ts = (base_time + timedelta(hours=i)).isoformat().replace("+00:00", "Z")
                _set_field(record, cursor_field, ts)
        for pk in primary_key_fields:
            existing = _get_field(record, pk)
            if isinstance(existing, int):
                _set_field(record, pk, 1000 + i)
            elif isinstance(existing, str) and existing:
                _set_field(record, pk, f"{existing[:8]}-pk{i}")
        records.append(record)
    return records


def _resolve_field_type(schema: StructType, path: str) -> Optional[DataType]:
    """Walk a dotted-path through nested StructTypes; return the leaf DataType."""
    cur: DataType = schema
    for part in path.split("."):
        if not isinstance(cur, StructType):
            return None
        match = next((f for f in cur.fields if f.name == part), None)
        if match is None:
            return None
        cur = match.dataType
    return cur


# ----- internals --------------------------------------------------------


def _gen_value(dt: DataType, rng: random.Random, depth: int) -> Any:
    if isinstance(dt, StructType):
        return _gen_struct(dt, rng=rng, depth=depth + 1)
    if isinstance(dt, ArrayType):
        n = rng.randint(0, 2)
        return [_gen_value(dt.elementType, rng, depth + 1) for _ in range(n)]
    if isinstance(dt, MapType):
        n = rng.randint(0, 2)
        return {
            f"key_{i}": _gen_value(dt.valueType, rng, depth + 1) for i in range(n)
        }
    if isinstance(dt, (IntegerType, LongType, ShortType)):
        return rng.randint(1, 100_000)
    if isinstance(dt, (DoubleType, FloatType, DecimalType)):
        return round(rng.random() * 1000, 2)
    if isinstance(dt, BooleanType):
        return rng.choice([True, False])
    if isinstance(dt, TimestampType):
        # Default to a recent ISO timestamp string. Most connectors
        # serialize timestamps as strings even when typed as Timestamp.
        days_ago = rng.randint(0, 30)
        secs = rng.randint(0, 86_400)
        ts = datetime(2024, 1, 1, tzinfo=timezone.utc) + timedelta(
            days=days_ago, seconds=secs
        )
        return ts.isoformat().replace("+00:00", "Z")
    if isinstance(dt, StringType):
        return _gen_string(rng)
    # Unknown — give back something JSON-safe.
    return None


def _gen_struct(schema: StructType, *, rng: random.Random, depth: int) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for field in schema.fields:
        if depth > 6:
            # Recursion guard for self-referential schemas — produce None.
            out[field.name] = None
            continue
        out[field.name] = _gen_value(field.dataType, rng, depth)
    return out


_VOCAB = (
    "alpha bravo charlie delta echo foxtrot golf hotel india juliet kilo "
    "lima mike november oscar papa quebec romeo sierra tango uniform victor "
    "whiskey xray yankee zulu"
).split()


def _gen_string(rng: random.Random) -> str:
    return f"{rng.choice(_VOCAB)}-{rng.randint(100, 999)}"


def _str_seed(s: str) -> int:
    return int.from_bytes(hashlib.sha256(s.encode()).digest()[:4], "big")


def _set_field(record: Dict[str, Any], path: str, value: Any) -> None:
    """Set a dotted-path field, traversing nested dicts."""
    parts = path.split(".")
    cur: Any = record
    for p in parts[:-1]:
        if not isinstance(cur, dict):
            return
        cur = cur.setdefault(p, {})
    if isinstance(cur, dict):
        cur[parts[-1]] = value


def _get_field(record: Dict[str, Any], path: str) -> Any:
    cur: Any = record
    for p in path.split("."):
        if isinstance(cur, dict) and p in cur:
            cur = cur[p]
        else:
            return None
    return cur
