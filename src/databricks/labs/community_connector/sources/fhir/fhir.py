"""FHIR R4 Lakeflow Community Connector.

Implements LakeflowConnect to ingest FHIR R4 resources from any
SMART-on-FHIR-compliant server into Databricks Delta tables.

See fhir_api_doc.md for API details and design decisions.
"""

import logging
import math
import re
from datetime import datetime, timedelta, timezone
from typing import Iterator, Optional

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.fhir.fhir_constants import (
    BULK_EXPORT_TIMEOUT,
    BULK_POLL_INTERVAL,
    BULK_VALID_INCREMENTAL_MODES,
    CURSOR_FIELD,
    DEFAULT_MAX_RECORDS,
    DEFAULT_PAGE_SIZE,
    DEFAULT_RESOURCES,
    PAGE_DELAY,
    UNTYPED_BUCKET,
)
from databricks.labs.community_connector.sources.fhir.fhir_schemas import get_schema
from databricks.labs.community_connector.sources.fhir.fhir_bulk import BulkExportClient
from databricks.labs.community_connector.sources.fhir.fhir_utils import (
    FhirHttpClient,
    SmartAuthClient,
    discover_token_url,
    extract_record,
    iter_bundle_pages,
)

logger = logging.getLogger(__name__)


_TS_RE = re.compile(
    r"^(?P<base>\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(?::\d{2})?)"
    r"(?:\.(?P<frac>\d+))?"
    r"(?P<tz>[Zz]|[+-]\d{2}:?\d{2})?$"
)


def _parse_ts(ts: str) -> datetime:
    """Parse a FHIR instant to a timezone-aware datetime.

    datetime.fromisoformat() alone is too strict for real FHIR servers on the
    Python versions this connector supports (3.10+). It rejects a trailing "Z"
    before 3.11, fractional seconds that aren't exactly 3 or 6 digits, and
    compact "+0000" offsets; and it yields naive values for instants with no
    offset, which then cannot be compared against aware ones. Normalise all of
    that so cursor comparisons never crash mid-microbatch.
    """
    raw = ts.strip()
    match = _TS_RE.match(raw)
    if not match:
        # Let fromisoformat produce the error for anything we don't recognise.
        parsed = datetime.fromisoformat(raw)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)

    base, frac, tz = match.group("base"), match.group("frac"), match.group("tz")
    # Python accepts only 3- or 6-digit fractions; pad/truncate to microseconds.
    normalised = f"{base}.{frac[:6].ljust(6, '0')}" if frac else base
    if tz and tz not in ("Z", "z"):
        offset = tz if ":" in tz else f"{tz[:3]}:{tz[3:]}"
        normalised += offset
    parsed = datetime.fromisoformat(normalised)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _canonical_ts(ts: str) -> str:
    """Render an instant as a UTC "Z" string.

    Two reasons every stored/sent cursor goes through this. A "+00:00" offset is
    percent-encoded as "%2B" and, inside the nested `_typeFilter` query, decoded
    back to a space — corrupting the date bound. And a server that re-spells the
    same instant between polls ("...12:00:00Z" vs "...12:00:00.000+00:00") would
    defeat any equality check done on the raw text.
    """
    return _parse_ts(ts).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _int_option(options: dict, key: str, default: int, minimum: int = 1) -> int:
    """Read an integer option, failing with an actionable message.

    A bare int() here surfaces as an opaque pipeline-init error, so validate at
    construction time the same way the bulk scope/mode options do.
    """
    raw = options.get(key)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        raise ValueError(f"Option '{key}' must be an integer (got {raw!r}).") from None
    if value < minimum:
        raise ValueError(f"Option '{key}' must be >= {minimum} (got {value}).")
    return value


def _float_option(options: dict, key: str, default: float, minimum: float = 0.0) -> float:
    """Read a float option, failing with an actionable message.

    Rejects nan/inf explicitly: both slip past a `< minimum` check, and an
    infinite page_delay would sleep the executor until the pipeline is killed.
    """
    raw = options.get(key)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        value = float(raw)
    except (TypeError, ValueError):
        raise ValueError(f"Option '{key}' must be a number (got {raw!r}).") from None
    if not math.isfinite(value):
        raise ValueError(f"Option '{key}' must be a finite number (got {raw!r}).")
    if value < minimum:
        raise ValueError(f"Option '{key}' must be >= {minimum} (got {value}).")
    return value


class FhirLakeflowConnect(LakeflowConnect):
    """LakeflowConnect implementation for FHIR R4 servers."""

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)

        auth_type = options.get("auth_type", "none")
        token_url = options.get("token_url", "").strip()

        if not token_url and auth_type != "none":
            token_url = discover_token_url(options["base_url"])

        auth_client = SmartAuthClient(
            token_url=token_url,
            client_id=options.get("client_id", ""),
            auth_type=auth_type,
            private_key_pem=options.get("private_key_pem", ""),
            client_secret=options.get("client_secret", ""),
            scope=options.get("scope", ""),
            kid=options.get("kid", ""),
            private_key_algorithm=options.get("private_key_algorithm", "RS384"),
        )
        self._client = FhirHttpClient(base_url=options["base_url"], auth_client=auth_client)

        # Cap cursor at init time -- never chase data written after this trigger started.
        self._init_ts = datetime.now(timezone.utc).isoformat()

        resource_types_opt = options.get("resource_types", "").strip()
        self._resources = (
            [r.strip() for r in resource_types_opt.split(",") if r.strip()]
            if resource_types_opt
            else list(DEFAULT_RESOURCES)
        )

        # Bulk export (opt-in via bulk_export_enabled=true). The mode and overlap
        # are assigned unconditionally so the cursor helpers work on any instance;
        # `_bulk_client` exists only when bulk mode is on, and `_read_table_bulk`
        # is reachable only through the `_bulk_enabled` guard in read_table.
        self._bulk_enabled = options.get("bulk_export_enabled", "false").lower() == "true"
        self._bulk_incremental_mode = options.get("bulk_incremental_mode", "since")
        # Servers differ on whether manifest transactionTime marks the start
        # or the completion of an export. A non-zero overlap re-reads a safe
        # window; duplicates are absorbed by the upsert on `id`.
        self._bulk_overlap_seconds = _int_option(options, "bulk_overlap_seconds", 0, minimum=0)
        if self._bulk_enabled:
            if self._bulk_incremental_mode not in BULK_VALID_INCREMENTAL_MODES:
                raise ValueError(
                    f"Invalid bulk_incremental_mode {self._bulk_incremental_mode!r}. "
                    f"Use one of: {sorted(BULK_VALID_INCREMENTAL_MODES)}."
                )
            self._bulk_client = BulkExportClient(
                base_url=options["base_url"],
                auth_client=auth_client,
                scope=options.get("bulk_export_scope", "system"),
                group_id=options.get("bulk_export_group_id") or None,
                poll_interval=_int_option(options, "bulk_export_poll_interval", BULK_POLL_INTERVAL),
                timeout=_int_option(options, "bulk_export_timeout", BULK_EXPORT_TIMEOUT),
                ignore_errors=options.get("bulk_export_ignore_errors", "false").lower() == "true",
            )

    def list_tables(self) -> list[str]:
        return list(self._resources)

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        self._validate_table(table_name)
        profile = table_options.get("profile", "uk_core")
        return get_schema(table_name, profile=profile)

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        self._validate_table(table_name)
        if self._bulk_enabled and self._bulk_incremental_mode == "full":
            # A full export is a complete copy of the server's data, so ingest it
            # as a snapshot: resources deleted on the server leave the target.
            # As "cdc" they would be upserted forever and never removed.
            return {"primary_keys": ["id"], "ingestion_type": "snapshot"}
        return {
            "primary_keys": ["id"],
            "cursor_field": CURSOR_FIELD,
            "ingestion_type": "cdc",
        }

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Fetch FHIR resources with bundle pagination and _lastUpdated incremental sync.

        Uses max_records_per_batch admission control (always required for CDC tables).
        Short-circuits when cursor >= _init_ts to prevent chasing live writes.
        When bulk_export_enabled=true, delegates to _read_table_bulk() instead.
        """
        self._validate_table(table_name)

        if self._bulk_enabled:
            return self._read_table_bulk(table_name, start_offset, table_options)

        since = start_offset.get("cursor") if start_offset else None
        if since and _parse_ts(since) >= _parse_ts(self._init_ts):
            return iter([]), start_offset

        page_size = _int_option(table_options, "page_size", DEFAULT_PAGE_SIZE)
        max_records = _int_option(table_options, "max_records_per_batch", DEFAULT_MAX_RECORDS)
        page_delay = _float_option(table_options, "page_delay", PAGE_DELAY)
        profile = table_options.get("profile", "uk_core")

        params: dict[str, str] = {"_count": str(page_size), "_sort": "_lastUpdated"}
        if since:
            params["_lastUpdated"] = f"gt{since}"

        # Truncate at max_records, but never inside a run of equal lastUpdated
        # values: the next read asks for lastUpdated > cursor, so a tie left
        # behind the cut would never be fetched. The batch may therefore exceed
        # max_records by the size of the boundary tie.
        records = []
        boundary = None  # lastUpdated of the record that filled the batch
        truncated = False
        out_of_order = False
        prev_dt = None
        for resource in iter_bundle_pages(self._client, table_name, params, page_delay=page_delay):
            record = extract_record(resource, table_name, profile=profile)
            ts = record.get(CURSOR_FIELD)
            dt = _parse_ts(ts) if ts else None
            if boundary is not None and dt != boundary:
                truncated = True
                break
            if dt is not None:
                if prev_dt is not None and dt < prev_dt:
                    out_of_order = True
                prev_dt = dt
            records.append(record)
            if boundary is None and len(records) >= max_records:
                boundary = dt
                if boundary is None:
                    # No timestamp to extend a tie on; cut here as before.
                    truncated = True
                    break

        # A truncated batch is only safe if results arrive sorted: otherwise the
        # cursor (max lastUpdated) jumps past records not yet fetched.
        if truncated and out_of_order:
            raise RuntimeError(
                f"FHIR server returned '{table_name}' results out of _lastUpdated order — "
                f"it appears to have ignored '_sort=_lastUpdated'. Truncating the batch at "
                f"max_records_per_batch={max_records} would skip records permanently. "
                f"Raise max_records_per_batch above the resource count, or use bulk export."
            )

        if not records:
            return iter([]), start_offset or {}

        # Detect if server ignored the _lastUpdated filter.
        # Only raise if there are records with a datable lastUpdated AND none of them are
        # newer than since — that combination is evidence the filter was ignored.
        # Records with null lastUpdated are excluded from this check (cardinality 0..1 in spec).
        if since:
            has_datable = [r for r in records if r.get(CURSOR_FIELD)]
            since_dt = _parse_ts(since)
            newer = [r for r in has_datable if _parse_ts(r[CURSOR_FIELD]) > since_dt]
            if has_datable and not newer:
                raise RuntimeError(
                    f"FHIR server appears to have ignored the '_lastUpdated=gt{since}' filter — "
                    f"all {len(has_datable)} records with a lastUpdated for '{table_name}' have "
                    f"lastUpdated <= '{since}'. "
                    f"This server may not support the _lastUpdated search parameter. "
                    f"See README.md for server requirements."
                )

        # Advance cursor to max lastUpdated across batch.
        cursors = [r[CURSOR_FIELD] for r in records if r.get(CURSOR_FIELD)]
        if not cursors:
            return iter(records), start_offset or {}

        last_cursor = max(cursors, key=_parse_ts)
        end_offset = {"cursor": last_cursor}

        if start_offset and start_offset == end_offset:
            return iter([]), start_offset

        return iter(records), end_offset

    def _read_table_bulk(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Bulk export path. Uses server transactionTime as the CDC cursor.

        In "full" mode there is no server cursor, so the offset carries this
        trigger's frozen `_init_ts` instead. That advances once per trigger —
        enough for the rows to be written — and short-circuits on the second
        call so Trigger.AvailableNow terminates. The next trigger is a new
        process with a new `_init_ts`, so the export runs again. It advances
        even when the export came back empty: leaving the offset alone there
        makes the framework call again and run a second $export for the same
        trigger (measured against HAPI).

        No instance "already exported" flag is used: on a cache miss the
        framework replays a batch via readBetweenOffsets -> read_table, and a
        sticky flag would make that replay return nothing.

        Note there is deliberately no per-batch record cap here. The bulk cursor
        is the server's export snapshot instant, which already covers every
        resource in the manifest, so truncating the batch while advancing the
        cursor would drop the remainder permanently. A cap would have to
        paginate within the manifest; until then the whole export is emitted.
        """
        profile = table_options.get("profile", "uk_core")
        full_mode = self._bulk_incremental_mode == "full"

        # One export per trigger, in every mode. A bulk $export is a whole
        # server-side job (hours on real EHRs, and Epic enforces a 24h cooldown
        # per group), so the trigger must never run a second one just to discover
        # there is nothing new. `run_ts` is this process's frozen `_init_ts`, so
        # it matches only on a later call within the same trigger.
        if start_offset and start_offset.get("run_ts") == self._init_ts:
            return iter([]), start_offset

        if full_mode:
            cursor = since = None
        else:
            cursor = start_offset.get("cursor") if start_offset else None
            if cursor and _parse_ts(cursor) >= _parse_ts(self._init_ts):
                return iter([]), start_offset
            # The overlap widens the query window only; the stored cursor tracks
            # transactionTime so it always moves forward.
            since = self._widen_since(cursor)

        transaction_time, resource_map = self._bulk_client.export(
            resource_types=[table_name],
            since=since,
            incremental_mode=self._bulk_incremental_mode,
        )

        raw_resources = list(resource_map.get(table_name) or [])
        # `_download_and_parse` buckets manifest entries with no "type" key under
        # UNTYPED_BUCKET. The export was scoped to this one type via `_type`, so
        # those rows are this table's data; merge them in rather than dropping a
        # completed export. Only that bucket is adopted — a differently-typed
        # bucket (e.g. OperationOutcome) must not be relabelled as this table.
        untyped = resource_map.get(UNTYPED_BUCKET)
        if untyped:
            logger.warning(
                "Bulk manifest for '%s' has %d entries with no resource type; "
                "treating them as '%s'.",
                table_name,
                len(untyped),
                table_name,
            )
            # De-duplicate on id: a server that reports the same file both with
            # and without a "type" would otherwise emit each resource twice.
            seen = {r.get("id") for r in raw_resources if r.get("id")}
            raw_resources.extend(r for r in untyped if not r.get("id") or r["id"] not in seen)

        for bucket, rows in resource_map.items():
            if bucket not in (table_name, UNTYPED_BUCKET) and rows:
                logger.warning(
                    "Bulk manifest for '%s' also returned %d %r resources; ignoring them.",
                    table_name,
                    len(rows),
                    bucket,
                )

        records = [extract_record(r, table_name, profile=profile) for r in raw_resources]

        if full_mode:
            # `run_ts` is stamped even for an empty export. Returning
            # `start_offset or {}` here would leave the offset unchanged, so the
            # framework calls read_table again and the `run_ts` guard above --
            # which is what limits a trigger to one export -- has nothing to
            # match on, kicking off a second $export for the same trigger.
            # Measured against HAPI: 2 jobs per trigger for a resource type with
            # no instances. Advancing on empty costs nothing (no rows to write)
            # and makes the second call short-circuit like the non-empty case.
            return iter(records), {"run_ts": self._init_ts}

        # Advance the cursor to the newest instant we can justify, never
        # backwards: start-time servers report a transactionTime that precedes
        # the stored cursor once a widened window is in play, and regressing
        # would re-read the same span every trigger.
        cursor_value = _canonical_ts(transaction_time) if transaction_time else cursor
        if cursor_value and cursor and _parse_ts(cursor_value) < _parse_ts(cursor):
            logger.warning(
                "Bulk export for '%s' reported transactionTime %s, which precedes "
                "the stored cursor %s; keeping the stored cursor.",
                table_name,
                transaction_time,
                cursor,
            )
            cursor_value = cursor

        # `run_ts` carries this trigger's frozen `_init_ts`, so the offset always
        # differs from the one we were handed and the batch is never discarded —
        # even when the cursor itself could not move. It is also what terminates
        # the trigger: the second call short-circuits above. Termination must not
        # depend on the cursor changing, because an export whose transactionTime
        # is stable (or textually re-spelled) would otherwise loop.
        end_offset = {"run_ts": self._init_ts}
        if cursor_value:
            end_offset["cursor"] = cursor_value
        return iter(records), end_offset

    def _widen_since(self, cursor: Optional[str]) -> Optional[str]:
        """Rewind the `_since` sent to the server by bulk_overlap_seconds.

        Servers differ on whether transactionTime marks the start or the end of
        an export, so a cursor taken from it can sit after resources that were
        written while the export ran. Widening only the query re-reads that
        window; duplicates are absorbed by the upsert on `id`.
        """
        if not cursor:
            return None
        if not self._bulk_overlap_seconds:
            # Canonicalise even with no overlap: a stored "+00:00" cursor would
            # otherwise reach _typeFilter verbatim and be corrupted there.
            return _canonical_ts(cursor)
        shifted = _parse_ts(cursor) - timedelta(seconds=self._bulk_overlap_seconds)
        # Always emit a UTC "Z" instant. Converting first matters: a cursor with
        # a non-UTC offset would otherwise keep it, and in typefilter mode this
        # value is embedded in a nested _typeFilter query where a URL-encoded
        # "+" decodes back to a space and corrupts the date bound.
        return shifted.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

    def _validate_table(self, table_name: str) -> None:
        if table_name not in self._resources:
            raise ValueError(
                f"Resource type '{table_name}' is not configured. Configured: {self._resources}"
            )
