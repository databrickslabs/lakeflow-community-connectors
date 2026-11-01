"""FHIR R4 Lakeflow Community Connector.

Implements LakeflowConnect to ingest FHIR R4 resources from any
SMART-on-FHIR-compliant server. Each configured resource type is its own
table, and every table shares one schema with the resource as a VARIANT.

See fhir_api_doc.md for API details and design decisions.
"""

import logging
import math
import re
from datetime import datetime, timedelta, timezone
from typing import Iterator, Optional

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.fhir.fhir_bulk import BulkExportClient
from databricks.labs.community_connector.sources.fhir.fhir_constants import (
    BULK_EXPORT_TIMEOUT,
    BULK_POLL_INTERVAL,
    BULK_VALID_INCREMENTAL_MODES,
    CURSOR_FIELD,
    DEFAULT_MAX_RECORDS,
    DEFAULT_PAGE_SIZE,
    DEFAULT_RESOURCES,
    HISTORY_OVERLAP_SECONDS,
    PAGE_DELAY,
    PRIMARY_KEYS,
)
from databricks.labs.community_connector.sources.fhir.fhir_utils import (
    RESOURCE_SCHEMA,
    FhirHttpClient,
    build_auth_client,
    extract_record,
    iter_bundle_entries,
    iter_bundle_pages,
)

logger = logging.getLogger(__name__)

_UNSET = object()


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
    """LakeflowConnect implementation for FHIR R4 servers.

    One table per resource type named in `resource_types`. All tables share
    RESOURCE_SCHEMA, so supporting another FHIR type needs no schema code.
    """

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)

        auth_client = build_auth_client(options)
        self._client = FhirHttpClient(base_url=options["base_url"], auth_client=auth_client)

        # Cap cursor at init time -- never chase data written after this trigger started.
        self._init_ts = datetime.now(timezone.utc).isoformat()
        self._fhir_version = _UNSET

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
        # Deletes come from each type's REST _history (opt-in: not every
        # server supports type-level history), in both REST and bulk modes.
        self._track_deletes = options.get("track_deletes", "false").lower() == "true"

        self._bulk_enabled = options.get("bulk_export_enabled", "false").lower() == "true"
        self._bulk_incremental_mode = options.get("bulk_incremental_mode", "since")
        # Servers differ on whether manifest transactionTime marks the start
        # or the completion of an export. A non-zero overlap re-reads a safe
        # window; duplicates are absorbed by the upsert on the primary key.
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
        return RESOURCE_SCHEMA

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        self._validate_table(table_name)
        if self._bulk_enabled and self._bulk_incremental_mode == "full":
            # A full export is a complete copy of the server's data, so ingest it
            # as a snapshot: resources deleted on the server leave the target.
            # As "cdc" they would be upserted forever and never removed.
            return {"primary_keys": PRIMARY_KEYS, "ingestion_type": "snapshot"}
        return {
            "primary_keys": PRIMARY_KEYS,
            "cursor_field": CURSOR_FIELD,
            "ingestion_type": "cdc_with_deletes" if self._track_deletes else "cdc",
        }

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Fetch one resource type with bundle pagination and _lastUpdated incremental sync.

        Uses max_records_per_batch admission control. Short-circuits when the
        cursor reaches _init_ts so a trigger never chases live writes.
        When bulk_export_enabled=true, delegates to _read_table_bulk() instead.
        """
        self._validate_table(table_name)
        if self._bulk_enabled:
            return self._read_table_bulk(table_name, start_offset)

        since = start_offset.get("cursor") if start_offset else None
        budget = _int_option(table_options, "max_records_per_batch", DEFAULT_MAX_RECORDS)
        resources, cursor = self._read_type(table_name, since, budget, table_options)
        if not resources or not cursor or cursor == since:
            # Nothing new, or nothing a cursor can advance past (every resource
            # lacks meta.lastUpdated): an unchanged offset ends the trigger.
            return iter([]), start_offset or {}
        return iter(self._records([(r, None) for r in resources])), {"cursor": cursor}

    def read_table_deletes(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read deletions of one resource type from GET [type]/_history (track_deletes=true).

        Runs once per trigger (the `run_ts` guard, as in bulk mode): history is
        newest-first, so it cannot be truncated safely and is read in full
        from the cursor. The cursor is this trigger's start minus
        HISTORY_OVERLAP_SECONDS, so the next trigger re-reads a small window
        rather than trust the server's clock to match the driver's.

        The first trigger reads nothing: its upsert stream loads current
        state, in which deleted resources are already absent.
        """
        self._validate_table(table_name)
        if start_offset and start_offset.get("run_ts") == self._init_ts:
            return iter([]), start_offset

        next_since = _parse_ts(self._init_ts) - timedelta(seconds=HISTORY_OVERLAP_SECONDS)
        end_offset = {"cursor": _canonical_ts(next_since.isoformat()), "run_ts": self._init_ts}
        since = start_offset.get("cursor") if start_offset else None
        if not since:
            return iter([]), end_offset
        return iter(self._read_deletes(table_name, since, table_options)), end_offset

    def _read_deletes(self, table_name: str, since: str, table_options: dict) -> list[dict]:
        """Delete records for every resource whose newest history entry is a DELETE.

        Some servers (HAPI, measured) give DELETE entries no timestamp. Because
        history is newest-first, the closest newer entry's timestamp bounds the
        deletion from above, so it is used as the delete's sequence value: it
        is later than the resource's last version, so the delete wins the
        merge. Deletions newer than every timestamped entry get the read time.
        A resource re-created after its deletion is left alone.
        """
        params = {
            "_since": since,
            "_count": str(_int_option(table_options, "page_size", DEFAULT_PAGE_SIZE)),
        }
        page_delay = _float_option(table_options, "page_delay", PAGE_DELAY)
        read_at = datetime.now(timezone.utc).isoformat()
        newer = read_at
        seen: set[str] = set()
        deleted: list[tuple[str, str]] = []
        try:
            for entry in iter_bundle_entries(
                self._client, f"{table_name}/_history", params, page_delay=page_delay
            ):
                resource = entry.get("resource") or {}
                stamp = (entry.get("response") or {}).get("lastModified") or _last_updated(resource)
                resource_id = resource.get("id") or _history_id(entry, table_name)
                if resource_id and resource_id not in seen:
                    seen.add(resource_id)
                    if (entry.get("request") or {}).get("method") == "DELETE":
                        deleted.append((resource_id, stamp or newer))
                if stamp:
                    newer = stamp
        except RuntimeError as exc:
            raise RuntimeError(
                f"track_deletes=true needs the server to support GET {table_name}/_history "
                f"with _since: {exc}"
            ) from exc

        fhir_version = self._server_fhir_version()
        return [
            {
                "resource_type": table_name,
                "resource_id": resource_id,
                "last_updated": deleted_at,
                "resource": None,
                "fhir_version": fhir_version,
                "ingested_at": read_at,
            }
            for resource_id, deleted_at in deleted
        ]

    def _read_type(
        self, resource_type: str, since: Optional[str], budget: int, table_options: dict
    ) -> tuple[list[dict], Optional[str]]:
        """Read one resource type from `since`; return (resources, cursor).

        Truncates at `budget`, but never inside a run of equal lastUpdated
        values: the next read asks for lastUpdated > cursor, so a tie left
        behind the cut would never be fetched. The batch may therefore exceed
        the budget by the size of the boundary tie.
        """
        if since and _parse_ts(since) >= _parse_ts(self._init_ts):
            return [], since

        page_size = _int_option(table_options, "page_size", DEFAULT_PAGE_SIZE)
        page_delay = _float_option(table_options, "page_delay", PAGE_DELAY)
        params: dict[str, str] = {"_count": str(page_size), "_sort": "_lastUpdated"}
        if since:
            params["_lastUpdated"] = f"gt{since}"

        resources: list[dict] = []
        boundary = None  # lastUpdated of the resource that filled the budget
        truncated = False
        out_of_order = False
        prev_dt = None
        for resource in iter_bundle_pages(
            self._client, resource_type, params, page_delay=page_delay
        ):
            ts = _last_updated(resource)
            dt = _parse_ts(ts) if ts else None
            if boundary is not None and dt != boundary:
                truncated = True
                break
            if dt is not None:
                if prev_dt is not None and dt < prev_dt:
                    out_of_order = True
                prev_dt = dt
            resources.append(resource)
            if boundary is None and len(resources) >= budget:
                boundary = dt
                if boundary is None:
                    # No timestamp to extend a tie on; cut here.
                    truncated = True
                    break

        # A truncated batch is only safe if results arrive sorted: otherwise the
        # cursor (max lastUpdated) jumps past resources not yet fetched.
        if truncated and out_of_order:
            raise RuntimeError(
                f"FHIR server returned '{resource_type}' results out of _lastUpdated order — "
                f"it appears to have ignored '_sort=_lastUpdated'. Truncating the batch at "
                f"max_records_per_batch would skip resources permanently. "
                f"Raise max_records_per_batch above the resource count, or use bulk export."
            )

        stamps = [ts for ts in map(_last_updated, resources) if ts]
        # Resources with a lastUpdated, none newer than `since`, mean the server
        # ignored the filter. Resources with no lastUpdated (0..1 in the spec)
        # are not evidence either way.
        if since and stamps and all(_parse_ts(ts) <= _parse_ts(since) for ts in stamps):
            raise RuntimeError(
                f"FHIR server appears to have ignored the '_lastUpdated=gt{since}' filter — "
                f"all {len(stamps)} '{resource_type}' resources with a lastUpdated are "
                f"<= '{since}'. This server may not support the _lastUpdated search "
                f"parameter. See README.md for server requirements."
            )

        cursor = max(stamps, key=_parse_ts) if stamps else since
        return resources, cursor

    def _read_table_bulk(self, table_name: str, start_offset: dict) -> tuple[Iterator[dict], dict]:
        """Bulk export path: one $export, scoped to this table's type, per trigger.

        The offset carries `run_ts`, this process's frozen `_init_ts`, so a
        later call in the same trigger short-circuits: a bulk $export is a whole
        server-side job (hours on real EHRs; Epic enforces a 24h cooldown per
        group), so a trigger must never run a second one just to find nothing
        new. A new trigger is a new process with a new `_init_ts`, so it exports
        again. No instance "already exported" flag is used: on a cache miss the
        framework replays a batch via readBetweenOffsets -> read_table, and a
        sticky flag would make that replay return nothing.

        `run_ts` is stamped even when the export is empty or the cursor cannot
        move. Leaving the offset unchanged makes the framework call again and
        run a second $export for the same trigger (measured against HAPI).

        There is no per-batch record cap: the cursor is the export's snapshot
        instant, which covers every resource in the manifest, so truncating
        while advancing it would drop the remainder permanently.
        """
        full_mode = self._bulk_incremental_mode == "full"
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

        transaction_time, exported = self._bulk_client.export(
            resource_types=[table_name],
            since=since,
            incremental_mode=self._bulk_incremental_mode,
        )
        records = self._bulk_records(table_name, exported)
        if full_mode:
            return iter(records), {"run_ts": self._init_ts}

        # Advance the cursor to the newest instant we can justify, never
        # backwards: start-time servers report a transactionTime that precedes
        # the stored cursor once a widened window is in play, and regressing
        # would re-read the same span every trigger.
        cursor_value = _canonical_ts(transaction_time) if transaction_time else cursor
        if cursor_value and cursor and _parse_ts(cursor_value) < _parse_ts(cursor):
            logger.warning(
                "Bulk export reported transactionTime %s, which precedes the stored "
                "cursor %s; keeping the stored cursor.",
                transaction_time,
                cursor,
            )
            cursor_value = cursor

        end_offset = {"run_ts": self._init_ts}
        if cursor_value:
            end_offset["cursor"] = cursor_value
        return iter(records), end_offset

    def _bulk_records(self, table_name: str, exported: list[tuple[str, dict]]) -> list[dict]:
        """Turn (raw NDJSON line, parsed resource) pairs into table records.

        The raw line becomes the VARIANT value, so FHIR decimals keep the
        precision the server sent; a json.loads/json.dumps round-trip would
        round them to float64. Each resource is typed by its own resourceType,
        so manifest entries that omit "type" are still ingested; resources of
        another type are dropped, and a resource listed twice (one file under
        two entries) is kept once.
        """
        by_id: dict[str, tuple[str, dict]] = {}
        ignored: dict[str, int] = {}
        for raw, resource in exported:
            resource_type = resource.get("resourceType")
            if resource_type != table_name:
                ignored[resource_type] = ignored.get(resource_type, 0) + 1
                continue
            by_id[resource.get("id")] = (raw, resource)
        if ignored:
            logger.warning(
                "Bulk export for '%s' also returned other resource types; ignoring %s",
                table_name,
                ignored,
            )
        return self._records([(resource, raw) for raw, resource in by_id.values()])

    def _records(self, pairs: list[tuple[dict, Optional[str]]]) -> list[dict]:
        """Build table records from (resource, raw JSON text or None) pairs."""
        fhir_version = self._server_fhir_version()
        ingested_at = datetime.now(timezone.utc).isoformat()
        return [
            extract_record(resource, raw=raw, fhir_version=fhir_version, ingested_at=ingested_at)
            for resource, raw in pairs
        ]

    def _server_fhir_version(self) -> Optional[str]:
        """FHIR version from the server's CapabilityStatement, fetched once per instance."""
        if self._fhir_version is _UNSET:
            self._fhir_version = None
            try:
                resp = self._client.get("metadata")
                if resp.status_code == 200:
                    self._fhir_version = resp.json().get("fhirVersion")
            except Exception as exc:  # pylint: disable=broad-except
                # Informational column only; never fail a read over it.
                logger.warning("Could not read fhirVersion from /metadata: %s", exc)
        return self._fhir_version

    def _widen_since(self, cursor: Optional[str]) -> Optional[str]:
        """Rewind the `_since` sent to the server by bulk_overlap_seconds.

        Servers differ on whether transactionTime marks the start or the end of
        an export, so a cursor taken from it can sit after resources that were
        written while the export ran. Widening only the query re-reads that
        window; duplicates are absorbed by the upsert on the primary key.
        """
        if not cursor:
            return None
        if not self._bulk_overlap_seconds:
            # Canonicalise even with no overlap: a stored "+00:00" cursor would
            # otherwise reach _typeFilter verbatim and be corrupted there.
            return _canonical_ts(cursor)
        shifted = _parse_ts(cursor) - timedelta(seconds=self._bulk_overlap_seconds)
        return _canonical_ts(shifted.isoformat())

    def _validate_table(self, table_name: str) -> None:
        if table_name not in self._resources:
            raise ValueError(
                f"Resource type '{table_name}' is not configured. Configured: {self._resources}"
            )


def _last_updated(resource: dict) -> Optional[str]:
    return (resource.get("meta") or {}).get("lastUpdated")


def _history_id(entry: dict, resource_type: str) -> Optional[str]:
    """Resource id from a history entry's request.url ("Patient/1/_history/2") or fullUrl."""
    for url in ((entry.get("request") or {}).get("url"), entry.get("fullUrl")):
        parts = (url or "").split("/")
        if resource_type in parts:
            i = len(parts) - 1 - parts[::-1].index(resource_type)
            if i + 1 < len(parts) and parts[i + 1]:
                return parts[i + 1]
    return None
