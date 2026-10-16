"""Klaviyo Lakeflow connector.

Implements ``LakeflowConnect`` against the Klaviyo JSON:API.  Eight
tables are supported: profiles, events, lists, campaigns, metrics,
flows, segments, templates.  See ``klaviyo_api_doc.md`` for endpoint
details, filter operators, rate limits, and per-table quirks.

Partitioning
------------
The connector also implements ``SupportsPartitionedStream`` so the
Spark engine can spread certain reads across executors.  Most Klaviyo
endpoints use opaque cursor pagination and don't benefit from parallel
windowed reads (and the smaller tiers have strict rate limits), so
``is_partitioned`` returns ``False`` for them — those tables fall back
to the single-driver ``read_table`` path.  Two tables opt in:

* ``events`` (XL tier — 350/s, 3500/m) is partitioned by time window.
  Klaviyo accepts ``greater-or-equal(datetime, ...)`` + ``less-than``
  so the connector can split the cursor range into windows and read
  each one on a different executor.
* ``campaigns`` is partitioned by channel.  Klaviyo *requires* the
  ``messages.channel`` filter on every campaign request.  On API
  revision 2024-10-15 the filter accepts ``email`` and ``sms`` only
  (verified live — ``mobile_push`` returns HTTP 400), so the
  connector fans out across those two channels.  Each channel still
  uses ordinary cursor pagination inside its partition.

Schema / Record shape
---------------------
Klaviyo wraps records as ``{type, id, attributes, relationships}``.
The connector flattens this to ``{id, type, ...attributes}`` so primary
keys are flat top-level columns (the engine doesn't allow dotted PKs).
See ``klaviyo_schemas.py`` for the resulting Spark schemas.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator

import requests
from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import (
    LakeflowConnect,
    SupportsPartitionedStream,
)
from databricks.labs.community_connector.sources.klaviyo.klaviyo_schemas import (
    CAMPAIGN_CHANNELS,
    CURSOR_FIELD,
    DEFAULT_LOOKBACK_SECONDS,
    GREATER_THAN_ONLY_TABLES,
    GTE_FILTER_TABLES,
    MAX_PAGE_SIZE,
    SORT_FIELD,
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
)
from databricks.labs.community_connector.sources.klaviyo.klaviyo_utils import (
    api_get,
    flatten_record,
    make_pacer,
    paginate_get,
    parse_int_option,
    relationship_id,
)


_LOG = logging.getLogger(__name__)


BASE_URL = "https://a.klaviyo.com/api"
DEFAULT_API_REVISION = "2024-10-15"

# Klaviyo can ingest events with a backdated ``datetime`` value — the
# api_doc recommends a 1-hour lookback for ``events`` to catch
# late-arriving records.
EVENTS_DEFAULT_LOOKBACK_SECONDS = 3600

# Default time-window size when partitioning ``events`` across
# executors.  One day is a reasonable balance — small accounts have
# < 1 day of events per window (cheap, one partition); large accounts
# get parallelism over multi-day backfills.  Override via the
# ``window_seconds`` table option.
EVENTS_DEFAULT_WINDOW_SECONDS = 86400


# --------------------------------------------------------------------------- #
# Datetime helpers
# --------------------------------------------------------------------------- #


def _now_iso() -> str:
    """Return the current UTC time as an RFC 3339 string."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_iso(value: str) -> datetime:
    """Parse an RFC 3339 / ISO 8601 string into an aware ``datetime``.

    Accepts either ``...Z`` or ``...+00:00`` suffixes.  Naive strings
    are interpreted as UTC.
    """
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _format_iso(value: datetime) -> str:
    """Format a ``datetime`` back to an RFC 3339 string with ``Z``."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _subtract_seconds(iso_value: str, seconds: int) -> str:
    """Return ``iso_value`` minus ``seconds`` as an RFC 3339 string."""
    if seconds <= 0:
        return iso_value
    return _format_iso(_parse_iso(iso_value) - timedelta(seconds=seconds))


def _add_seconds(iso_value: str, seconds: int) -> str:
    """Return ``iso_value`` plus ``seconds`` as an RFC 3339 string."""
    if seconds <= 0:
        return iso_value
    return _format_iso(_parse_iso(iso_value) + timedelta(seconds=seconds))


# =========================================================================== #
# Connector
# =========================================================================== #


class KlaviyoLakeflowConnect(LakeflowConnect, SupportsPartitionedStream):
    """LakeflowConnect implementation for Klaviyo.

    Connection options:
        api_key       (required, secret) — Klaviyo private API key
                      (prefix ``pk_``).
        api_revision  (optional, default ``2024-10-15``) — API revision
                      sent as the ``revision`` header.

    Per-table options (read from ``table_options`` per call):
        max_records_per_batch — admission cap; the connector returns at
                                most this many records per microbatch
                                and emits a partial cursor when reached.
        page_size             — page size for collection requests
                                (clamped to the per-endpoint max).
        lookback_seconds      — overrides the default lookback for
                                ``lists`` / ``segments`` (default 1)
                                and ``events`` (default 3600).
        window_seconds        — partition window size for ``events``
                                when running under the partitioned
                                stream path (default 86400).
    """

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)

        api_key = options.get("api_key")
        if not api_key:
            raise ValueError("Klaviyo connector requires 'api_key'")

        self._api_key = api_key
        self._api_revision = options.get("api_revision") or DEFAULT_API_REVISION

        self._session = requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Klaviyo-API-Key {self._api_key}",
                "revision": self._api_revision,
                "Accept": "application/json",
            }
        )

        # Admission-control cap.  CDC and append tables cap their
        # returned cursor at this value so ``Trigger.AvailableNow``
        # converges even on a busy account: once the stream drains to
        # ``_init_ts`` the next ``latest_offset`` / ``read_table`` call
        # returns a stable (no-progress) offset.  A subsequent trigger
        # creates a fresh connector instance with a newer ``_init_ts``
        # and picks up the gap.
        self._init_ts = _now_iso()

    # ------------------------------------------------------------------ #
    # LakeflowConnect interface
    # ------------------------------------------------------------------ #

    def list_tables(self) -> list[str]:
        return list(SUPPORTED_TABLES)

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        if table_name not in TABLE_SCHEMAS:
            raise ValueError(f"Unsupported table: {table_name!r}")
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        if table_name not in TABLE_METADATA:
            raise ValueError(f"Unsupported table: {table_name!r}")
        return TABLE_METADATA[table_name]

    def read_table(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        if table_name not in SUPPORTED_TABLES:
            raise ValueError(f"Unsupported table: {table_name!r}")

        start_offset = start_offset or {}
        table_options = table_options or {}

        if table_name == "metrics":
            return self._read_snapshot_table(table_name, table_options)
        if table_name == "campaigns":
            return self._read_campaigns(start_offset, table_options)
        if table_name == "events":
            return self._read_events_single(start_offset, table_options)
        # All remaining tables share the same paginated incremental
        # pattern with a cursor-bearing filter.
        return self._read_incremental(table_name, start_offset, table_options)

    # ------------------------------------------------------------------ #
    # SupportsPartitionedStream
    # ------------------------------------------------------------------ #

    def is_partitioned(self, table_name: str) -> bool:
        """Return True only for tables that benefit from parallel reads.

        * ``events`` — XL rate limit (350/s), high volume, supports
          ``greater-or-equal(datetime,...) AND less-than(datetime,...)``
          which we use to split the cursor range into windows.
        * ``campaigns`` — the required ``messages.channel`` filter on
          revision 2024-10-15 only accepts ``email`` and ``sms`` (see
          ``CAMPAIGN_CHANNELS`` for the live-verified set), so this is
          a two-way partition; each executor handles one channel's
          cursor-paginated walk.

        Every other table falls back to the single-driver
        ``read_table`` path: their per-second budgets are too low to
        benefit from parallelism (lists/segments cap at 10/page;
        metrics is a snapshot lookup), or they have strict rate limits
        (flows: 3/s).
        """
        return table_name in {"events", "campaigns"}

    def latest_offset(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict | None = None,
    ) -> dict:
        """Return the high-water-mark offset, capped at ``_init_ts``.

        Klaviyo doesn't expose a cheap "max cursor" endpoint, so we
        simply return ``_init_ts`` — the connector init timestamp —
        which serves as both the upper bound for partitioning and the
        AvailableNow termination cap.  Reading the actual max cursor
        from the source would require a paginated walk anyway.

        For ``campaigns`` the offset is structural (per-channel cursor
        bag); we still cap at ``_init_ts`` so windowing terminates.
        """
        if table_name == "campaigns":
            # Per-channel cursor bag plus the cap.
            offset: dict[str, str] = {"cap": self._init_ts}
            for channel in CAMPAIGN_CHANNELS:
                key = f"updated_at__{channel}"
                offset[key] = self._init_ts
            return offset
        # events (and any other partitioned table) — single cursor.
        return {"cursor": self._init_ts}

    def get_partitions(  # type: ignore[override]
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict | None = None,
        end_offset: dict | None = None,
    ) -> list[dict]:
        """Split a streaming offset range into per-executor partitions.

        * ``events`` — split the ``(start_cursor, end_cursor]`` time
          range into fixed-size windows; each window is one partition
          and ``read_partition`` queries it via
          ``greater-or-equal``/``less-than`` filters.
        * ``campaigns`` — emit one partition per channel value.  Each
          partition carries its per-channel start cursor.

        For all other tables, partitioned streaming is opted out via
        ``is_partitioned``; this method should not be called, but if
        the framework does invoke it we raise so the bug surfaces.
        """
        if table_name not in {"events", "campaigns"}:
            raise NotImplementedError(f"Table '{table_name}' does not support partitioned reads")

        table_options = table_options or {}

        if table_name == "campaigns":
            return self._campaigns_partitions(start_offset, end_offset)

        # events — time-window split.
        return self._events_partitions(table_options, start_offset, end_offset)

    def read_partition(  # type: ignore[override]
        self,
        table_name: str,
        partition: dict,
        table_options: dict[str, str],
    ) -> Iterator[dict]:
        """Read one partition's worth of records.

        Runs on a Spark executor; must rebuild any per-call state from
        ``self.options`` (which is preserved across executors because
        the connector is pickled by Spark).  Network sessions are
        constructed lazily inside the helper readers.
        """
        if table_name not in {"events", "campaigns"}:
            raise NotImplementedError(f"Table '{table_name}' does not support partitioned reads")

        table_options = table_options or {}

        if table_name == "events":
            return self._read_events_partition(partition, table_options)
        # campaigns
        return self._read_campaigns_partition(partition, table_options)

    # ================================================================== #
    # Snapshot tables (metrics)
    # ================================================================== #

    def _read_snapshot_table(
        self, table_name: str, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Full-refresh paginated read.  Used by ``metrics``.

        Klaviyo's ``metrics`` endpoint has no timestamp filter, so we
        always reload the full set.  The table is small (api_doc:
        typically < 200 records) and the L tier is generous.
        """
        url = f"{BASE_URL}/{table_name}/"
        params = self._initial_params(table_name, table_options)
        records: list[dict[str, Any]] = []
        for raw in paginate_get(self._session, url, params, table_name):
            records.append(flatten_record(raw))
        return iter(records), {}

    # ================================================================== #
    # Generic CDC / append reader
    # ================================================================== #

    def _read_incremental(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Paginated incremental read for the single-cursor tables.

        Handles ``profiles``, ``lists``, ``flows``, ``segments``,
        ``templates``.  ``events`` and ``campaigns`` use dedicated
        readers (events: 1-hour lookback + partitioned; campaigns:
        channel fan-out).

        Termination contract:
          * The cursor advances to ``max(seen_cursor, _init_ts)`` when
            the API window is fully drained.  This makes a follow-up
            call with the returned offset terminate immediately (since
            ``start >= _init_ts`` short-circuits).
          * The cursor is capped at ``_init_ts`` even when the API
            returns rows past it (defensive: server may not always
            honor a hypothetical ``less-than`` cap, and we don't
            enforce one for these L-tier endpoints to keep query
            shapes simple).
        """
        cursor_key = CURSOR_FIELD[table_name]
        since: str | None = start_offset.get("cursor")

        # Already drained up to init_ts — return a stable offset so
        # the framework terminates this microbatch.
        if since and since >= self._init_ts:
            return iter([]), start_offset

        max_records = parse_int_option(table_options, "max_records_per_batch")
        lookback = (
            parse_int_option(
                table_options,
                "lookback_seconds",
                DEFAULT_LOOKBACK_SECONDS.get(table_name, 0),
            )
            or 0
        )

        url = f"{BASE_URL}/{table_name}/"
        params = self._initial_params(table_name, table_options)
        params["sort"] = SORT_FIELD[table_name]

        if since:
            params["filter"] = self._make_cursor_filter(table_name, since, lookback)

        pacer = make_pacer(table_name)
        records: list[dict[str, Any]] = []
        max_seen: str | None = since
        cap_hit = False

        for raw in paginate_get(self._session, url, params, table_name, pacer=pacer):
            flat = flatten_record(raw)
            cursor_val = flat.get(cursor_key)

            # When we applied a lookback (``lists`` / ``segments``)
            # records strictly <= ``since`` slipped through the
            # server-side filter — drop them here.  For ``greater-or-
            # equal`` tables the server may also return the boundary
            # record on every refresh; drop it too to keep the cursor
            # strictly forward-moving.
            if since and cursor_val is not None and cursor_val <= since:
                continue

            # Defensive: drop records past init_ts so the cursor never
            # advances past the cap.
            if cursor_val is not None and cursor_val > self._init_ts:
                continue

            records.append(flat)
            if cursor_val is not None and (max_seen is None or cursor_val > max_seen):
                max_seen = cursor_val

            if max_records is not None and len(records) >= max_records:
                cap_hit = True
                break

        if cap_hit:
            return iter(records), {"cursor": max_seen}

        # Window drained — pin the cursor at init_ts so the next call
        # short-circuits (or, if we did see records past ``since``,
        # use the larger of the two).
        if max_seen and max_seen >= self._init_ts:
            end_cursor = max_seen
        else:
            end_cursor = self._init_ts
        return iter(records), {"cursor": end_cursor}

    # ================================================================== #
    # Events — single-driver path (paginated, 1-hour lookback)
    # ================================================================== #

    def _read_events_single(
        self,
        start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Single-driver event reader used when partitioning is off.

        Same shape as ``_read_incremental`` but with the events-
        specific 1-hour lookback and FK-hoisting for
        ``profile_id`` / ``metric_id``.  Used both when the framework
        falls back from ``SupportsPartitionedStream`` and as the
        ``read_table`` entry point for the same table.
        """
        since: str | None = start_offset.get("cursor")

        if since and since >= self._init_ts:
            return iter([]), start_offset

        max_records = parse_int_option(table_options, "max_records_per_batch")
        lookback = (
            parse_int_option(
                table_options,
                "lookback_seconds",
                EVENTS_DEFAULT_LOOKBACK_SECONDS,
            )
            or 0
        )

        # Apply lookback to the cursor we send to the server (events
        # supports ``greater-or-equal``).  We still strict-`>` filter
        # client-side to make the boundary exclusive.
        since_param = _subtract_seconds(since, lookback) if since else None

        url = f"{BASE_URL}/events/"
        params = self._initial_params("events", table_options)
        params["sort"] = SORT_FIELD["events"]
        filter_parts = []
        if since_param:
            filter_parts.append(f"greater-or-equal(datetime,{since_param})")
        # Server-side cap at init_ts so we never include records past
        # connector init — guarantees the cap_hit ladder converges.
        filter_parts.append(f"less-than(datetime,{self._init_ts})")
        params["filter"] = ",".join(filter_parts)

        records: list[dict[str, Any]] = []
        max_seen: str | None = since
        cap_hit = False

        for raw in paginate_get(self._session, url, params, "events"):
            flat = flatten_record(raw)
            flat["profile_id"] = relationship_id(raw, "profile")
            flat["metric_id"] = relationship_id(raw, "metric")

            cursor_val = flat.get("datetime")

            # Strict `>` to make the lookback-tolerant filter exclusive.
            if since and cursor_val is not None and cursor_val <= since:
                continue
            # Defensive: drop anything past the cap.
            if cursor_val is not None and cursor_val >= self._init_ts:
                continue

            records.append(flat)
            if cursor_val is not None and (max_seen is None or cursor_val > max_seen):
                max_seen = cursor_val

            if max_records is not None and len(records) >= max_records:
                cap_hit = True
                break

        if cap_hit:
            return iter(records), {"cursor": max_seen}

        if max_seen and max_seen >= self._init_ts:
            end_cursor = max_seen
        else:
            end_cursor = self._init_ts
        return iter(records), {"cursor": end_cursor}

    # ================================================================== #
    # Events — partitioned path
    # ================================================================== #

    def _events_partitions(
        self,
        table_options: dict[str, str],
        start_offset: dict | None,
        end_offset: dict | None,
    ) -> list[dict]:
        """Split the events cursor range into fixed-size time windows."""
        end_cursor = (end_offset or {}).get("cursor") or self._init_ts
        start_cursor = (start_offset or {}).get("cursor")
        if start_cursor is None:
            # First microbatch — start from a sensible default (an hour
            # before the cap).  The caller can override via
            # ``start_timestamp`` for backfills.
            override = (table_options or {}).get("start_timestamp")
            if override:
                start_cursor = override
            else:
                start_cursor = _subtract_seconds(end_cursor, EVENTS_DEFAULT_WINDOW_SECONDS)

        if start_cursor >= end_cursor:
            return []

        window_seconds = (
            parse_int_option(
                table_options,
                "window_seconds",
                EVENTS_DEFAULT_WINDOW_SECONDS,
            )
            or EVENTS_DEFAULT_WINDOW_SECONDS
        )

        partitions: list[dict] = []
        cur = start_cursor
        # Bound the number of partitions to avoid pathological splits
        # when a user passes a very old ``start_timestamp`` with a tiny
        # ``window_seconds``.  500 windows is plenty (e.g. 500 days at
        # 1-day windows).
        for _ in range(500):
            if cur >= end_cursor:
                break
            next_end = min(_add_seconds(cur, window_seconds), end_cursor)
            partitions.append({"start": cur, "end": next_end})
            cur = next_end
        return partitions

    def _read_events_partition(
        self, partition: dict, table_options: dict[str, str]
    ) -> Iterator[dict]:
        """Read records for a single events time-window partition.

        Runs on an executor.  Builds its own filter from the partition
        bounds; pagination uses ``links.next`` cursors as usual.
        """
        start = partition["start"]
        end = partition["end"]

        url = f"{BASE_URL}/events/"
        params = self._initial_params("events", table_options)
        params["sort"] = SORT_FIELD["events"]
        params["filter"] = f"greater-or-equal(datetime,{start}),less-than(datetime,{end})"

        records: list[dict[str, Any]] = []
        for raw in paginate_get(self._session, url, params, "events"):
            flat = flatten_record(raw)
            flat["profile_id"] = relationship_id(raw, "profile")
            flat["metric_id"] = relationship_id(raw, "metric")
            records.append(flat)
        return iter(records)

    # ================================================================== #
    # Campaigns — channel fan-out
    # ================================================================== #

    def _read_campaigns(
        self,
        start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Single-driver campaigns reader (channel fan-out).

        Klaviyo requires a ``messages.channel`` filter on every request,
        so we make one paginated request per channel and union the
        results.  The cursor is a per-channel bag, e.g.::

            {"updated_at__email": "...", "updated_at__sms": "..."}

        On revision 2024-10-15 the filter accepts ``email`` and ``sms``
        only; see ``CAMPAIGN_CHANNELS``.  Each channel terminates
        independently at ``_init_ts``; the microbatch terminates only
        when every channel has drained.  Dedup-by-id is unnecessary
        because a campaign belongs to exactly one channel — the filter
        is mutually exclusive across the active values.
        """
        max_records = parse_int_option(table_options, "max_records_per_batch")

        end_offset = dict(start_offset) if start_offset else {}
        all_records: list[dict[str, Any]] = []
        cap_hit = False

        for channel in CAMPAIGN_CHANNELS:
            key = f"updated_at__{channel}"
            since: str | None = (start_offset or {}).get(key)

            if since and since >= self._init_ts:
                # Channel already drained — keep the existing offset
                # and skip the request.
                end_offset[key] = since
                continue

            channel_records, channel_cursor, channel_cap_hit = self._read_campaigns_channel(
                channel=channel,
                since=since,
                table_options=table_options,
                remaining=((max_records - len(all_records)) if max_records is not None else None),
            )
            all_records.extend(channel_records)
            end_offset[key] = channel_cursor or (since or self._init_ts)
            if channel_cap_hit:
                cap_hit = True
                # We stop the fan-out here so the next microbatch
                # resumes from the correct per-channel cursors.
                break

        # Mark the rest of the channels as untouched (preserve their
        # existing offsets, or seed to None which is fine).
        if cap_hit:
            for channel in CAMPAIGN_CHANNELS:
                end_offset.setdefault(
                    f"updated_at__{channel}",
                    (start_offset or {}).get(f"updated_at__{channel}"),
                )

        # AvailableNow termination guard: if nothing changed return
        # start_offset verbatim so the framework converges.
        if not all_records and end_offset == (start_offset or {}):
            return iter([]), start_offset or {}
        return iter(all_records), end_offset

    def _read_campaigns_channel(
        self,
        channel: str,
        since: str | None,
        table_options: dict[str, str],
        remaining: int | None,
    ) -> tuple[list[dict[str, Any]], str | None, bool]:
        """Walk one campaigns channel; return ``(records, cursor, capped)``.

        ``remaining`` caps the records returned by this channel call.
        ``None`` means uncapped.  ``capped`` is True when we stopped
        early because we hit the cap.
        """
        url = f"{BASE_URL}/campaigns/"
        params = self._initial_params("campaigns", table_options)
        params["sort"] = SORT_FIELD["campaigns"]

        filter_parts = [f"equals(messages.channel,'{channel}')"]
        if since:
            filter_parts.append(f"greater-or-equal(updated_at,{since})")
        # Server-side cap to guarantee termination.  ``campaigns``
        # supports ``less-than`` on ``updated_at`` per the api_doc.
        filter_parts.append(f"less-than(updated_at,{self._init_ts})")
        params["filter"] = ",".join(filter_parts)

        records: list[dict[str, Any]] = []
        max_seen: str | None = since
        cap_hit = False

        for raw in paginate_get(self._session, url, params, "campaigns"):
            flat = flatten_record(raw)
            # Stamp the queried channel on the row so callers can join
            # / filter by it without re-parsing nested data.
            flat["channel"] = channel
            cursor_val = flat.get("updated_at")

            if since and cursor_val is not None and cursor_val <= since:
                continue
            if cursor_val is not None and cursor_val >= self._init_ts:
                continue

            records.append(flat)
            if cursor_val is not None and (max_seen is None or cursor_val > max_seen):
                max_seen = cursor_val

            if remaining is not None and len(records) >= remaining:
                cap_hit = True
                break

        if cap_hit:
            return records, max_seen, True
        # Window drained — pin cursor at init_ts so next call short-
        # circuits.
        end_cursor = max_seen if max_seen and max_seen >= self._init_ts else self._init_ts
        return records, end_cursor, False

    def _campaigns_partitions(
        self,
        start_offset: dict | None,
        end_offset: dict | None,
    ) -> list[dict]:
        """One partition per Klaviyo campaign channel.

        Channels are read independently — that's the natural axis of
        parallelism for this endpoint.  We propagate per-channel
        cursors from ``start_offset`` into each partition descriptor
        so executors don't need driver-side state.

        Returns an empty list when ``start_offset == end_offset`` (i.e.
        the microbatch has nothing to do) — the framework's
        convergence contract requires equality-of-offsets to produce
        zero partitions.
        """
        # Convergence: when start and end offsets are equal, every
        # channel has already drained to the cap. Nothing to schedule.
        if start_offset is not None and end_offset is not None and start_offset == end_offset:
            return []
        del end_offset  # cap is already in self._init_ts
        partitions = []
        for channel in CAMPAIGN_CHANNELS:
            key = f"updated_at__{channel}"
            since = (start_offset or {}).get(key)
            partitions.append({"channel": channel, "since": since or ""})
        return partitions

    def _read_campaigns_partition(
        self, partition: dict, table_options: dict[str, str]
    ) -> Iterator[dict]:
        """Read one channel's worth of campaigns on an executor."""
        channel = partition["channel"]
        since = partition.get("since") or None
        records, _, _ = self._read_campaigns_channel(
            channel=channel,
            since=since,
            table_options=table_options,
            remaining=None,
        )
        return iter(records)

    # ================================================================== #
    # Helpers
    # ================================================================== #

    def _initial_params(self, table_name: str, table_options: dict[str, str]) -> dict[str, str]:
        """Return the per-page params for a paginated request.

        Applies ``page[size]`` based on ``page_size`` from
        ``table_options`` clamped to the endpoint's max.

        ``metrics`` and ``campaigns`` reject the ``page[size]`` parameter
        at runtime with HTTP 400 (``'page_size' is not a valid field for
        the resource ...``) — verified live against the 2024-10-15 API
        revision.  Both still support ``page[cursor]`` for pagination,
        so we just omit ``page[size]`` and let the server use its
        default page size.  ``MAX_PAGE_SIZE`` is set to 0 for these
        tables to opt them out cleanly.
        """
        max_size = MAX_PAGE_SIZE.get(table_name, 100)
        if max_size <= 0:
            return {}
        requested = parse_int_option(table_options, "page_size", max_size)
        size = max(1, min(requested or max_size, max_size))
        return {"page[size]": str(size)}

    def _make_cursor_filter(self, table_name: str, since: str, lookback_seconds: int) -> str:
        """Build the ``filter=`` expression for an incremental call.

        Tables fall into two filter dialects:

        * ``GREATER_THAN_ONLY_TABLES`` (lists, segments) — only
          ``greater-than`` is supported on ``updated``.  We subtract
          ``lookback_seconds`` from ``since`` so the strict ``>``
          boundary catches records whose timestamp equals the previous
          cursor.
        * ``GTE_FILTER_TABLES`` — ``greater-or-equal`` is supported.
          We use that directly and drop the boundary record client-
          side to keep the cursor strictly forward-moving.
        """
        cursor_field = CURSOR_FIELD[table_name]
        if table_name in GREATER_THAN_ONLY_TABLES:
            bound = _subtract_seconds(since, lookback_seconds) if lookback_seconds > 0 else since
            return f"greater-than({cursor_field},{bound})"
        if table_name in GTE_FILTER_TABLES:
            return f"greater-or-equal({cursor_field},{since})"
        # Defensive fallback — shouldn't be reachable for the tables
        # that go through this code path.
        return f"greater-or-equal({cursor_field},{since})"
