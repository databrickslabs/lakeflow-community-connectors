"""Energy Quantified Lakeflow connector.

Wraps the public Energy Quantified REST API (https://app.energyquantified.com/api)
behind a single ``LakeflowConnect`` implementation.

The connector exposes six tables drawn from EQ's curve-centric data
model.  Five of them are curve-scoped (one curve per table option)
and four use range queries that map naturally to partitioned reads:

    +-------------+------------+----------+-----------------------------+
    | Table       | Ingestion  | Cursor   | Partitioned?                |
    +-------------+------------+----------+-----------------------------+
    | curves      | snapshot   | -        | No (single page sequence)   |
    | timeseries  | append     | datetime | Yes (begin/end range)       |
    | instances   | append     | issued   | No (cursor walk-backward)   |
    | periods     | cdc        | begin    | Yes (begin/end range)       |
    | ohlc        | append     | traded_at| Yes (begin/end range)       |
    | srmc        | append     | date     | Yes (begin/end range)       |
    +-------------+------------+----------+-----------------------------+

For partitioned tables the connector implements
``SupportsPartitionedStream`` so Spark can split a date range across
executors.  ``is_partitioned`` returns ``False`` for ``curves`` and
``instances`` so the framework falls back to ``simpleStreamReader``
for those two.

See ``energy_quantified_api_doc.md`` for full endpoint details, the
``+`` encoding quirk for curve names, EQ's ``Europe/Gas_Day`` synthetic
timezone, and other per-table notes.
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
from databricks.labs.community_connector.sources.energy_quantified.energy_quantified_schemas import (
    API_BASE_URL,
    DEFAULT_CURVES_PAGE_SIZE,
    DEFAULT_INITIAL_LOOKBACK_DAYS,
    DEFAULT_INSTANCES_LIMIT,
    DEFAULT_LOOKBACK_DAYS,
    DEFAULT_PARTITION_DAYS,
    PARTITIONED_TABLES,
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
)
from databricks.labs.community_connector.sources.energy_quantified.energy_quantified_utils import (
    RateLimiter,
    api_get,
    days_ago_iso_date,
    encode_curve_name,
    parse_int_option,
    parse_iso_date_or_datetime,
    parse_max_records,
    require_option,
    to_iso_date,
    to_iso_datetime,
)


_LOG = logging.getLogger(__name__)


class EnergyQuantifiedLakeflowConnect(LakeflowConnect, SupportsPartitionedStream):
    """LakeflowConnect implementation for Energy Quantified.

    Connection options:
        api_key (required, secret) — Energy Quantified API key, sent
            as the ``X-API-Key`` header on every request.

    Per-table options (read from ``table_options`` per call):
        curve_name (required for non-``curves`` tables)
            Exact curve identifier; URL-encoded into the path.
        period (required for ``srmc``; optional for ``ohlc``)
            Contract period — ``month`` / ``quarter`` / ``year`` /
            ``week`` / ``day``.
        front (required for ``srmc`` unless ``delivery`` is provided;
            optional for ``ohlc``)
            Front contract number (1 = front month, 2 = next, ...).
        delivery (optional for ``ohlc`` / ``srmc``)
            Specific delivery date.  Mutually exclusive with
            ``front`` on ``srmc``.
        frequency (optional for ``timeseries``)
            Aggregation frequency override (``H``, ``P1D``, ...).
        timezone (optional)
            Timezone for returned datetimes.  Defaults to ``UTC``.
        start_date / end_date (optional)
            Date range for range-query tables.  Falls back to
            ``now() - 30d`` and ``now()`` respectively.
        lookback_days (optional)
            Cursor lookback for the ``periods`` CDC table (default 7).
            Re-scans recently-opened intervals each microbatch to
            catch revisions.
        partition_days (optional)
            Per-partition window size for partitioned reads.  Default
            30 days; tune to balance executor parallelism against
            request count.
        max_records_per_batch (optional)
            Admission-control cap on records returned per call.
    """

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)

        api_key = options.get("api_key")
        if not api_key:
            raise ValueError("Energy Quantified connector requires 'api_key'")
        self._api_key = api_key

        self._session = requests.Session()
        self._session.headers.update(
            {
                "X-API-Key": api_key,
                "Accept": "application/json",
            }
        )
        self._rate_limiter = RateLimiter()

        # Init-time cap.  Append / CDC tables clamp their returned
        # cursor to this value so Trigger.AvailableNow converges even
        # when the source is continuously publishing new data.  A
        # subsequent trigger creates a fresh instance with a newer
        # ``_init_ts`` and picks up the gap.
        now = datetime.now(timezone.utc)
        self._init_ts = to_iso_datetime(now)
        self._init_date = to_iso_date(now)

    # ================================================================== #
    # LakeflowConnect interface
    # ================================================================== #

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

        if table_name == "curves":
            return self._read_curves(table_options)
        if table_name == "instances":
            return self._read_instances(start_offset, table_options)

        # Range-query tables.  They prefer the partitioned path (via
        # ``read_partition``) but must remain usable from the single-
        # driver fallback too: SimpleDataSourceStreamReader is used
        # whenever ``is_partitioned`` returns False, and the framework
        # may also call ``read_table`` directly for batch reads if
        # ``get_partitions`` raises.
        if table_name == "timeseries":
            return self._read_timeseries(start_offset, table_options)
        if table_name == "ohlc":
            return self._read_ohlc(start_offset, table_options)
        if table_name == "srmc":
            return self._read_srmc(start_offset, table_options)
        if table_name == "periods":
            return self._read_periods(start_offset, table_options)

        raise ValueError(f"Unhandled table: {table_name!r}")

    # ================================================================== #
    # SupportsPartitionedStream
    # ================================================================== #

    def is_partitioned(self, table_name: str) -> bool:
        """Return whether this table runs through the partitioned path.

        ``curves`` (paginated catalog) and ``instances`` (cursor walk-
        backward) stay on the single-driver path.  The four range-
        query tables benefit from parallel reads.
        """
        return table_name in PARTITIONED_TABLES

    def latest_offset(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict | None = None,
    ) -> dict:
        """Return the high-water-mark offset, capped at ``_init_ts``.

        Only invoked for partitioned-streaming tables — see
        ``is_partitioned``.  The offset value is the ISO 8601 end
        date that the next microbatch should read up to (exclusive),
        clamped at the init-time cap.
        """
        if table_name not in PARTITIONED_TABLES:
            # Defensive — the framework only calls this for
            # partitioned tables.
            return {"cursor": self._init_date}

        end_date = table_options.get("end_date")
        if end_date:
            return {"cursor": min(end_date, self._init_date)}
        return {"cursor": self._init_date}

    def get_partitions(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict | None = None,
        end_offset: dict | None = None,
    ) -> list[dict]:
        """Split the date range into per-partition windows.

        The framework calls this for every table when the connector
        inherits from ``SupportsPartition`` — ``is_partitioned`` is
        only honoured on the streaming path.  For non-partitioned
        tables we raise ``NotImplementedError`` so the framework's
        per-task proxy falls back to ``read_table`` (it treats any
        exception from ``get_partitions`` as "this table isn't
        partitionable").
        """
        if table_name not in PARTITIONED_TABLES:
            raise NotImplementedError(f"Table '{table_name}' does not support partitioned reads")

        partition_days = max(
            1, parse_int_option(table_options, "partition_days", DEFAULT_PARTITION_DAYS)
        )

        if start_offset is None and end_offset is None:
            # Batch mode — cover the full user-supplied range, clamped
            # at the init-time cap.
            start = self._resolve_start_date(table_options)
            end = min(self._resolve_end_date(table_options), self._init_date)
        else:
            start = (start_offset or {}).get("cursor")
            if not start:
                start = self._resolve_start_date(table_options)
            end = (end_offset or {}).get("cursor", self._init_date)
            # Spark's micro-batch loop terminates by feeding the prior
            # end_offset back as start_offset. When the two match,
            # there is no new data to scan — return an empty partition
            # list directly so the CDC lookback widening (below) cannot
            # synthesize a non-empty range from equal offsets.
            if start == end:
                return []
            # CDC tables widen the window backward by ``lookback_days``
            # to catch revisions to recently opened intervals.
            if table_name == "periods":
                lookback = parse_int_option(table_options, "lookback_days", DEFAULT_LOOKBACK_DAYS)
                start = _subtract_days_iso_date(start, lookback)

        if not start or not end or start >= end:
            return []

        # Curve scope is required for every partitioned table; surface
        # the missing-option error here rather than waiting for an
        # executor-side failure.
        require_option(table_options, "curve_name", table_name)

        return _split_date_range_into_partitions(start, end, partition_days)

    def read_partition(
        self,
        table_name: str,
        partition: dict,
        table_options: dict[str, str],
    ) -> Iterator[dict]:
        """Read records for one partitioned date window.

        Runs on Spark executors.  ``partition`` carries the
        ``begin``/``end`` window; the curve and any per-table
        parameters come from ``table_options``.
        """
        if table_name not in PARTITIONED_TABLES:
            raise ValueError(f"Table '{table_name}' does not support partitioned reads")
        begin = partition["begin"]
        end = partition["end"]

        if table_name == "timeseries":
            records = self._fetch_timeseries(table_options, begin, end)
        elif table_name == "ohlc":
            records = self._fetch_ohlc(table_options, begin, end)
        elif table_name == "srmc":
            records = self._fetch_srmc(table_options, begin, end)
        elif table_name == "periods":
            records = self._fetch_periods(table_options, begin, end)
        else:
            raise ValueError(f"Unsupported table for partitioned read: {table_name}")
        return iter(records)

    # ================================================================== #
    # curves — paginated catalog (snapshot)
    # ================================================================== #

    def _read_curves(self, table_options: dict[str, str]) -> tuple[Iterator[dict], dict]:
        """Read the full curve catalog by page-walking ``/metadata/curves/``.

        EQ returns an envelope ``{total_items, total_pages, page,
        page_size, data: [...]}``.  We iterate until ``page >=
        total_pages``.
        """
        page_size = parse_int_option(table_options, "page_size", DEFAULT_CURVES_PAGE_SIZE)
        params: dict[str, Any] = {"page-size": page_size}

        # Optional catalog filters — pass through verbatim using the
        # hyphenated query param names EQ expects.  Only declared
        # options are forwarded so the connector's external_options
        # allowlist stays tight.
        for opt_key, http_key in (
            ("q", "q"),
            ("area", "area"),
            ("curve_type", "curve-type"),
            ("data_type", "data-type"),
            ("category", "category"),
            ("exact_category", "exact-category"),
            ("commodity", "commodity"),
            ("source", "source"),
            ("frequency", "frequency"),
            ("only_subscribed", "only-subscribed"),
            ("has_place", "has-place"),
        ):
            if opt_key in table_options:
                params[http_key] = table_options[opt_key]

        url = f"{API_BASE_URL}/metadata/curves/"
        records: list[dict] = []
        page = 1
        while True:
            params["page"] = page
            body = api_get(
                self._session,
                url,
                "curves",
                rate_limiter=self._rate_limiter,
                params=params,
            )
            for raw in body.get("data", []) or []:
                records.append(_normalise_curve(raw))
            total_pages = int(body.get("total_pages") or 0)
            if page >= total_pages or total_pages == 0:
                break
            page += 1

        return iter(records), {}

    # ================================================================== #
    # instances — cursor walk-backward (append)
    # ================================================================== #

    def _read_instances(
        self,
        start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Read forecast instances for one curve.

        ``/instances/{curve}/`` returns full timeseries data per
        instance.  EQ's list is reverse-chronological (newest first)
        and the cursor walks backward via ``issued-at-latest``.
        Incremental sync uses ``issued-at-earliest`` advanced to the
        max ``issued`` seen so far.
        """
        curve_name = require_option(table_options, "curve_name", "instances")
        since: str | None = start_offset.get("issued")
        if since and since >= self._init_ts:
            return iter([]), start_offset

        max_records = parse_max_records(table_options)
        limit = parse_int_option(table_options, "limit", DEFAULT_INSTANCES_LIMIT)
        # EQ caps at 25 (10 with ensembles); enforce so a misconfigured
        # option doesn't trigger a 400 from the server.
        limit = max(1, min(limit, 25))

        # Walk backward from the upper bound (init_ts).  On subsequent
        # microbatches we constrain the lower bound to ``since`` so we
        # only fetch newly-issued forecasts.
        params: dict[str, Any] = {
            "limit": limit,
            "issued-at-latest": self._init_ts,
        }
        if since:
            params["issued-at-earliest"] = since

        # Optional tag filters.
        if "tags" in table_options:
            params["tags"] = table_options["tags"]
        if "exclude_tags" in table_options:
            params["exclude-tags"] = table_options["exclude_tags"]
        if "frequency" in table_options:
            params["frequency"] = table_options["frequency"]
        params["timezone"] = table_options.get("timezone", "UTC")

        url = f"{API_BASE_URL}/instances/{encode_curve_name(curve_name)}/"

        records: list[dict] = []
        max_issued: str | None = since
        cap_hit = False
        cursor_upper = self._init_ts

        while True:
            params["issued-at-latest"] = cursor_upper
            body = api_get(
                self._session,
                url,
                "instances",
                rate_limiter=self._rate_limiter,
                params=params,
            )
            # EQ returns a JSON array of timeseries objects for the
            # full-load endpoint.
            batch = body if isinstance(body, list) else []
            if not batch:
                break

            oldest_in_batch: str | None = None
            for ts_obj in batch:
                for rec in _explode_instance_response(curve_name, ts_obj):
                    issued = rec.get("issued")
                    if since and issued and issued <= since:
                        continue
                    if issued and issued > self._init_ts:
                        # Defensive — should be filtered server-side
                        # via ``issued-at-latest=_init_ts``.
                        continue
                    records.append(rec)
                    if issued and (max_issued is None or issued > max_issued):
                        max_issued = issued
                    if max_records is not None and len(records) >= max_records:
                        cap_hit = True
                        break
                if cap_hit:
                    break
                instance_issued = (ts_obj.get("instance") or {}).get("issued")
                if instance_issued and (
                    oldest_in_batch is None or instance_issued < oldest_in_batch
                ):
                    oldest_in_batch = instance_issued
            if cap_hit:
                break

            # EQ returns up to ``limit`` instances per call; stop when
            # the page is short or we've walked all the way back to
            # ``since``.
            if len(batch) < limit:
                break
            if oldest_in_batch is None:
                break
            if since and oldest_in_batch <= since:
                break
            # Advance the upper cursor backward to one tick before
            # the oldest issue we just saw to avoid re-fetching it.
            cursor_upper = oldest_in_batch

        if cap_hit:
            return iter(records), {"issued": max_issued or self._init_ts}

        # Window fully drained — anchor at init_ts so subsequent calls
        # short-circuit via the early-exit at the top of this method.
        end_cursor = max_issued if max_issued and max_issued >= self._init_ts else self._init_ts
        end_offset = {"issued": end_cursor}
        if start_offset and start_offset == end_offset:
            return iter([]), start_offset
        return iter(records), end_offset

    # ================================================================== #
    # Range-query readers (timeseries / ohlc / srmc / periods)
    #
    # Each ``_read_*`` helper handles the single-driver fallback path:
    # it walks the date range in partition-sized windows internally
    # and returns a (records, offset) tuple suitable for read_table.
    # The corresponding ``_fetch_*`` helper handles a single window
    # and is what the partitioned path calls from read_partition.
    # ================================================================== #

    def _read_timeseries(
        self,
        start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        return self._read_range_table(
            "timeseries",
            start_offset,
            table_options,
            cursor_field="datetime",
            fetcher=self._fetch_timeseries,
        )

    def _read_ohlc(
        self,
        start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        return self._read_range_table(
            "ohlc",
            start_offset,
            table_options,
            cursor_field="traded_at",
            fetcher=self._fetch_ohlc,
        )

    def _read_srmc(
        self,
        start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        return self._read_range_table(
            "srmc",
            start_offset,
            table_options,
            cursor_field="date",
            fetcher=self._fetch_srmc,
        )

    def _read_periods(
        self,
        start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        return self._read_range_table(
            "periods",
            start_offset,
            table_options,
            cursor_field="begin",
            fetcher=self._fetch_periods,
            apply_lookback=True,
        )

    def _read_range_table(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
        *,
        cursor_field: str,
        fetcher,
        apply_lookback: bool = False,
    ) -> tuple[Iterator[dict], dict]:
        """Single-driver fallback for range-query tables.

        Walks a date range in partition-sized windows on the driver
        and returns the union of records plus a forward-advancing
        cursor capped at ``_init_date``.  Honours
        ``max_records_per_batch`` for admission control: when the
        cap is hit mid-walk we stop and return a partial cursor.
        """
        require_option(table_options, "curve_name", table_name)
        cursor_key = "cursor"
        since: str | None = start_offset.get(cursor_key)
        if since and since >= self._init_date:
            return iter([]), start_offset

        start = since or self._resolve_start_date(table_options)
        if apply_lookback and since:
            lookback = parse_int_option(table_options, "lookback_days", DEFAULT_LOOKBACK_DAYS)
            start = _subtract_days_iso_date(start, lookback)

        end = min(self._resolve_end_date(table_options), self._init_date)
        if start >= end:
            terminal = {cursor_key: self._init_date}
            if start_offset and start_offset == terminal:
                return iter([]), start_offset
            return iter([]), terminal

        partition_days = max(
            1, parse_int_option(table_options, "partition_days", DEFAULT_PARTITION_DAYS)
        )
        max_records = parse_max_records(table_options)

        records: list[dict] = []
        last_window_end = start
        cap_hit = False

        for window in _split_date_range_into_partitions(start, end, partition_days):
            batch = fetcher(table_options, window["begin"], window["end"])
            for rec in batch:
                cursor_val = rec.get(cursor_field)
                if cursor_val and cursor_val > self._init_date:
                    # Defensive — outside the window we requested.
                    continue
                records.append(rec)
                if max_records is not None and len(records) >= max_records:
                    cap_hit = True
                    break
            last_window_end = window["end"]
            if cap_hit:
                break

        if cap_hit:
            # Resume at the last completed window's end on the next
            # call so we don't lose data inside a partially-emitted
            # window (the underlying fetch is for the whole window,
            # so progress is window-granular not record-granular).
            return iter(records), {cursor_key: last_window_end}

        terminal = {cursor_key: self._init_date}
        if start_offset and start_offset == terminal:
            return iter([]), start_offset
        return iter(records), terminal

    # ------------------------------------------------------------------ #
    # Per-window fetchers (used by both single-driver and partitioned paths)
    # ------------------------------------------------------------------ #

    def _fetch_timeseries(
        self,
        table_options: dict[str, str],
        begin: str,
        end: str,
    ) -> list[dict]:
        curve_name = require_option(table_options, "curve_name", "timeseries")
        params = self._timeseries_common_params(table_options, begin, end)
        if "aggregation" in table_options:
            params["aggregation"] = table_options["aggregation"]
        if "hour_filter" in table_options:
            params["hour-filter"] = table_options["hour_filter"]
        if "threshold" in table_options:
            params["threshold"] = table_options["threshold"]
        if "threshold_pct" in table_options:
            params["threshold-pct"] = table_options["threshold_pct"]
        url = f"{API_BASE_URL}/timeseries/{encode_curve_name(curve_name)}/"
        body = api_get(
            self._session,
            url,
            "timeseries",
            rate_limiter=self._rate_limiter,
            params=params,
        )
        return _explode_timeseries_response(curve_name, body)

    def _fetch_ohlc(
        self,
        table_options: dict[str, str],
        begin: str,
        end: str,
    ) -> list[dict]:
        curve_name = require_option(table_options, "curve_name", "ohlc")
        params: dict[str, Any] = {"begin": begin, "end": end}
        if "period" in table_options:
            params["period"] = table_options["period"]
        if "delivery" in table_options:
            params["delivery"] = table_options["delivery"]
        if "front" in table_options:
            params["front"] = table_options["front"]
        if "unit" in table_options:
            params["unit"] = table_options["unit"]
        url = f"{API_BASE_URL}/ohlc/{encode_curve_name(curve_name)}/"
        body = api_get(
            self._session,
            url,
            "ohlc",
            rate_limiter=self._rate_limiter,
            params=params,
        )
        return _explode_ohlc_response(curve_name, body)

    def _fetch_srmc(
        self,
        table_options: dict[str, str],
        begin: str,
        end: str,
    ) -> list[dict]:
        curve_name = require_option(table_options, "curve_name", "srmc")
        period = require_option(table_options, "period", "srmc")
        if "front" not in table_options and "delivery" not in table_options:
            raise ValueError(
                "Table 'srmc' requires either table_options['front'] or table_options['delivery']"
            )

        params: dict[str, Any] = {"begin": begin, "end": end, "period": period}
        if "front" in table_options:
            params["front"] = table_options["front"]
        if "delivery" in table_options:
            params["delivery"] = table_options["delivery"]
        for opt_key, http_key in (
            ("fill", "fill"),
            ("unit", "unit"),
            ("efficiency", "efficiency"),
            ("carbon_emissions", "carbon_emissions"),
            ("api2_tonne_to_mwh", "api2_tonne_to_mwh"),
            ("gas_therm_to_mwh", "gas_therm_to_mwh"),
            ("carbon_tax_area", "carbon_tax_area"),
        ):
            if opt_key in table_options:
                params[http_key] = table_options[opt_key]

        url = f"{API_BASE_URL}/srmc/{encode_curve_name(curve_name)}/timeseries/"
        body = api_get(
            self._session,
            url,
            "srmc",
            rate_limiter=self._rate_limiter,
            params=params,
        )
        return _explode_srmc_response(curve_name, body, table_options)

    def _fetch_periods(
        self,
        table_options: dict[str, str],
        begin: str,
        end: str,
    ) -> list[dict]:
        curve_name = require_option(table_options, "curve_name", "periods")
        params: dict[str, Any] = {
            "begin": begin,
            "end": end,
            "timezone": table_options.get("timezone", "UTC"),
        }
        if "unit" in table_options:
            params["unit"] = table_options["unit"]
        url = f"{API_BASE_URL}/periods/{encode_curve_name(curve_name)}/"
        body = api_get(
            self._session,
            url,
            "periods",
            rate_limiter=self._rate_limiter,
            params=params,
        )
        return _explode_periods_response(curve_name, body)

    # ------------------------------------------------------------------ #
    # Param / date resolution helpers
    # ------------------------------------------------------------------ #

    def _timeseries_common_params(
        self,
        table_options: dict[str, str],
        begin: str,
        end: str,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "begin": begin,
            "end": end,
            "timezone": table_options.get("timezone", "UTC"),
        }
        if "frequency" in table_options:
            params["frequency"] = table_options["frequency"]
        if "unit" in table_options:
            params["unit"] = table_options["unit"]
        return params

    def _resolve_start_date(self, table_options: dict[str, str]) -> str:
        explicit = table_options.get("start_date")
        if explicit:
            return explicit
        return days_ago_iso_date(DEFAULT_INITIAL_LOOKBACK_DAYS)

    def _resolve_end_date(self, table_options: dict[str, str]) -> str:
        explicit = table_options.get("end_date")
        if explicit:
            return explicit
        return self._init_date


# =========================================================================== #
# Response normalisation helpers
# =========================================================================== #


def _normalise_curve(raw: dict[str, Any]) -> dict[str, Any]:
    """Return a curve record with only the schema's declared fields.

    Drops any keys the API may add in future versions so the schema
    stays stable.  ``place`` is preserved as-is; the embedded struct
    schema tolerates missing keys.
    """
    if not isinstance(raw, dict):
        return {}
    out: dict[str, Any] = {
        "name": raw.get("name"),
        "curve_type": raw.get("curve_type"),
        "data_type": raw.get("data_type"),
        "area": raw.get("area"),
        "area_sink": raw.get("area_sink"),
        "area_source": raw.get("area_source"),
        "place": _slim_place(raw.get("place")),
        "frequency": raw.get("frequency"),
        "timezone": raw.get("timezone"),
        "categories": raw.get("categories"),
        "unit": raw.get("unit"),
        "denominator": raw.get("denominator"),
        "source": raw.get("source"),
        "commodity": raw.get("commodity"),
        "subscription": _slim_subscription(raw.get("subscription")),
    }
    return out


def _slim_place(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    return {
        "type": value.get("type"),
        "key": value.get("key"),
        "name": value.get("name"),
        "unit": value.get("unit"),
        "fuels": value.get("fuels"),
        "areas": value.get("areas"),
    }


def _slim_subscription(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    return {"access": value.get("access"), "type": value.get("type")}


def _embedded_curve(raw: dict[str, Any]) -> dict[str, Any]:
    """Slim down the embedded curve metadata to the declared struct fields."""
    if not isinstance(raw, dict):
        return {}
    return {
        "name": raw.get("name"),
        "curve_type": raw.get("curve_type"),
        "data_type": raw.get("data_type"),
        "area": raw.get("area"),
        "frequency": raw.get("frequency"),
        "timezone": raw.get("timezone"),
        "unit": raw.get("unit"),
        "denominator": raw.get("denominator"),
        "source": raw.get("source"),
        "commodity": raw.get("commodity"),
    }


def _explode_timeseries_response(curve_name: str, body: dict[str, Any]) -> list[dict[str, Any]]:
    """Flatten ``/timeseries/{curve}/`` response into one row per data point."""
    if not isinstance(body, dict):
        return []
    resolution = body.get("resolution") or {}
    unit = body.get("unit")
    denominator = body.get("denominator")
    curve = body.get("curve") or {}
    embedded = _embedded_curve(curve)
    rows: list[dict[str, Any]] = []
    for item in body.get("data", []) or []:
        if not isinstance(item, dict):
            continue
        rows.append(
            {
                "curve_name": curve.get("name") or curve_name,
                "datetime": item.get("d"),
                "value": item.get("v"),
                "scenarios": item.get("s"),
                "unit": unit,
                "denominator": denominator,
                "frequency": resolution.get("frequency"),
                "timezone": resolution.get("timezone"),
                "curve": embedded,
            }
        )
    return rows


def _explode_instance_response(curve_name: str, body: dict[str, Any]) -> list[dict[str, Any]]:
    """Flatten one instance object (full-load form) into per-delivery rows."""
    if not isinstance(body, dict):
        return []
    resolution = body.get("resolution") or {}
    unit = body.get("unit")
    denominator = body.get("denominator")
    curve = body.get("curve") or {}
    embedded = _embedded_curve(curve)
    instance = body.get("instance") or {}
    issued = instance.get("issued")
    tag = instance.get("tag") or ""
    rows: list[dict[str, Any]] = []
    for item in body.get("data", []) or []:
        if not isinstance(item, dict):
            continue
        rows.append(
            {
                "curve_name": curve.get("name") or curve_name,
                "issued": issued,
                "tag": tag,
                "datetime": item.get("d"),
                "value": item.get("v"),
                "unit": unit,
                "denominator": denominator,
                "frequency": resolution.get("frequency"),
                "timezone": resolution.get("timezone"),
                "curve": embedded,
            }
        )
    return rows


def _explode_periods_response(curve_name: str, body: dict[str, Any]) -> list[dict[str, Any]]:
    """Flatten ``/periods/{curve}/`` response into one row per period."""
    if not isinstance(body, dict):
        return []
    resolution = body.get("resolution") or {}
    unit = body.get("unit")
    denominator = body.get("denominator")
    curve = body.get("curve") or {}
    embedded = _embedded_curve(curve)
    rows: list[dict[str, Any]] = []
    for item in body.get("data", []) or []:
        if not isinstance(item, dict):
            continue
        rows.append(
            {
                "curve_name": curve.get("name") or curve_name,
                "begin": item.get("begin"),
                "end": item.get("end"),
                "value": item.get("v"),
                "capacity": item.get("capacity"),
                "unit": unit,
                "denominator": denominator,
                "timezone": resolution.get("timezone"),
                "curve": embedded,
            }
        )
    return rows


def _explode_ohlc_response(curve_name: str, body: dict[str, Any]) -> list[dict[str, Any]]:
    """Flatten ``/ohlc/{curve}/`` response.

    Hoists every ``product.*`` field to top-level so the framework
    can use them in the composite primary key.
    """
    if not isinstance(body, dict):
        return []
    unit = body.get("unit")
    denominator = body.get("denominator")
    curve = body.get("curve") or {}
    embedded = _embedded_curve(curve)
    rows: list[dict[str, Any]] = []
    for item in body.get("data", []) or []:
        if not isinstance(item, dict):
            continue
        product = item.get("product") or {}
        rows.append(
            {
                "curve_name": curve.get("name") or curve_name,
                "traded_at": product.get("traded_at"),
                "period": product.get("period") or "",
                "front": product.get("front"),
                "delivery": product.get("delivery"),
                "open": item.get("open"),
                "high": item.get("high"),
                "low": item.get("low"),
                "close": item.get("close"),
                "settlement": item.get("settlement"),
                "volume": item.get("volume"),
                "open_interest": item.get("open_interest"),
                "unit": unit,
                "denominator": denominator,
                "curve": embedded,
            }
        )
    return rows


def _explode_srmc_response(
    curve_name: str,
    body: dict[str, Any],
    table_options: dict[str, str],
) -> list[dict[str, Any]]:
    """Flatten the SRMC timeseries response.

    Stamps the contract period / front / delivery from the response
    ``contract`` block (falling back to ``table_options`` if absent)
    so each row carries its full composite PK.
    """
    if not isinstance(body, dict):
        return []
    resolution = body.get("resolution") or {}
    unit = body.get("unit")
    denominator = body.get("denominator")
    curve = body.get("curve") or {}
    embedded = _embedded_curve(curve)
    contract = body.get("contract") or {}
    options = body.get("options") or {}

    period = contract.get("period") or table_options.get("period") or ""
    front = contract.get("front")
    if front is None and "front" in table_options:
        try:
            front = int(table_options["front"])
        except (TypeError, ValueError):
            front = None
    delivery = contract.get("delivery") or table_options.get("delivery")

    rows: list[dict[str, Any]] = []
    for item in body.get("data", []) or []:
        if not isinstance(item, dict):
            continue
        rows.append(
            {
                "curve_name": curve.get("name") or curve_name,
                "date": item.get("d"),
                "value": item.get("v"),
                "period": period,
                "front": front,
                "delivery": delivery,
                "efficiency": options.get("efficiency"),
                "carbon_emissions": options.get("carbon_emissions"),
                "unit": unit,
                "denominator": denominator,
                "frequency": resolution.get("frequency"),
                "timezone": resolution.get("timezone"),
                "options": _slim_srmc_options(options),
                "curve": embedded,
            }
        )
    return rows


def _slim_srmc_options(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    return {
        "efficiency": value.get("efficiency"),
        "carbon_emissions": value.get("carbon_emissions"),
        "api2_tonne_to_mwh": value.get("api2_tonne_to_mwh"),
        "gas_therm_to_mwh": value.get("gas_therm_to_mwh"),
        "carbon_tax_area": value.get("carbon_tax_area"),
    }


# =========================================================================== #
# Date helpers
# =========================================================================== #


def _split_date_range_into_partitions(start: str, end: str, days_per_window: int) -> list[dict]:
    """Split ``[start, end)`` into fixed-size windows.

    Each partition dict has ``begin``/``end`` ISO date strings.
    Returns an empty list if the inputs cannot be parsed or already
    cover an empty range.  EQ uses inclusive ``begin`` / exclusive
    ``end`` for every range endpoint, so window boundaries are
    exact (no overlap, no gap).
    """
    start_dt = parse_iso_date_or_datetime(start)
    end_dt = parse_iso_date_or_datetime(end)
    if not start_dt or not end_dt or start_dt >= end_dt:
        return []

    partitions: list[dict] = []
    cursor = start_dt
    step = timedelta(days=days_per_window)
    while cursor < end_dt:
        window_end = min(cursor + step, end_dt)
        partitions.append({"begin": to_iso_date(cursor), "end": to_iso_date(window_end)})
        cursor = window_end
    return partitions


def _subtract_days_iso_date(value: str, days: int) -> str:
    """Subtract ``days`` from an ISO date / datetime string.

    Returns the original value if it cannot be parsed.  Date-only
    inputs return date-only outputs; datetimes return datetimes.
    """
    parsed = parse_iso_date_or_datetime(value)
    if not parsed:
        return value
    adjusted = parsed - timedelta(days=days)
    if len(value) == 10:
        return to_iso_date(adjusted)
    return to_iso_datetime(adjusted)
