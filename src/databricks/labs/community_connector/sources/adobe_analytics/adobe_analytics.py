"""Adobe Analytics 2.0 connector (read-only, OAuth Server-to-Server).

Connection options
------------------
- ``client_id`` / ``client_secret`` (required): an OAuth Server-to-Server credential
  from Adobe Developer Console with the Adobe Analytics API added. Its technical
  account must belong to an Analytics product profile; data is limited to what
  that profile allows.
- ``global_company_id`` (optional): skip auto-discovery. When omitted, the
  connector calls ``/discovery/me`` once and uses the only company found. It
  stops with an error listing the choices when there are several, and explains
  how to grant access when there are none.
- ``scopes`` (optional): comma-separated OAuth scopes. Default:
  ``openid,AdobeID,read_organizations,additional_info.projectedProductContext``.

Tables
------
Seven built-in metadata tables, all ``snapshot`` (re-read in full each run):

====================== ========================= ===============================
Table                  Primary key               Table options
====================== ========================= ===============================
report_suites          rsid                      none
virtual_report_suites  id                        none
dimensions             rsid, id                  ``rsid`` (optional, one id or a
metrics                rsid, id                  comma-separated list; default:
                                                 every report suite)
segments               id                        ``include_type`` (optional)
calculated_metrics     id                        ``include_type`` (optional)
date_ranges            id                        ``include_type`` (optional)
====================== ========================= ===============================

``include_type`` controls which components are listed. Default: ``all`` (every
component in the company, needs Analytics administrator rights), falling back
to ``shared`` with a warning when Adobe answers 403. Set any value Adobe accepts
(``all``, ``shared``, ``templates``, or a comma-separated mix), or ``none`` to
list only components the technical account owns.

Segment and calculated metric definition edits apply retroactively to all
dates. ``segments`` and ``calculated_metrics`` expose ``modified`` and the full
``definition`` (JSON text) so you can detect such edits and fully refresh the
affected report tables.

Report tables
-------------
Any source table name that is not one of the built-in names above defines a
report table. The name is yours to choose (for example ``daily_traffic``), and
its table options describe the report. Like ``google_analytics_aggregated``, you
can define as many report tables as you need. Each one covers a single report
suite.

==========================  ======== ========================================================
Option                      Required Meaning
==========================  ======== ========================================================
rsid                        yes      Report suite id; virtual report suite ids (``vrs_...``)
                                     also work.
metrics                     yes      Comma-separated list or JSON array of metric ids, for
                                     example ``metrics/visits,metrics/pageviews``. Saved
                                     calculated metric ids (``cm...``) are allowed.
dimension                   no       One non-time dimension id, for example
                                     ``variables/evar1``. Omit for one row per time bucket.
segment_ids                 no       Comma-separated segment ids applied as global filters
                                     (all must match).
granularity                 no       ``day`` (default), ``hour``, ``week`` or ``month``.
                                     Week and month work only without a dimension.
start_date                  no       First date to ingest, ``YYYY-MM-DD``. Default: 30 days
                                     before the first run.
lookback_days               no       Days before the last ingested date that every run
                                     re-reads, to pick up late-arriving data. Default 3.
nones_behavior              no       ``return`` (default) keeps the "Unspecified" row so
                                     totals match Analysis Workspace; ``exclude`` drops it.
count_repeat_instances      no       ``true`` (default, the Analysis Workspace default) or
                                     ``false``.
max_rows_per_bucket         no       Keep only the top N dimension values per bucket,
                                     ranked by the first metric. Capped data is incomplete.
limit                       no       Rows per API page, at most 50000 (default).
max_records_per_batch       no       Rows per micro-batch during a backfill. Default 100000.
delete_stale_rows           no       ``true`` (default) removes dimension rows that
                                     disappear when a date is re-read. See below.
max_tracked_keys            no       Upper bound on dimension values remembered for stale-row
                                     removal. Default 10000.
==========================  ======== ========================================================

Columns: ``rsid``, ``date``, ``hour`` (hourly only), ``dimension_value`` and
``dimension_item_id`` (with a dimension), ``segment_ids`` (sorted, comma-joined,
empty string when none), ``retrieved_at`` (when the row was read), then one
DOUBLE column per metric. Metric columns are named from the last part of the
metric id in lower snake_case: ``metrics/visits`` becomes ``visits``, and
``cm300000000_0123abcd`` stays as it is. A name that clashes with another
column gets a numeric suffix.

Primary key: ``rsid, date[, hour][, dimension_value], segment_ids``.
Ingestion type: ``cdc`` (``cdc_with_deletes`` when a dimension is set and
``delete_stale_rows`` is on). ``retrieved_at`` is the sequencing column, so a
re-read row always replaces the older copy.

All dates and hours are in the report suite's own time zone, taken from its
settings (UTC if it cannot be read). ``date`` is the first day of the bucket.

How a report table is read
~~~~~~~~~~~~~~~~~~~~~~~~~~
- The first run reads from ``start_date`` to today. Each later run re-reads
  from ``lookback_days`` before the last ingested date through today. Today
  is always in progress and is re-read on the next run.
- Without a dimension, the connector sends one trended request per window
  (hourly in 31-day chunks). With a dimension, it sends one ranked request per
  day, or per hour for hourly tables. Each request is paged until Adobe reports
  the last page.
- Request volume with a dimension is buckets times pages. A 30-day daily
  backfill is about 31 requests. A 30-day hourly backfill is about 720 buckets,
  so 720 or more requests. The connector sends at most 10 requests per 6 s,
  below Adobe's 12, so that takes 7 minutes or more. With ``delete_stale_rows``
  on, each run also re-requests the lookback window once more (see below).
- Week buckets follow the report suite's calendar (they may start on Sunday or
  Monday), and month buckets follow its calendar type. A partial bucket at the
  start of the window is dropped and logged at INFO level, so pick a
  ``start_date`` at the start of a week or month.

Stale rows and full refresh
~~~~~~~~~~~~~~~~~~~~~~~~~~~
- When a date is re-read, a dimension value can be missing from the new result,
  for example after reprocessing or when ``max_rows_per_bucket`` keeps a
  different top N. The delete flow (``read_table_deletes``) remembers the
  dimension values it saw for every bucket in the lookback window, in its own
  checkpoint. On the next run it re-reads those buckets and deletes values that
  are no longer returned. Limits: only buckets inside the lookback window are
  checked. Once more than ``max_tracked_keys`` values are remembered, the
  oldest remaining buckets are not tracked and a WARNING names how many; values
  that vanish from those buckets stay in the table until a full refresh. A value that
  changes in the few seconds between the upsert and delete requests can be
  missed.
- Changes older than the lookback window are not picked up automatically. That
  includes edits to segment or calculated metric definitions, processing or bot
  rule changes, and classification uploads. Run a full refresh of the affected
  table in the pipeline (Databricks pipelines: "Full refresh selection"). This
  clears the table and its checkpoint, so the next run re-reads from
  ``start_date``. A changed ``start_date`` also only takes effect after a full
  refresh.

Interpreting metrics
~~~~~~~~~~~~~~~~~~~~
Rows are not additive. Deduplicated metrics such as unique visitors cannot be
summed across dates or dimension values. Visit metrics are also slightly
over-counted when summed across dates, because a visit that crosses midnight
counts on both days. Ratio and percent metrics must not be summed either.

Errors and limits
~~~~~~~~~~~~~~~~~
HTTP 429 responses honor ``Retry-After``. 429, 5xx and timeouts are retried up
to 5 times with exponential backoff. A successful response whose body isn't
valid JSON (usually a large page cut off) is retried up to 3 times. A page that
keeps timing out or arriving cut off is retried with 10000 rows per page. With
``max_rows_per_bucket`` set, pages never ask for more rows than are kept.
A partial result (HTTP 206 with ``columnErrors``)
stops the table with Adobe's explanation and never writes null metrics.

All reads run sequentially on the driver: Adobe's rate limit is per user, so
parallel readers would only cause more throttling.

Deferred (not in this version): multiple dimensions (itemId breakdowns),
per-metric filters (``metricFilters`` of type segment or breakdown), week or
month granularity with a dimension, annotations, projects, users, audit logs,
real-time reports and Data Warehouse or Data Feed exports.
"""

import difflib
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Iterator
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.adobe_analytics.adobe_analytics_client import (
    DEFAULT_SCOPES,
    AdobeAnalyticsAPIError,
    AdobeAnalyticsClient,
    AdobeAnalyticsInvalidResponseError,
    AdobeAnalyticsTimeoutError,
    extract_list_page,
)
from databricks.labs.community_connector.sources.adobe_analytics.adobe_analytics_reports import (
    COL_DATE,
    COL_DIMENSION_ITEM_ID,
    COL_DIMENSION_VALUE,
    COL_HOUR,
    COL_RETRIEVED_AT,
    COL_RSID,
    COL_SEGMENT_IDS,
    DEFAULT_START_DAYS_AGO,
    FALLBACK_PAGE_LIMIT,
    TRENDED_CHUNK_DAYS,
    TRENDED_PAD_DAYS,
    Bucket,
    ReportConfig,
    bucket_from_key,
    build_report_request,
    build_report_schema,
    buckets_for_day,
    check_column_errors,
    day_range,
    parse_list_option,
    parse_report_config,
    parse_time_item_id,
    report_primary_keys,
    row_metric_values,
)
from databricks.labs.community_connector.sources.adobe_analytics.adobe_analytics_schemas import (
    CALCULATED_METRICS,
    CALCULATED_METRICS_EXPANSION,
    CALCULATED_METRICS_PATH,
    DATE_RANGES,
    DATE_RANGES_EXPANSION,
    DATE_RANGES_PATH,
    DIMENSIONS,
    DIMENSIONS_EXPANSION,
    DIMENSIONS_PATH,
    METADATA_TABLES,
    METRICS,
    METRICS_EXPANSION,
    METRICS_PATH,
    REPORT_SUITES,
    REPORTS_PATH,
    SEGMENTS,
    SEGMENTS_EXPANSION,
    SEGMENTS_PATH,
    SUITES_EXPANSION,
    SUITES_PATH,
    TABLE_PRIMARY_KEYS,
    TABLE_SCHEMAS,
    VIRTUAL_REPORT_SUITES,
    VRS_EXPANSION,
    VRS_PATH,
    map_calculated_metric,
    map_date_range,
    map_dimension,
    map_metric,
    map_report_suite,
    map_segment,
    map_virtual_report_suite,
)

logger = logging.getLogger(__name__)

# includeType values tried in order when the user does not set include_type.
_DEFAULT_INCLUDE_TYPES = ("all", "shared", None)


class AdobeAnalyticsLakeflowConnect(LakeflowConnect):
    """LakeflowConnect implementation for the Adobe Analytics 2.0 API."""

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        client_id = str(options.get("client_id") or "").strip()
        client_secret = str(options.get("client_secret") or "").strip()
        missing = [
            n for n, v in (("client_id", client_id), ("client_secret", client_secret)) if not v
        ]
        if missing:
            raise ValueError(
                f"Adobe Analytics connection is missing {', '.join(missing)}. Copy both from "
                "the OAuth Server-to-Server credential of your Adobe Developer Console project."
            )
        scopes_raw = str(options.get("scopes") or "").strip() or DEFAULT_SCOPES
        scopes = ",".join(s.strip() for s in scopes_raw.split(",") if s.strip())
        self._client = AdobeAnalyticsClient(
            client_id,
            client_secret,
            scopes=scopes,
            global_company_id=str(options.get("global_company_id") or "").strip() or None,
        )
        # "Today" and the run marker are frozen per instance so one trigger has a
        # fixed end and Trigger.AvailableNow terminates.
        self._init_utc = datetime.now(timezone.utc)
        self._run_id = self._init_utc.isoformat()
        self._timezones: dict[str, ZoneInfo | timezone] = {}
        self._validated: set = set()
        self._expansion_rejected: set[tuple[str, str]] = set()

    # ------------------------------------------------------------------
    # LakeflowConnect interface
    # ------------------------------------------------------------------

    def list_tables(self) -> list[str]:
        return list(METADATA_TABLES)

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        cfg = self._report_config(table_name, table_options)
        if cfg is None:
            return TABLE_SCHEMAS[table_name]
        self._validate_report_components(cfg)
        return build_report_schema(cfg)

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        cfg = self._report_config(table_name, table_options)
        if cfg is None:
            return {
                "primary_keys": list(TABLE_PRIMARY_KEYS[table_name]),
                "ingestion_type": "snapshot",
            }
        return {
            "primary_keys": report_primary_keys(cfg),
            "cursor_field": COL_RETRIEVED_AT,
            "ingestion_type": "cdc_with_deletes" if cfg.tracks_deletes else "cdc",
        }

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        cfg = self._report_config(table_name, table_options)
        if cfg is not None:
            return self._read_report(cfg, start_offset)
        readers = {
            REPORT_SUITES: self._read_report_suites,
            VIRTUAL_REPORT_SUITES: self._read_virtual_report_suites,
            DIMENSIONS: lambda opts: self._read_per_suite(DIMENSIONS, opts),
            METRICS: lambda opts: self._read_per_suite(METRICS, opts),
            SEGMENTS: lambda opts: self._read_components(SEGMENTS, opts),
            CALCULATED_METRICS: lambda opts: self._read_components(CALCULATED_METRICS, opts),
            DATE_RANGES: lambda opts: self._read_components(DATE_RANGES, opts),
        }
        return iter(readers[table_name](table_options)), {}

    def read_table_deletes(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Emit deletes for dimension values that vanished from re-read buckets.

        The offset stores ``{"keys": {bucket_key: [dimension values]}, "run": id}``
        for the lookback window. Each run re-reads the remembered buckets plus the
        current lookback window, deletes remembered values that are no longer
        returned, then remembers the current window.
        """
        cfg = self._report_config(table_name, table_options)
        if cfg is None or not cfg.tracks_deletes:
            raise ValueError(
                f"Table '{table_name}' does not produce deletes; only report tables with a "
                "dimension and delete_stale_rows=true do."
            )
        if start_offset and start_offset.get("run") == self._run_id:
            return iter([]), start_offset

        today, start_date = self._report_dates(cfg)
        prior: dict = (start_offset or {}).get("keys") or {}
        track_from = max(start_date, today - timedelta(days=cfg.lookback_days))
        track_buckets: list[Bucket] = []
        day = track_from
        while day <= today:
            track_buckets.extend(buckets_for_day(day, cfg.granularity))
            day += timedelta(days=1)

        to_read = {b.key: b for b in track_buckets}
        for key in prior:
            bucket = bucket_from_key(key, cfg.granularity)
            if start_date <= bucket.day <= today:
                to_read.setdefault(key, bucket)

        current: dict[str, set] = {}
        for key in sorted(to_read):
            records = self._ranked_bucket_records(cfg, to_read[key])
            current[key] = {
                r[COL_DIMENSION_VALUE] for r in records if r[COL_DIMENSION_VALUE] is not None
            }

        deletes = []
        for key in sorted(prior):
            if key not in current:
                continue
            bucket = to_read[key]
            for value in sorted(set(prior[key]) - current[key]):
                record = self._base_record(cfg, bucket.day, bucket.hour)
                record[COL_DIMENSION_VALUE] = value
                deletes.append(record)

        remembered: dict[str, list[str]] = {}
        total, skipped = 0, 0
        # Newest buckets first: they are re-read for the most runs.
        for bucket in sorted(track_buckets, key=lambda b: b.key, reverse=True):
            values = sorted(current.get(bucket.key, set()))
            if total + len(values) > cfg.max_tracked_keys:
                skipped += 1
                continue
            remembered[bucket.key] = values
            total += len(values)
        if skipped:
            logger.warning(
                "Adobe Analytics table '%s': max_tracked_keys=%d was reached, so %d of %d "
                "buckets in the lookback window are not tracked. Dimension values that "
                "disappear from those buckets will NOT be deleted on the next run and may "
                "remain in the table. Raise max_tracked_keys, lower lookback_days, or run a "
                "full refresh to clear them.",
                cfg.table_name,
                cfg.max_tracked_keys,
                skipped,
                len(track_buckets),
            )
        end_offset = {"keys": dict(sorted(remembered.items())), "run": self._run_id}
        return iter(deletes), end_offset

    # ------------------------------------------------------------------
    # Table routing and validation
    # ------------------------------------------------------------------

    @staticmethod
    def _report_config(table_name: str, table_options: dict[str, str]) -> ReportConfig | None:
        """Return None for built-in tables, else the validated report configuration."""
        table_options = table_options or {}
        if table_name in METADATA_TABLES:
            if table_options.get("metrics"):
                raise ValueError(
                    f"'{table_name}' is a built-in table name and cannot be used for a report "
                    "table. Choose another source table name for your report."
                )
            return None
        if not table_options.get("rsid") and not table_options.get("metrics"):
            raise ValueError(
                f"Table '{table_name}' is not a built-in table ({', '.join(METADATA_TABLES)}). "
                "To define a report table under this name, set the table options 'rsid' and "
                "'metrics' (optionally 'dimension', 'granularity', 'start_date', ...)."
            )
        return parse_report_config(table_name, table_options)

    def _validate_report_components(self, cfg: ReportConfig) -> None:
        """Reject unknown metric or dimension ids early, when Adobe's catalog is readable."""
        key = (cfg.rsid, cfg.metrics, cfg.dimension)
        if key in self._validated:
            return
        problems = []
        standard_metrics = [m for m in cfg.metrics if m.startswith("metrics/")]
        if standard_metrics:
            known = self._component_ids(METRICS_PATH, cfg.rsid)
            if known:
                problems.extend(
                    self._describe_unknown("metric", m, known)
                    for m in standard_metrics
                    if m not in known
                )
        if cfg.dimension:
            known = self._component_ids(DIMENSIONS_PATH, cfg.rsid)
            if known and cfg.dimension not in known:
                problems.append(self._describe_unknown("dimension", cfg.dimension, known))
        if problems:
            raise ValueError(
                f"Report table '{cfg.table_name}' uses ids that report suite '{cfg.rsid}' does "
                f"not offer: {'; '.join(problems)}. The 'metrics' and 'dimensions' tables list "
                "the valid ids per report suite."
            )
        self._validated.add(key)

    @staticmethod
    def _describe_unknown(kind: str, component_id: str, known: set[str]) -> str:
        close = difflib.get_close_matches(component_id, sorted(known), n=3)
        hint = f" (did you mean {', '.join(close)}?)" if close else ""
        return f"unknown {kind} {component_id!r}{hint}"

    def _component_ids(self, path: str, rsid: str) -> set[str] | None:
        try:
            items, _ = extract_list_page(self._client.get(path, {"rsid": rsid}))
        except (AdobeAnalyticsAPIError, ValueError) as exc:
            logger.warning("Adobe Analytics: skipping id validation for %s (%s)", rsid, exc)
            return None
        return {i["id"] for i in items if i.get("id")}

    # ------------------------------------------------------------------
    # Metadata tables
    # ------------------------------------------------------------------

    def _list_with_expansion(
        self, path: str, params: dict, expansion: str | None, paged: bool = True
    ) -> list[dict]:
        """Fetch a list; if Adobe rejects the expansion list (400), retry without it."""

        def fetch(query: dict) -> list[dict]:
            if paged:
                return self._client.get_paged(path, query)
            return extract_list_page(self._client.get(path, query))[0]

        if expansion and (path, expansion) not in self._expansion_rejected:
            try:
                return fetch({**params, "expansion": expansion})
            except AdobeAnalyticsAPIError as exc:
                if exc.status_code != 400:
                    raise
                logger.warning(
                    "Adobe Analytics rejected expansion=%s on %s (%s); retrying without it. "
                    "Some columns will be empty.",
                    expansion,
                    path,
                    exc,
                )
                self._expansion_rejected.add((path, expansion))
        return fetch(params)

    def _read_report_suites(self, _table_options: dict) -> list[dict]:
        items = self._list_with_expansion(SUITES_PATH, {}, SUITES_EXPANSION)
        return _dedupe([map_report_suite(i) for i in items], ("rsid",))

    def _read_virtual_report_suites(self, _table_options: dict) -> list[dict]:
        items = self._list_with_expansion(VRS_PATH, {}, VRS_EXPANSION)
        return _dedupe([map_virtual_report_suite(i) for i in items], ("id",))

    def _read_per_suite(self, table_name: str, table_options: dict) -> list[dict]:
        explicit = parse_list_option(table_options.get("rsid"))
        if explicit:
            rsids = explicit
        else:
            suites = self._client.get_paged(SUITES_PATH, {})
            rsids = list(
                dict.fromkeys(
                    s.get("rsid") or s.get("id") for s in suites if s.get("rsid") or s.get("id")
                )
            )
        path, expansion, mapper = {
            DIMENSIONS: (DIMENSIONS_PATH, DIMENSIONS_EXPANSION, map_dimension),
            METRICS: (METRICS_PATH, METRICS_EXPANSION, map_metric),
        }[table_name]

        records = []
        for rsid in rsids:
            try:
                items = self._list_with_expansion(path, {"rsid": rsid}, expansion, paged=False)
            except AdobeAnalyticsAPIError as exc:
                if exc.status_code in (401, 403) and not explicit:
                    logger.warning(
                        "Adobe Analytics: skipping report suite %s for %s; the credential has "
                        "no access to it (%s).",
                        rsid,
                        table_name,
                        exc,
                    )
                    continue
                raise
            records.extend(mapper(item, rsid) for item in items)
        return _dedupe(records, ("rsid", "id"))

    def _read_components(self, table_name: str, table_options: dict) -> list[dict]:
        path, expansion, mapper, extra = {
            SEGMENTS: (
                SEGMENTS_PATH,
                SEGMENTS_EXPANSION,
                map_segment,
                {"sortProperty": "id", "sortDirection": "ASC"},
            ),
            CALCULATED_METRICS: (
                CALCULATED_METRICS_PATH,
                CALCULATED_METRICS_EXPANSION,
                map_calculated_metric,
                {"sortProperty": "id", "sortDirection": "ASC"},
            ),
            DATE_RANGES: (DATE_RANGES_PATH, DATE_RANGES_EXPANSION, map_date_range, {}),
        }[table_name]

        user_value = str(table_options.get("include_type") or "").strip()
        if user_value:
            candidates = (None,) if user_value.lower() == "none" else (user_value,)
        else:
            candidates = _DEFAULT_INCLUDE_TYPES

        items: list[dict] = []
        for position, include_type in enumerate(candidates):
            params = dict(extra)
            if include_type:
                params["includeType"] = include_type
            try:
                items = self._list_with_expansion(path, params, expansion)
            except AdobeAnalyticsAPIError as exc:
                if exc.status_code == 403 and position + 1 < len(candidates):
                    fallback = candidates[position + 1] or "(not set: owned components only)"
                    logger.warning(
                        "Adobe Analytics refused includeType=%s for %s (HTTP 403; 'all' needs "
                        "Analytics administrator rights). Falling back to includeType=%s.",
                        include_type,
                        table_name,
                        fallback,
                    )
                    continue
                raise
            logger.info(
                "Adobe Analytics: read %d %s with includeType=%s",
                len(items),
                table_name,
                include_type or "(not set)",
            )
            break
        return _dedupe([mapper(i) for i in items], ("id",))

    # ------------------------------------------------------------------
    # Report tables
    # ------------------------------------------------------------------

    def _report_timezone(self, rsid: str):
        """The report suite's time zone; virtual suites use their own or their parent's."""
        if rsid in self._timezones:
            return self._timezones[rsid]
        tz_name = None
        try:
            for suite in self._list_with_expansion(
                SUITES_PATH, {"rsids": rsid}, "timezoneZoneinfo"
            ):
                if (suite.get("rsid") or suite.get("id")) == rsid:
                    tz_name = suite.get("timezoneZoneinfo")
                    break
            else:
                for vrs in self._list_with_expansion(VRS_PATH, {}, "timezoneZoneinfo,parentRsid"):
                    if vrs.get("id") == rsid:
                        tz_name = vrs.get("timezoneZoneinfo") or vrs.get("timezoneZoneInfo")
                        parent = vrs.get("parentRsid")
                        if not tz_name and parent and parent != rsid:
                            parent_tz = self._report_timezone(parent)
                            self._timezones[rsid] = parent_tz
                            return parent_tz
                        break
        except (AdobeAnalyticsAPIError, ValueError) as exc:
            logger.warning("Adobe Analytics: could not read the time zone of %s (%s)", rsid, exc)
        tz = timezone.utc
        if tz_name:
            try:
                tz = ZoneInfo(tz_name)
            except (ZoneInfoNotFoundError, ValueError):
                logger.warning(
                    "Adobe Analytics: unknown time zone %r for %s; using UTC", tz_name, rsid
                )
        else:
            logger.warning(
                "Adobe Analytics: no time zone found for %s; "
                "using UTC to decide which day is today",
                rsid,
            )
        self._timezones[rsid] = tz
        return tz

    def _report_dates(self, cfg: ReportConfig) -> tuple[date, date]:
        today = self._init_utc.astimezone(self._report_timezone(cfg.rsid)).date()
        start_date = cfg.start_date or today - timedelta(days=DEFAULT_START_DAYS_AGO)
        return today, start_date

    def _read_report(self, cfg: ReportConfig, start_offset: dict) -> tuple[Iterator[dict], dict]:
        """Offsets: ``{"cursor": last date read, "resume": true}`` mid-backfill, and
        ``{"cursor": ..., "run": run id}`` once the window reached today. The run
        marker makes a same-day re-run produce a new offset while letting this
        instance stop on its next call."""
        if start_offset and start_offset.get("run") == self._run_id:
            return iter([]), start_offset

        today, start_date = self._report_dates(cfg)
        if start_date > today:
            logger.warning(
                "Adobe Analytics table '%s': start_date %s is in the future; nothing to read yet.",
                cfg.table_name,
                start_date,
            )
            return iter([]), start_offset or {}

        prior_cursor = None
        if start_offset and start_offset.get("cursor"):
            prior_cursor = date.fromisoformat(start_offset["cursor"])
        window_from = self._window_start(cfg, start_offset, prior_cursor, start_date, today)

        if cfg.dimension:
            records, cursor, done = self._read_ranked_window(cfg, window_from, today)
        else:
            records, cursor, done = self._read_trended_window(
                cfg, window_from, today, start_date, prior_cursor
            )

        end_offset = {"cursor": cursor.isoformat()}
        if done:
            end_offset["run"] = self._run_id
        else:
            end_offset["resume"] = True
        return iter(records), end_offset

    @staticmethod
    def _window_start(
        cfg: ReportConfig,
        start_offset: dict,
        prior_cursor: date | None,
        start_date: date,
        today: date,
    ) -> date:
        if prior_cursor is None:
            return start_date
        if start_offset.get("resume"):
            return max(start_date, prior_cursor + timedelta(days=1))
        back = prior_cursor - timedelta(days=cfg.lookback_days)
        if cfg.granularity == "week":
            # Whole weeks keep the window on the suite's own week boundary.
            back = prior_cursor - timedelta(days=7 * -(-cfg.lookback_days // 7))
        return max(start_date, min(back, today))

    def _read_ranked_window(
        self, cfg: ReportConfig, window_from: date, today: date
    ) -> tuple[list[dict], date, bool]:
        records: list[dict] = []
        day = window_from
        last_done = window_from
        while day <= today:
            for bucket in buckets_for_day(day, cfg.granularity):
                records.extend(self._ranked_bucket_records(cfg, bucket))
            last_done = day
            day += timedelta(days=1)
            # Stop only between whole days so every bucket is complete.
            if len(records) >= cfg.max_records_per_batch:
                break
        return records, last_done, day > today

    def _ranked_bucket_records(self, cfg: ReportConfig, bucket: Bucket) -> list[dict]:
        rows = self._fetch_report_rows(
            cfg, bucket.date_range, trended=False, row_cap=cfg.max_rows_per_bucket
        )
        records, seen = [], set()
        for row, values in rows:
            value = row.get("value")
            if value in seen:
                logger.warning(
                    "Adobe Analytics table '%s': duplicate dimension value %r in bucket %s; "
                    "keeping the first row.",
                    cfg.table_name,
                    value,
                    bucket.key,
                )
                continue
            seen.add(value)
            record = self._base_record(cfg, bucket.day, bucket.hour)
            record[COL_DIMENSION_VALUE] = value
            record[COL_DIMENSION_ITEM_ID] = (
                None if row.get("itemId") is None else str(row.get("itemId"))
            )
            record.update(zip(cfg.metric_columns, values))
            records.append(record)
        return records

    def _read_trended_window(
        self,
        cfg: ReportConfig,
        window_from: date,
        today: date,
        start_date: date,
        prior_cursor: date | None,
    ) -> tuple[list[dict], date, bool]:
        granularity = cfg.granularity
        if granularity in TRENDED_CHUNK_DAYS:
            request_from = window_from
            chunk_end = min(
                today, window_from + timedelta(days=TRENDED_CHUNK_DAYS[granularity] - 1)
            )
        else:
            request_from = max(
                start_date, window_from - timedelta(days=TRENDED_PAD_DAYS[granularity])
            )
            chunk_end = today
        oldest_plausible = request_from - timedelta(days=TRENDED_PAD_DAYS.get(granularity, 0))

        rows = self._fetch_report_rows(cfg, day_range(request_from, chunk_end), trended=True)
        records, seen = [], set()
        latest_bucket = None
        out_of_range: list[date] = []
        for row, values in rows:
            bucket_day, hour = parse_time_item_id(row.get("itemId"), granularity)
            if bucket_day > chunk_end or bucket_day < oldest_plausible:
                # Adobe can return days outside the dateRange (seen live with
                # metrics/bouncerate). Such a row covers only part of its day, so writing
                # it would overwrite a complete row from an earlier run under the same key.
                out_of_range.append(bucket_day)
                continue
            if bucket_day < request_from:
                # Expected on most week/month runs: the padded request start falls mid-bucket.
                logger.info(
                    "Adobe Analytics table '%s': dropped partial %s bucket starting %s because "
                    "the request began on %s, mid-bucket; its totals would be incomplete.",
                    cfg.table_name,
                    granularity,
                    bucket_day,
                    request_from,
                )
                continue
            if (bucket_day, hour) in seen:
                # Can happen on the repeated local hour when daylight saving time ends.
                logger.warning(
                    "Adobe Analytics table '%s': duplicate time bucket %s hour %s; "
                    "keeping the first row.",
                    cfg.table_name,
                    bucket_day,
                    hour,
                )
                continue
            seen.add((bucket_day, hour))
            record = self._base_record(cfg, bucket_day, hour)
            record.update(zip(cfg.metric_columns, values))
            records.append(record)
            latest_bucket = bucket_day if latest_bucket is None else max(latest_bucket, bucket_day)

        if out_of_range:
            logger.warning(
                "Adobe Analytics table '%s': skipped %d row(s) dated %s because they fall "
                "outside the requested dates %s to %s. They were not written, so they "
                "cannot replace complete rows from an earlier run.",
                cfg.table_name,
                len(out_of_range),
                ", ".join(sorted({d.isoformat() for d in out_of_range})),
                request_from,
                chunk_end,
            )

        if granularity in TRENDED_CHUNK_DAYS:
            return records, chunk_end, chunk_end >= today
        # Week/month: the cursor is the start of the newest (in-progress) bucket.
        candidates = [d for d in (latest_bucket, prior_cursor) if d is not None]
        return records, max(candidates) if candidates else window_from, True

    def _fetch_report_rows(
        self, cfg: ReportConfig, date_range: str, trended: bool, row_cap: int | None = None
    ) -> list[tuple[dict, list]]:
        # Never ask for more rows than will be kept: a 50,000-row page can be ~5 MB,
        # and pages that large have been seen arriving cut off.
        limit = cfg.limit if row_cap is None else min(cfg.limit, row_cap)
        while True:
            try:
                return self._fetch_report_pages(cfg, date_range, trended, limit, row_cap)
            except (AdobeAnalyticsTimeoutError, AdobeAnalyticsInvalidResponseError) as exc:
                if limit <= FALLBACK_PAGE_LIMIT:
                    raise
                problem = (
                    "keep timing out"
                    if isinstance(exc, AdobeAnalyticsTimeoutError)
                    else "keep arriving cut off"
                )
                logger.warning(
                    "Adobe Analytics table '%s': pages of %d rows %s; retrying %s "
                    "with %d rows per page.",
                    cfg.table_name,
                    limit,
                    problem,
                    date_range,
                    FALLBACK_PAGE_LIMIT,
                )
                limit = FALLBACK_PAGE_LIMIT

    def _fetch_report_pages(
        self, cfg: ReportConfig, date_range: str, trended: bool, limit: int, row_cap: int | None
    ) -> list[tuple[dict, list]]:
        rows: list[tuple[dict, list]] = []
        page = 0
        while True:
            body = self._client.post(
                REPORTS_PATH, build_report_request(cfg, date_range, page, limit, trended)
            )
            if not isinstance(body, dict):
                raise ValueError(
                    f"Unexpected /reports response for table '{cfg.table_name}': {body!r:.300}"
                )
            check_column_errors(body, cfg)
            column_ids = (body.get("columns") or {}).get("columnIds")
            batch = body.get("rows") or []
            rows.extend(
                (row, row_metric_values(row, column_ids, len(cfg.metrics))) for row in batch
            )
            if row_cap is not None and len(rows) >= row_cap:
                return rows[:row_cap]
            last_page = body.get("lastPage")
            if last_page is True or not batch:
                return rows
            if last_page is None:
                total_pages = body.get("totalPages")
                if total_pages is None and len(batch) < limit:
                    return rows
                if total_pages is not None and page + 1 >= int(total_pages):
                    return rows
            page += 1

    def _base_record(self, cfg: ReportConfig, day: date, hour: int | None) -> dict:
        record = {COL_RSID: cfg.rsid, COL_DATE: day.isoformat()}
        if cfg.granularity == "hour":
            record[COL_HOUR] = hour
        if cfg.dimension:
            record[COL_DIMENSION_VALUE] = None
            record[COL_DIMENSION_ITEM_ID] = None
        record[COL_SEGMENT_IDS] = cfg.segment_key
        record[COL_RETRIEVED_AT] = self._run_id
        for name in cfg.metric_columns:
            record[name] = None
        return record


def _dedupe(records: list[dict], key_fields: tuple[str, ...]) -> list[dict]:
    """Drop records with a missing or repeated primary key (pages can overlap)."""
    seen, out = set(), []
    for record in records:
        key = tuple(record.get(f) for f in key_fields)
        if any(k is None for k in key) or key in seen:
            continue
        seen.add(key)
        out.append(record)
    return out
