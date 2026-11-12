"""Report-table configuration, schema, time buckets and response parsing for
Adobe Analytics ``POST /reports``."""

import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta

from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

GRANULARITIES = ("day", "hour", "week", "month")
TIME_DIMENSIONS = {
    "hour": "variables/daterangehour",
    "day": "variables/daterangeday",
    "week": "variables/daterangeweek",
    "month": "variables/daterangemonth",
}

DEFAULT_START_DAYS_AGO = 30
DEFAULT_LOOKBACK_DAYS = 3
MAX_PAGE_LIMIT = 50_000
# Page size to fall back to when a large page keeps timing out.
FALLBACK_PAGE_LIMIT = 10_000
DEFAULT_MAX_RECORDS_PER_BATCH = 100_000
DEFAULT_MAX_TRACKED_KEYS = 10_000

# Days requested per trended (no-dimension) call. Hourly is capped at 31 days so
# one page holds at most 744 rows.
TRENDED_CHUNK_DAYS = {"hour": 31, "day": 366}
# Padding before the window start for week/month trended reads. The bucket that
# straddles the padded start is partial and is dropped, so the buckets that
# overlap the real window are always complete. 42 days covers 4-5-4 retail months.
TRENDED_PAD_DAYS = {"week": 7, "month": 42}

COL_RSID = "rsid"
COL_DATE = "date"
COL_HOUR = "hour"
COL_DIMENSION_VALUE = "dimension_value"
COL_DIMENSION_ITEM_ID = "dimension_item_id"
COL_SEGMENT_IDS = "segment_ids"
COL_RETRIEVED_AT = "retrieved_at"
RESERVED_COLUMNS = {
    COL_RSID,
    COL_DATE,
    COL_HOUR,
    COL_DIMENSION_VALUE,
    COL_DIMENSION_ITEM_ID,
    COL_SEGMENT_IDS,
    COL_RETRIEVED_AT,
}

REPORT_OPTION_NAMES = (
    "rsid",
    "metrics",
    "dimension",
    "segment_ids",
    "start_date",
    "lookback_days",
    "granularity",
    "limit",
    "max_rows_per_bucket",
    "nones_behavior",
    "count_repeat_instances",
    "max_records_per_batch",
    "delete_stale_rows",
    "max_tracked_keys",
)

_NONES_BEHAVIOR = {
    "return": "return-nones",
    "return-nones": "return-nones",
    "exclude": "exclude-nones",
    "exclude-nones": "exclude-nones",
}
_TRUE = {"true", "1", "yes", "y"}
_FALSE = {"false", "0", "no", "n"}


@dataclass(frozen=True)
class ReportConfig:  # pylint: disable=too-many-instance-attributes
    """Validated report-table options."""

    table_name: str
    rsid: str
    metrics: tuple[str, ...]
    metric_columns: tuple[str, ...]
    dimension: str | None
    segment_ids: tuple[str, ...]
    start_date: date | None
    lookback_days: int
    granularity: str
    limit: int
    max_rows_per_bucket: int | None
    nones_behavior: str
    count_repeat_instances: bool
    max_records_per_batch: int
    delete_stale_rows: bool
    max_tracked_keys: int

    @property
    def segment_key(self) -> str:
        return ",".join(sorted(self.segment_ids))

    @property
    def tracks_deletes(self) -> bool:
        # Without a dimension every bucket always returns exactly one row, so no
        # row can disappear on re-read.
        return bool(self.dimension) and self.delete_stale_rows


# ---------------------------------------------------------------------------
# Option parsing
# ---------------------------------------------------------------------------


def parse_list_option(value) -> list[str]:
    """Accept a comma-separated string or a JSON array string."""
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        items = value
    else:
        text = str(value).strip()
        if not text:
            return []
        if text.startswith("["):
            try:
                items = json.loads(text)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Could not read {text!r} as a JSON list: {exc}") from exc
            if not isinstance(items, list):
                raise ValueError(f"Expected a JSON list, got {text!r}")
        else:
            items = text.split(",")
    return [str(x).strip() for x in items if str(x).strip()]


def _int_option(
    opts: dict, name: str, default: int | None, minimum: int, maximum: int | None = None
):
    raw = opts.get(name)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        value = int(str(raw).strip())
    except ValueError as exc:
        raise ValueError(f"Table option '{name}' must be a whole number, got {raw!r}.") from exc
    if value < minimum or (maximum is not None and value > maximum):
        upper = f" and at most {maximum}" if maximum is not None else ""
        raise ValueError(f"Table option '{name}' must be at least {minimum}{upper}, got {value}.")
    return value


def _bool_option(opts: dict, name: str, default: bool) -> bool:
    raw = opts.get(name)
    if raw is None or str(raw).strip() == "":
        return default
    text = str(raw).strip().lower()
    if text in _TRUE:
        return True
    if text in _FALSE:
        return False
    raise ValueError(f"Table option '{name}' must be true or false, got {raw!r}.")


def parse_report_config(table_name: str, opts: dict) -> ReportConfig:
    """Validate report-table options and raise plain-English errors on mistakes."""
    rsid = str(opts.get("rsid") or "").strip()
    if not rsid:
        raise ValueError(
            f"Report table '{table_name}' needs the table option 'rsid' (the report suite "
            "or virtual report suite id, for example 'examplersid'). The 'report_suites' "
            "and 'virtual_report_suites' tables list the ids available to this credential."
        )

    metrics = parse_list_option(opts.get("metrics"))
    if not metrics:
        raise ValueError(
            f"Report table '{table_name}' needs the table option 'metrics': a comma-separated "
            "list (or JSON array) of metric ids, for example 'metrics/visits,metrics/pageviews'. "
            "The 'metrics' and 'calculated_metrics' tables list valid ids."
        )
    duplicates = sorted({m for m in metrics if metrics.count(m) > 1})
    if duplicates:
        raise ValueError(f"Table option 'metrics' lists these ids more than once: {duplicates}.")
    for metric in metrics:
        if "/" not in metric and not metric.startswith("cm"):
            raise ValueError(
                f"Metric id {metric!r} is not in the full form the Reports API expects. "
                f"Use 'metrics/{metric}' for a standard metric, or the 'cm...' id of a saved "
                "calculated metric."
            )

    dimension = str(opts.get("dimension") or "").strip() or None
    if dimension:
        if dimension.startswith("variables/daterange"):
            raise ValueError(
                f"Dimension {dimension!r} is a time dimension. Leave 'dimension' empty and set "
                "'granularity' (day, hour, week or month) instead; every report table is "
                "already split by date."
            )
        if "/" not in dimension:
            raise ValueError(
                f"Dimension id {dimension!r} is not in the full form the Reports API expects. "
                f"Use 'variables/{dimension}'. The 'dimensions' table lists valid ids."
            )

    granularity = str(opts.get("granularity") or "day").strip().lower()
    if granularity not in GRANULARITIES:
        raise ValueError(
            f"Table option 'granularity' must be one of {list(GRANULARITIES)}, got {granularity!r}."
        )
    if dimension and granularity in ("week", "month"):
        raise ValueError(
            f"granularity '{granularity}' together with a dimension is not supported yet. "
            "Week and month boundaries depend on each report suite's calendar settings, "
            "which the connector cannot compute on its own. Use 'day' or 'hour' with a "
            "dimension, or remove the dimension to get one row per week or month."
        )

    start_raw = str(opts.get("start_date") or "").strip()
    start_date = None
    if start_raw:
        try:
            start_date = date.fromisoformat(start_raw)
        except ValueError as exc:
            raise ValueError(
                f"Table option 'start_date' must be a date in YYYY-MM-DD form, got {start_raw!r}."
            ) from exc

    nones_raw = str(opts.get("nones_behavior") or "return").strip().lower()
    if nones_raw not in _NONES_BEHAVIOR:
        raise ValueError(
            f"Table option 'nones_behavior' must be 'return' or 'exclude', got {nones_raw!r}."
        )

    return ReportConfig(
        table_name=table_name,
        rsid=rsid,
        metrics=tuple(metrics),
        metric_columns=tuple(metric_column_names(metrics)),
        dimension=dimension,
        segment_ids=tuple(parse_list_option(opts.get("segment_ids"))),
        start_date=start_date,
        lookback_days=_int_option(opts, "lookback_days", DEFAULT_LOOKBACK_DAYS, 0),
        granularity=granularity,
        limit=_int_option(opts, "limit", MAX_PAGE_LIMIT, 1, MAX_PAGE_LIMIT),
        max_rows_per_bucket=_int_option(opts, "max_rows_per_bucket", None, 1),
        nones_behavior=_NONES_BEHAVIOR[nones_raw],
        count_repeat_instances=_bool_option(opts, "count_repeat_instances", True),
        max_records_per_batch=_int_option(
            opts, "max_records_per_batch", DEFAULT_MAX_RECORDS_PER_BATCH, 1
        ),
        delete_stale_rows=_bool_option(opts, "delete_stale_rows", True),
        max_tracked_keys=_int_option(opts, "max_tracked_keys", DEFAULT_MAX_TRACKED_KEYS, 0),
    )


# ---------------------------------------------------------------------------
# Schema and keys
# ---------------------------------------------------------------------------


def sanitize_column_name(metric_id: str) -> str:
    """``metrics/visits`` -> ``visits``; any id -> lower snake_case."""
    name = metric_id.rsplit("/", 1)[-1]
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name).lower()
    name = re.sub(r"[^a-z0-9]+", "_", name).strip("_")
    if not name:
        name = "metric"
    if name[0].isdigit():
        name = f"m_{name}"
    return name


def metric_column_names(metric_ids: list[str]) -> list[str]:
    """Derive one unique column name per metric; clashes get a numeric suffix."""
    used = set(RESERVED_COLUMNS)
    names = []
    for metric_id in metric_ids:
        base = sanitize_column_name(metric_id)
        name, n = base, 2
        while name in used:
            name, n = f"{base}_{n}", n + 1
        used.add(name)
        names.append(name)
    return names


def build_report_schema(cfg: ReportConfig) -> StructType:
    fields = [
        StructField(COL_RSID, StringType(), False),
        StructField(COL_DATE, DateType(), False),
    ]
    if cfg.granularity == "hour":
        fields.append(StructField(COL_HOUR, LongType(), False))
    if cfg.dimension:
        fields.append(StructField(COL_DIMENSION_VALUE, StringType(), True))
        fields.append(StructField(COL_DIMENSION_ITEM_ID, StringType(), True))
    fields.append(StructField(COL_SEGMENT_IDS, StringType(), False))
    fields.append(StructField(COL_RETRIEVED_AT, TimestampType(), True))
    fields.extend(StructField(name, DoubleType(), True) for name in cfg.metric_columns)
    return StructType(fields)


def report_primary_keys(cfg: ReportConfig) -> list[str]:
    keys = [COL_RSID, COL_DATE]
    if cfg.granularity == "hour":
        keys.append(COL_HOUR)
    if cfg.dimension:
        keys.append(COL_DIMENSION_VALUE)
    keys.append(COL_SEGMENT_IDS)
    return keys


# ---------------------------------------------------------------------------
# Time buckets
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Bucket:
    day: date
    hour: int | None
    date_range: str

    @property
    def key(self) -> str:
        return (
            self.day.isoformat() if self.hour is None else f"{self.day.isoformat()}T{self.hour:02d}"
        )


def format_date_range(start: datetime, end: datetime) -> str:
    """Adobe treats the end of a dateRange as exclusive, so ``end`` is the next bucket's start."""
    return f"{start:%Y-%m-%dT%H:%M:%S}.000/{end:%Y-%m-%dT%H:%M:%S}.000"


def day_range(first: date, last: date) -> str:
    """dateRange covering whole local days ``first``..``last`` inclusive."""
    start = datetime(first.year, first.month, first.day)
    end = datetime(last.year, last.month, last.day) + timedelta(days=1)
    return format_date_range(start, end)


def buckets_for_day(day: date, granularity: str) -> list[Bucket]:
    midnight = datetime(day.year, day.month, day.day)
    if granularity == "hour":
        return [
            Bucket(
                day,
                h,
                format_date_range(midnight + timedelta(hours=h), midnight + timedelta(hours=h + 1)),
            )
            for h in range(24)
        ]
    return [Bucket(day, None, format_date_range(midnight, midnight + timedelta(days=1)))]


def bucket_from_key(key: str, granularity: str) -> Bucket:
    if "T" in key:
        day_text, hour_text = key.split("T", 1)
        day, hour = date.fromisoformat(day_text), int(hour_text)
        start = datetime(day.year, day.month, day.day) + timedelta(hours=hour)
        return Bucket(day, hour, format_date_range(start, start + timedelta(hours=1)))
    return buckets_for_day(date.fromisoformat(key), granularity)[0]


def parse_time_item_id(item_id, granularity: str) -> tuple[date, int | None]:
    """Decode the ``itemId`` of a ``variables/daterange*`` row into (bucket start date, hour).

    The itemId is used because the display ``value`` is locale-dependent. Adobe's
    encoding is all digits: a century digit (0 = 1900s, 1 = 2000s, ...), then
    YYMMDD for day and week rows (the date the bucket starts), YYMM01 for month
    rows, and YYMMDDHH for hour rows. MM is zero-based (00 = January), so itemId
    1250114 is February 14, 2025, 125011409 is 09:00 that day, and 1250601 is
    July 2025. Month rows ending in 00 or with no day digits are also accepted;
    anything else raises.
    """
    text = str(item_id or "").strip()
    expected = {"day": (7,), "week": (7,), "month": (7, 5), "hour": (9,)}[granularity]
    if not text.isdigit() or len(text) not in expected:
        raise ValueError(
            f"Could not read the {granularity} bucket from Adobe itemId {item_id!r} "
            f"(expected {' or '.join(str(n) for n in expected)} digits)."
        )
    year = 1900 + 100 * int(text[0]) + int(text[1:3])
    month = int(text[3:5]) + 1
    if granularity == "month":
        day_of_month = 1
    else:
        day_of_month = int(text[5:7])
    hour = int(text[7:9]) if granularity == "hour" else None
    try:
        bucket_day = date(year, month, day_of_month)
    except ValueError as exc:
        raise ValueError(f"Adobe itemId {item_id!r} does not encode a valid date.") from exc
    if hour is not None and not 0 <= hour <= 23:
        raise ValueError(f"Adobe itemId {item_id!r} does not encode a valid hour.")
    return bucket_day, hour


# ---------------------------------------------------------------------------
# Requests and responses
# ---------------------------------------------------------------------------


def build_report_request(
    cfg: ReportConfig, date_range: str, page: int, limit: int, trended: bool
) -> dict:
    global_filters = [{"type": "dateRange", "dateRange": date_range}]
    global_filters.extend({"type": "segment", "segmentId": s} for s in cfg.segment_ids)
    metrics = [{"columnId": str(i), "id": m} for i, m in enumerate(cfg.metrics)]
    settings = {
        "limit": limit,
        "page": page,
        "countRepeatInstances": cfg.count_repeat_instances,
    }
    if trended:
        dimension = TIME_DIMENSIONS[cfg.granularity]
        settings["dimensionSort"] = "asc"
    else:
        dimension = cfg.dimension
        # Sorting by the first metric keeps the biggest values when max_rows_per_bucket truncates.
        metrics[0]["sort"] = "desc"
        settings["nonesBehavior"] = cfg.nones_behavior
    return {
        "rsid": cfg.rsid,
        "globalFilters": global_filters,
        "metricContainer": {"metrics": metrics},
        "dimension": dimension,
        "settings": settings,
    }


def check_column_errors(body: dict, cfg: ReportConfig) -> None:
    """A 206 with columnErrors means some metrics came back empty; never emit those as nulls."""
    columns = body.get("columns") if isinstance(body, dict) else None
    errors = (columns or {}).get("columnErrors") or []
    if not errors:
        return
    details = []
    for err in errors:
        metric = None
        try:
            metric = cfg.metrics[int(err.get("columnId"))]
        except (TypeError, ValueError, IndexError):
            pass
        label = metric or f"column {err.get('columnId')}"
        code = err.get("errorCode") or "error"
        details.append(f"{label}: {code} - {err.get('errorDescription') or ''}".strip())
    raise ValueError(
        f"Adobe could not compute some columns of report table '{cfg.table_name}' for report "
        f"suite '{cfg.rsid}': {'; '.join(details)}. Usually the metric or dimension is not "
        "enabled for this report suite, or the credential's product profile does not allow it."
    )


def _to_float(value) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def row_metric_values(row: dict, column_ids, metric_count: int) -> list[float | None]:
    """Map ``row.data`` onto request metric order via ``columns.columnIds``."""
    data = row.get("data") or []
    values: list[float | None] = [None] * metric_count
    if column_ids:
        for position, column_id in enumerate(column_ids):
            try:
                index = int(column_id)
            except (TypeError, ValueError):
                continue
            if 0 <= index < metric_count and position < len(data):
                values[index] = _to_float(data[position])
    else:
        for index in range(min(metric_count, len(data))):
            values[index] = _to_float(data[index])
    return values
