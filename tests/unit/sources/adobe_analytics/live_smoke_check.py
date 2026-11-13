"""Read-only live smoke check for the Adobe Analytics connector.

Use this instead of record mode. It signs in to a real Adobe Analytics account,
reads a few rows from every built-in table and from small report tables, checks
the rows against the connector's schemas and primary keys, and then checks these
behaviours against the live account:

  - the itemId format of hour, day, week and month rows, and how Adobe labels a
    week or month that starts before the requested date range
  - which rows nones_behavior=exclude removes, and the label of the "Unspecified" row
  - the type and precision of a percent metric and a time metric, and whether
    percent values arrive as fractions
  - the page size Adobe's list endpoints return, and whether includeType=all is
    accepted or the connector falls back to shared
  - whether a virtual report suite ID works as the report suite of a report table

It does not settle the open items listed under "Still to be confirmed" in the
connector README (for example whether time metrics are in seconds).

Everything stays in memory: nothing is recorded, cached or written to disk.

What it prints: table names, row counts, formats (digits shown as 9 and letters
as A/a, plus a decoded pattern such as CYYMMDD), the Unspecified row's label,
metric types and order-of-magnitude ranges, page sizes, and Pass/Fail/Warning.
It never prints row contents, metric values, report suite IDs, component IDs or
names. The Unspecified label is printed only when exactly one row is removed by
nones_behavior=exclude and the label is short. A failed check shows the first
line of the error, which can name a report suite or component ID. Connector log
messages are hidden by default because some name a report suite or dimension
value; pass --show-warnings to print them on screen. Do not redirect the output
to a file inside the repository.

Usage (from the repository root):

  CONNECTOR_TEST_CONFIG_PATH=/path/outside/repo/adobe.json \\
    python tests/unit/sources/adobe_analytics/live_smoke_check.py --dimension variables/page

Credentials come from CONNECTOR_TEST_CONFIG_JSON (inline JSON) or
CONNECTOR_TEST_CONFIG_PATH (a JSON file kept outside the repository), with the
connection keys client_id, client_secret and optionally global_company_id and
scopes.

Options:
  --rsid ID             Report suite or vrs_ ID to test
                        (default: the first suite the credential can see)
  --metrics IDS         Comma-separated metric IDs (default: metrics/visits,metrics/pageviews)
  --dimension ID        Test a report broken down by this dimension, the stale-row delete flow,
                        and nones_behavior (choose a dimension that has unset values)
  --percent-metric ID   Percent metric to inspect (default: the first percent-type metric found)
  --time-metric ID      Time metric to inspect (default: the first time-type metric found)
  --days N              Days of data to read for daily report tables (default: 2)
  --max-rows N          Rows to inspect per table (default: 25)
  --skip-open-items     Run only the basic table checks
  --show-warnings       Print connector warnings on screen

A full run sends about 45 requests and takes several minutes because the
connector stays under Adobe's rate limit.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from collections import Counter
from datetime import date, datetime, timedelta
from itertools import islice
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO_ROOT / "src"))

from databricks.labs.community_connector.sources.adobe_analytics.adobe_analytics import (  # noqa: E402
    AdobeAnalyticsLakeflowConnect,
)
from databricks.labs.community_connector.sources.adobe_analytics.adobe_analytics_client import (  # noqa: E402
    AdobeAnalyticsAPIError,
    extract_list_page,
)
from databricks.labs.community_connector.sources.adobe_analytics.adobe_analytics_reports import (  # noqa: E402
    TIME_DIMENSIONS,
    day_range,
    metric_column_names,
    parse_time_item_id,
)
from databricks.labs.community_connector.sources.adobe_analytics.adobe_analytics_schemas import (  # noqa: E402
    CALCULATED_METRICS_PATH,
    DATE_RANGES_PATH,
    METRICS_PATH,
    REPORTS_PATH,
    SEGMENTS_PATH,
    SUITES_PATH,
    VRS_PATH,
)

DEFAULT_METRICS = "metrics/visits,metrics/pageviews"
CONNECTOR_LOGGER = "databricks.labs.community_connector.sources.adobe_analytics"
LIST_PROBE_LIMIT = 1000
# Adobe's own metrics are the same in every account, so naming them reveals nothing.
STANDARD_METRICS = {
    "metrics/bouncerate",
    "metrics/timespentvisit",
    "metrics/timespentvisitor",
    "metrics/averagetimespentonsite",
    "metrics/averagetimespentonpage",
    "metrics/averagepagedepth",
    "metrics/reloads",
}
PREFERRED_PERCENT = ("metrics/bouncerate",)
PREFERRED_TIME = ("metrics/timespentvisit", "metrics/averagetimespentonsite")
WEEKDAYS = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")


class _LogCapture(logging.Handler):
    """Keeps connector log records in memory so the script can count them without printing them."""

    def __init__(self) -> None:
        super().__init__(level=logging.INFO)
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)

    def warnings(self) -> int:
        return sum(1 for r in self.records if r.levelno >= logging.WARNING)

    def matching(self, prefix: str) -> list[logging.LogRecord]:
        return [r for r in self.records if str(r.msg).startswith(prefix)]


class _Results:
    def __init__(self) -> None:
        self.items: list[tuple[str, str]] = []

    def add(self, status: str, label: str, detail: str) -> None:
        icon = {"pass": "Pass ✅", "fail": "Fail ❌", "warn": "Warning ⚠️"}[status]
        self.items.append((status, label))
        print(f"{icon}  {label}: {detail}")

    def count(self, status: str) -> int:
        return sum(1 for s, _ in self.items if s == status)


def _load_credentials() -> dict:
    inline = os.environ.get("CONNECTOR_TEST_CONFIG_JSON", "").strip()
    if inline:
        return json.loads(inline)
    path = os.environ.get("CONNECTOR_TEST_CONFIG_PATH", "").strip()
    if path:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    raise SystemExit(
        "Fail ❌  No credentials. Set CONNECTOR_TEST_CONFIG_PATH to a JSON file outside the "
        "repository (or CONNECTOR_TEST_CONFIG_JSON to inline JSON) with client_id and "
        "client_secret."
    )


def _error_line(exc: Exception) -> str:
    if isinstance(exc, AdobeAnalyticsAPIError) and exc.status_code is not None:
        # A 2xx error comes from the client itself (e.g. a body that isn't JSON); its
        # message names only the path and parse error. Other error bodies can quote IDs.
        if 200 <= exc.status_code < 300:
            return str(exc).splitlines()[0][:240]
        return f"HTTP {exc.status_code}"
    return f"{type(exc).__name__}: {str(exc).splitlines()[0][:160] if str(exc) else ''}"


def _mask(text: str) -> str:
    """Show a value's shape only: digits become 9, letters become A or a."""
    text = re.sub(r"[0-9]", "9", text)
    text = re.sub(r"[A-Z]", "A", text)
    return re.sub(r"[a-z]", "a", text)


def _item_id_pattern(item_id: str, decoded: tuple[date, int | None] | None) -> str:
    """Name the itemId layout, e.g. CYYMMDD, by rebuilding it from the decoded date."""
    if decoded is None:
        return f"not decodable ({_mask(item_id)})"
    day, hour = decoded
    # MM is zero-based (00 = January), as the connector decodes it.
    prefix = f"{(day.year - 1900) // 100}{day:%y}{day.month - 1:02d}"
    candidates = [
        ("CYYMMDDHH", f"{prefix}{day.day:02d}{hour:02d}" if hour is not None else None),
        ("CYYMMDD", f"{prefix}{day.day:02d}"),
        ("CYYMM00", f"{prefix}00"),
        ("CYYMM01", f"{prefix}01"),
        ("CYYMM", prefix),
    ]
    for name, rebuilt in candidates:
        if rebuilt == item_id:
            return name
    return f"unrecognised ({_mask(item_id)})"


def _magnitude(values: list[float]) -> str:
    present = [abs(v) for v in values if isinstance(v, (int, float))]
    if not present:
        return "no data"
    if not any(present):
        return "all zero"
    top = max(present)
    for limit, label in ((1, "0 to 1"), (100, "1 to 100"), (10_000, "100 to 10,000")):
        if top <= limit:
            return label
    return "above 10,000"


def _check_rows(
    connector, table: str, options: dict, max_rows: int, deletes: bool = False
) -> tuple[bool, str, list[dict]]:
    """Read up to max_rows and confirm they match the schema and primary keys."""
    schema = connector.get_table_schema(table, options)
    meta = connector.read_table_metadata(table, options)
    fields = {f.name for f in schema.fields}
    keys = meta.get("primary_keys") or []
    reader = connector.read_table_deletes if deletes else connector.read_table
    records, _offset = reader(table, None, options)
    rows = list(islice(records, max_rows))
    problems = []
    for row in rows:
        extra = set(row) - fields
        if extra:
            problems.append(f"{len(extra)} column(s) not in the schema")
        if any(row.get(k) is None for k in keys if k != "segment_ids"):
            problems.append("a primary key column is empty")
    if problems:
        return False, f"{len(rows)} row(s) read; " + "; ".join(sorted(set(problems))), rows
    kind = "delete record(s)" if deletes else "row(s)"
    detail = f"{len(rows)} {kind} read, schema and keys OK ({meta.get('ingestion_type')})"
    return True, detail, rows


# ---------------------------------------------------------------------------
# Open items
# ---------------------------------------------------------------------------


def _time_report(client, rsid: str, granularity: str, first: date, last: date) -> list[dict]:
    body = {
        "rsid": rsid,
        "globalFilters": [{"type": "dateRange", "dateRange": day_range(first, last)}],
        "metricContainer": {"metrics": [{"columnId": "0", "id": "metrics/visits"}]},
        "dimension": TIME_DIMENSIONS[granularity],
        "settings": {"limit": 1000, "page": 0, "dimensionSort": "asc"},
    }
    return (client.post(REPORTS_PATH, body) or {}).get("rows") or []


def _decode_rows(rows: list[dict], granularity: str) -> list[tuple[date, int | None] | None]:
    decoded: list[tuple[date, int | None] | None] = []
    for row in rows:
        try:
            decoded.append(parse_time_item_id(row.get("itemId"), granularity))
        except ValueError:
            decoded.append(None)
    return decoded


# English label layouts Adobe uses, e.g. "09:00 2025-02-14", "Feb 14, 2025", "Jul 2025".
LABEL_FORMATS = {
    "hour": ("%H:%M %Y-%m-%d",),
    "day": ("%b %d, %Y",),
    "week": ("%b %d, %Y",),
    "month": ("%b %Y", "%B %Y"),
}


def _label_start(label, granularity: str) -> tuple[date, int | None] | None:
    """Read a bucket start from Adobe's display label, or None if the layout is unknown."""
    text = str(label or "").strip()
    for fmt in LABEL_FORMATS[granularity]:
        try:
            parsed = datetime.strptime(text, fmt)
        except ValueError:
            continue
        return parsed.date(), parsed.hour if granularity == "hour" else None
    return None


def _describe_gap(decoded: tuple[date, int | None], labelled: tuple[date, int | None]) -> str:
    days = (labelled[0] - decoded[0]).days
    if days:
        side = "earlier" if days > 0 else "later"
        return f"the first mismatch decodes {abs(days)} day(s) {side} than its label"
    return f"the first mismatch decodes hour {decoded[1]} but its label says hour {labelled[1]}"


def _compare_labels(rows: list[dict], decoded, granularity: str) -> tuple[str, str | None]:
    """Compare each decoded itemId with that row's label.

    Returns (note to append, failure message or None). Labels are locale-dependent,
    so a layout this script can't read is inconclusive rather than a failure.
    """
    checked = mismatched = unreadable = 0
    gap = ""
    for row, dec in zip(rows, decoded):
        if dec is None:
            continue
        labelled = _label_start(row.get("value"), granularity)
        if labelled is None:
            unreadable += 1
            continue
        checked += 1
        if labelled != dec:
            mismatched += 1
            gap = gap or _describe_gap(dec, labelled)
    if mismatched:
        return "", (
            f"the decoded date differs from Adobe's label in {mismatched} of {checked} row(s); "
            f"{gap}. The connector's itemId decoding is wrong for {granularity} rows"
        )
    if not checked:
        return "; labels are in a layout this script can't read, so they were not compared", None
    note = f"; all {checked} decoded date(s) match Adobe's labels"
    if unreadable:
        note += f" ({unreadable} label(s) in an unknown layout skipped)"
    return note, None


def _describe_formats(rows: list[dict], decoded) -> str:
    patterns = sorted({_item_id_pattern(str(r.get("itemId")), d) for r, d in zip(rows, decoded)})
    labels = sorted({_mask(str(r.get("value"))) for r in rows})
    return f"itemId {', '.join(patterns)}; display label {' | '.join(labels[:3])}"


def check_time_formats(client, rsid: str, today: date, results: _Results) -> None:
    print("\nItem 1: date and time formats (formats only)...")
    yesterday = today - timedelta(days=1)

    for granularity, first in (("day", today - timedelta(days=2)), ("hour", yesterday)):
        try:
            rows = _time_report(client, rsid, granularity, first, today)
        except Exception as exc:  # noqa: BLE001
            results.add("fail", f"{granularity} format", _error_line(exc))
            continue
        decoded = _decode_rows(rows, granularity)
        detail = f"{len(rows)} row(s); {_describe_formats(rows, decoded)}"
        if not rows:
            results.add("warn", f"{granularity} format", "no rows returned; inconclusive")
            continue
        if None in decoded:
            results.add(
                "fail", f"{granularity} format", detail + "; the connector cannot decode it"
            )
            continue
        label_note, mismatch = _compare_labels(rows, decoded, granularity)
        if mismatch:
            results.add("fail", f"{granularity} format", f"{detail}; {mismatch}")
            continue
        detail += label_note
        if granularity == "hour":
            valid = [d for d in decoded if d is not None]
            per_day = Counter(d for d, _h in valid)
            hours = sorted({h for _d, h in valid if h is not None})
            detail += (
                f"; {per_day.get(yesterday, 0)} hourly row(s) for the last complete day; "
                f"hours {hours[0]} to {hours[-1]}"
            )
        results.add("pass", f"{granularity} format", detail + "; the connector decodes it")

    # A range starting on a Wednesday shows how Adobe labels the first, partial week.
    mid_week = today - timedelta(days=(today.weekday() - 2) % 7 + 14)
    _check_partial_period(client, rsid, "week", mid_week, today, results)
    # A range starting on the 15th does the same for months.
    two_months_back = (today.replace(day=1) - timedelta(days=1)).replace(day=1) - timedelta(days=1)
    mid_month = two_months_back.replace(day=15)
    _check_partial_period(client, rsid, "month", mid_month, today, results)


def _check_partial_period(
    client, rsid: str, granularity: str, first: date, today: date, results: _Results
) -> None:
    starts_on = WEEKDAYS[first.weekday()] if granularity == "week" else "on day 15"
    label = f"{granularity} format (range starts {starts_on})"
    try:
        rows = _time_report(client, rsid, granularity, first, today)
    except Exception as exc:  # noqa: BLE001
        results.add("fail", label, _error_line(exc))
        return
    decoded = _decode_rows(rows, granularity)
    if not rows:
        results.add("warn", label, "no rows returned; inconclusive")
        return
    detail = f"{len(rows)} row(s); {_describe_formats(rows, decoded)}"
    if None in decoded:
        results.add("fail", label, detail + "; the connector cannot decode it")
        return
    label_note, mismatch = _compare_labels(rows, decoded, granularity)
    if mismatch:
        results.add("fail", label, f"{detail}; {mismatch}")
        return
    detail += label_note
    starts = [d[0] for d in decoded if d is not None]
    first_bucket = starts[0]
    if granularity == "week":
        later = sorted({WEEKDAYS[d.weekday()] for d in starts[1:]})
        detail += f"; full weeks start on {', '.join(later) or 'unknown'}"
        # The range start is a genuine period start when weeks begin on that weekday.
        genuine_start = later == [WEEKDAYS[first.weekday()]]
    else:
        # Retail (4-5-4 and similar) calendars start months on other days than the 1st.
        later_days = {d.day for d in starts[1:]}
        detail += (
            f"; full months start on day {', '.join(map(str, sorted(later_days))) or 'unknown'}"
        )
        genuine_start = later_days != {1}
    if first_bucket < first:
        gap = (first - first_bucket).days
        results.add(
            "pass",
            label,
            detail + f"; the first row is labelled with the true period start ({gap} day(s) "
            "before the range), so the connector correctly drops it as partial",
        )
    elif first_bucket == first and genuine_start:
        results.add(
            "warn",
            label,
            detail + "; the range happens to begin on a period start in this report suite's "
            "calendar, so this run can't show how a partial period is labelled; inconclusive",
        )
    elif first_bucket == first:
        results.add(
            "fail",
            label,
            detail + "; the first row is labelled with the range start, not the true period "
            "start, so the connector would keep a partial period as if complete (needs a fix)",
        )
    else:
        results.add(
            "warn", label, detail + "; the first row starts after the range start; inconclusive"
        )


def check_nones(connector, rsid: str, dimension: str | None, start: str, results: _Results) -> None:
    print("\nItem 2: nones_behavior and the Unspecified row...")
    if not dimension:
        results.add("warn", "nones_behavior", "skipped; pass --dimension (one with unset values)")
        return
    base = {
        "rsid": rsid,
        "metrics": "metrics/visits",
        "dimension": dimension,
        "start_date": start,
        "max_rows_per_bucket": "5000",
        "delete_stale_rows": "false",
    }
    try:
        kept, _ = connector.read_table(
            "smoke_nones_return", None, {**base, "nones_behavior": "return"}
        )
        dropped, _ = connector.read_table(
            "smoke_nones_exclude", None, {**base, "nones_behavior": "exclude"}
        )
        kept, dropped = list(kept), list(dropped)
    except Exception as exc:  # noqa: BLE001
        results.add("fail", "nones_behavior", _error_line(exc))
        return
    kept_items = {(r.get("dimension_item_id"), r.get("dimension_value")) for r in kept}
    dropped_items = {(r.get("dimension_item_id"), r.get("dimension_value")) for r in dropped}
    only_in_return = kept_items - dropped_items
    detail = (
        f"return: {len(kept)} row(s), exclude: {len(dropped)} row(s); "
        f"{len(only_in_return)} distinct value(s) appear only with return"
    )
    if not only_in_return:
        results.add(
            "warn",
            "nones_behavior",
            detail + "; no Unspecified row in this window (try a dimension with unset values)",
        )
        return
    if len(only_in_return) == 1:
        item_id, label = next(iter(only_in_return))
        label = str(label)
        shown = f"'{label}'" if len(label) <= 30 else f"(hidden, {len(label)} characters)"
        detail += f"; Unspecified label {shown}, itemId format {_mask(str(item_id))}"
        results.add("pass", "nones_behavior", detail)
        return
    results.add(
        "warn",
        "nones_behavior",
        detail
        + "; more than one value differs, so the label is not shown (top-N cut-off may differ)",
    )


def _pick_metric(
    metrics: list[dict], wanted_type: str, override: str | None, preferred
) -> dict | None:
    by_id = {m.get("id"): m for m in metrics}
    if override:
        return by_id.get(override) or {"id": override, "type": None}
    typed = [m for m in metrics if str(m.get("type") or "").lower() == wanted_type]
    for metric_id in preferred:
        if metric_id in by_id and by_id[metric_id] in typed:
            return by_id[metric_id]
    if typed:
        return typed[0]
    # Adobe types its standard time metrics as "int" (seconds), not "time".
    return next((by_id[m] for m in preferred if m in by_id), None)


def check_metric_units(
    connector, client, rsid: str, start: str, percent_override, time_override, results: _Results
) -> None:
    print("\nItem 3: percent and time metric types...")
    try:
        metrics = client.get(METRICS_PATH, {"rsid": rsid})
    except Exception as exc:  # noqa: BLE001
        results.add("fail", "metric list", _error_line(exc))
        return
    metrics = [m for m in (metrics if isinstance(metrics, list) else []) if isinstance(m, dict)]
    types = Counter(str(m.get("type") or "none").lower() for m in metrics)
    print(f"        Metric types in this report suite: {dict(sorted(types.items()))}")

    chosen = []
    for kind, override, preferred in (
        ("percent", percent_override, PREFERRED_PERCENT),
        ("time", time_override, PREFERRED_TIME),
    ):
        metric = _pick_metric(metrics, kind, override, preferred)
        if metric is None:
            results.add(
                "warn",
                f"{kind} metric",
                f"no {kind}-type metric or standard {kind} metric found; pass --{kind}-metric",
            )
            continue
        chosen.append((kind, metric))
    if not chosen:
        return

    metric_ids = [m["id"] for _k, m in chosen]
    options = {"rsid": rsid, "metrics": ",".join(metric_ids), "start_date": start}
    try:
        records, _ = connector.read_table("smoke_metric_units", None, options)
        records = list(records)
    except Exception as exc:  # noqa: BLE001
        results.add("fail", "metric types report", _error_line(exc))
        return
    columns = metric_column_names(metric_ids)
    for (kind, metric), column in zip(chosen, columns):
        name = metric["id"] if metric["id"] in STANDARD_METRICS else "a metric (ID hidden)"
        values = [r.get(column) for r in records]
        size = _magnitude(values)
        whole = all(float(v).is_integer() for v in values if isinstance(v, (int, float)))
        detail = (
            f"{name}: type {metric.get('type')}, precision {metric.get('precision')}; "
            f"{len(records)} daily value(s) in the range {size}, "
            f"{'whole numbers' if whole else 'with decimals'}"
        )
        if size in ("no data", "all zero"):
            results.add("warn", f"{kind} metric", detail + "; inconclusive")
        elif kind == "percent":
            unit = (
                "a fraction (0.25 means 25%)"
                if size == "0 to 1"
                else "percentage points (25 means 25%)"
            )
            results.add("pass", f"{kind} metric", detail + f"; returned as {unit}")
        else:
            results.add(
                "pass",
                f"{kind} metric",
                detail + "; type and range read (whether time metrics are in seconds stays open)",
            )


def check_list_endpoints(client, log: _LogCapture, results: _Results) -> None:
    print("\nItem 4: list endpoint page sizes and includeType...")
    probes = [
        ("report_suites", SUITES_PATH, False),
        ("virtual_report_suites", VRS_PATH, False),
        ("segments", SEGMENTS_PATH, True),
        ("calculated_metrics", CALCULATED_METRICS_PATH, True),
        ("date_ranges", DATE_RANGES_PATH, True),
    ]
    for table, path, components in probes:
        attempts = ["all", "shared", None] if components else [None]
        for include_type in attempts:
            params: dict[str, object] = {"limit": LIST_PROBE_LIMIT, "page": 0}
            if include_type:
                params["includeType"] = include_type
            try:
                body = client.get(path, params)
            except AdobeAnalyticsAPIError as exc:
                if components and exc.status_code == 403 and include_type != attempts[-1]:
                    print(
                        f"        {table}: includeType={include_type} refused (HTTP 403); "
                        "trying the next option"
                    )
                    continue
                results.add("fail", f"{table} page size", _error_line(exc))
                break
            except Exception as exc:  # noqa: BLE001
                results.add("fail", f"{table} page size", _error_line(exc))
                break
            items, last_page = extract_list_page(body)
            shape = "array" if isinstance(body, list) else "object with 'content'"
            paging = sorted(
                k
                for k in (
                    "totalElements",
                    "totalPages",
                    "numberOfElements",
                    "size",
                    "number",
                    "lastPage",
                )
                if isinstance(body, dict) and k in body
            )
            total = body.get("totalElements") if isinstance(body, dict) else None
            more_pages = last_page is False or (isinstance(total, int) and total > len(items))
            if more_pages and len(items) >= LIST_PROBE_LIMIT:
                size_note = (
                    f"returned a full page of {len(items)} with more pages after it "
                    "(Adobe accepts at least this many per page)"
                )
            elif more_pages:
                size_note = f"server capped the page at {len(items)} (asked for {LIST_PROBE_LIMIT})"
            else:
                size_note = f"all {len(items)} item(s) fit on one page of {LIST_PROBE_LIMIT}"
            detail = f"{shape}; paging fields {', '.join(paging) or 'none'}; {size_note}" + (
                f"; includeType={include_type or '(not set)'} accepted" if components else ""
            )
            results.add("pass", f"{table} page size", detail)
            break

    # What the connector itself did when it read these tables earlier in this run.
    fallbacks = log.matching("Adobe Analytics refused includeType")
    used = {
        r.args[1]: r.args[2]
        for r in log.matching("Adobe Analytics: read %d %s with includeType")
        if isinstance(r.args, tuple) and len(r.args) == 3
    }
    for table in ("segments", "calculated_metrics", "date_ranges"):
        if table in used:
            status = "pass" if used[table] in ("all", "shared") else "warn"
            note = (
                ""
                if used[table] == "all"
                else " (fell back; 'all' needs Analytics administrator rights)"
            )
            results.add(
                status,
                f"connector includeType for {table}",
                f"used includeType={used[table]}{note}",
            )
    if fallbacks:
        print(f"        The connector logged {len(fallbacks)} includeType fallback warning(s).")


class _Redact(logging.Filter):
    """Replace one ID in log messages before they reach the screen."""

    def __init__(self, secret: str) -> None:
        super().__init__()
        self.secret = secret

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        if self.secret in message:
            record.msg, record.args = message.replace(self.secret, "<virtual report suite>"), None
        return True


def check_virtual_report_suite(connector, metrics: str, today: date, results: _Results) -> None:
    print("\nItem 5: a virtual report suite ID as the report suite...")
    label = "virtual report suite as rsid"
    try:
        suites, _ = connector.read_table("virtual_report_suites", None, {})
        vrs_id = next((s.get("id") for s in suites if s.get("id")), None)
    except Exception as exc:  # noqa: BLE001
        results.add("fail", label, "could not list virtual report suites: " + _error_line(exc))
        return
    if not vrs_id:
        results.add(
            "warn", label, "the credential can't see any virtual report suites; inconclusive"
        )
        return
    # Connector warnings (shown with --show-warnings) and error text can name the ID.
    redact = _Redact(vrs_id)
    handlers = logging.getLogger().handlers
    for handler in handlers:
        handler.addFilter(redact)
    options = {
        "rsid": vrs_id,
        "metrics": metrics,
        "start_date": (today - timedelta(days=1)).isoformat(),
        "lookback_days": "1",
    }
    try:
        ok, detail, _rows = _check_rows(connector, "smoke_vrs_daily", options, 25)
    except Exception as exc:  # noqa: BLE001
        ok, detail = False, _error_line(exc)
    finally:
        for handler in handlers:
            handler.removeFilter(redact)
    detail = detail.replace(vrs_id, "<virtual report suite>")
    if ok:
        detail += "; Adobe accepted the virtual report suite ID as rsid for a daily report"
    results.add("pass" if ok else "fail", label, detail)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Read-only live smoke check for Adobe Analytics.")
    parser.add_argument("--rsid")
    parser.add_argument("--metrics", default=DEFAULT_METRICS)
    parser.add_argument("--dimension")
    parser.add_argument("--percent-metric")
    parser.add_argument("--time-metric")
    parser.add_argument("--days", type=int, default=2)
    parser.add_argument("--max-rows", type=int, default=25)
    parser.add_argument("--skip-open-items", action="store_true")
    parser.add_argument("--show-warnings", action="store_true")
    args = parser.parse_args(argv)

    log = _LogCapture()
    connector_logger = logging.getLogger(CONNECTOR_LOGGER)
    connector_logger.setLevel(logging.INFO)
    connector_logger.addHandler(log)
    connector_logger.propagate = args.show_warnings
    if args.show_warnings:
        logging.basicConfig(format="  log: %(levelname)s %(message)s")
        for handler in logging.getLogger().handlers:
            handler.setLevel(logging.WARNING)

    print("Signing in to Adobe Analytics (read-only, nothing is written to disk)...")
    try:
        connector = AdobeAnalyticsLakeflowConnect(_load_credentials())
        suites, _ = connector.read_table("report_suites", None, {})
        suites = list(suites)
    except Exception as exc:  # noqa: BLE001 - report any sign-in failure plainly
        print(f"Fail ❌  Could not sign in or list report suites: {type(exc).__name__}")
        print("        Check client_id, client_secret, the product profile, and global_company_id.")
        return 1
    print(f"Pass ✅  Signed in; the credential can see {len(suites)} report suite(s).")

    rsid = args.rsid or next((s.get("rsid") for s in suites if s.get("rsid")), None)
    if not rsid:
        print("Fail ❌  No report suite available to test. Pass --rsid.")
        return 1

    results = _Results()
    today = date.today()

    def run(label: str, table: str, options: dict, deletes: bool = False) -> None:
        try:
            ok, detail, _rows = _check_rows(connector, table, options, args.max_rows, deletes)
        except Exception as exc:  # noqa: BLE001 - one failing table shouldn't stop the rest
            ok, detail = False, _error_line(exc)
        results.add("pass" if ok else "fail", label, detail)

    print("\nChecking built-in reference tables...")
    for table in connector.list_tables():
        options = {"rsid": rsid} if table in ("dimensions", "metrics") else {}
        run(table, table, options)

    start = (today - timedelta(days=max(args.days - 1, 0))).isoformat()
    base = {"rsid": rsid, "metrics": args.metrics, "start_date": start, "lookback_days": "1"}
    print(
        f"\nChecking report tables ({args.days} day(s) of daily data, "
        "plus tiny hourly, weekly and monthly tables)..."
    )
    run("report, daily, no dimension", "smoke_daily", dict(base))
    run(
        "report, hourly, no dimension",
        "smoke_hourly",
        {**base, "granularity": "hour", "start_date": (today - timedelta(days=1)).isoformat()},
    )
    mid_week = today - timedelta(days=(today.weekday() - 2) % 7 + 14)
    mid_month = (
        (today.replace(day=1) - timedelta(days=1)).replace(day=1) - timedelta(days=1)
    ).replace(day=15)
    partial_before = len(log.matching("Adobe Analytics table '%s': dropped partial"))
    run(
        "report, weekly, starts mid-week",
        "smoke_weekly",
        {**base, "granularity": "week", "start_date": mid_week.isoformat()},
    )
    run(
        "report, monthly, starts mid-month",
        "smoke_monthly",
        {**base, "granularity": "month", "start_date": mid_month.isoformat()},
    )
    dropped = len(log.matching("Adobe Analytics table '%s': dropped partial")) - partial_before
    results.add(
        "pass" if dropped >= 2 else "warn",
        "partial first week and month",
        f"the connector dropped and logged {dropped} partial period(s) (expected 2)",
    )
    if args.dimension:
        dim_options = {**base, "dimension": args.dimension, "max_rows_per_bucket": "50"}
        run("report, daily by dimension", "smoke_daily_by_dimension", dim_options)
        run("report, stale-row delete flow", "smoke_daily_by_dimension", dim_options, deletes=True)

    if not args.skip_open_items:
        print("\nChecking live behaviours (formats, labels and counts only)...")
        client = connector._client
        steps = [
            lambda: check_time_formats(client, rsid, today, results),
            lambda: check_nones(connector, rsid, args.dimension, start, results),
            lambda: check_metric_units(
                connector, client, rsid, start, args.percent_metric, args.time_metric, results
            ),
            lambda: check_list_endpoints(client, log, results),
            lambda: check_virtual_report_suite(connector, args.metrics, today, results),
        ]
        for step in steps:
            try:
                step()
            except Exception as exc:  # noqa: BLE001 - keep going so one run answers as much as possible
                results.add("fail", "open-item check", _error_line(exc))

    print()
    if log.warnings():
        hint = "" if args.show_warnings else " (hidden; rerun with --show-warnings to see them)"
        print(f"Warning ⚠️  The connector logged {log.warnings()} warning(s){hint}.")
    passed, failed, warned = results.count("pass"), results.count("fail"), results.count("warn")
    summary = f"{passed} passed, {failed} failed, {warned} inconclusive"
    if failed:
        print(f"Fail ❌  {summary}. See the lines marked Fail above. Nothing was saved.")
        return 1
    print(f"Pass ✅  {summary}. Nothing was saved.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
