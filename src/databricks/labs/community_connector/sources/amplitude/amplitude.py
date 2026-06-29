"""Lakeflow community connector for Amplitude.

Covers ten objects exposed across Amplitude's Analytics APIs:

| Table                          | API                  | Ingestion | Cursor              |
|--------------------------------|----------------------|-----------|---------------------|
| ``events``                     | Export API           | append    | server_upload_time  |
| ``events_list``                | Dashboard REST API   | snapshot  | —                   |
| ``active_users_counts``        | Dashboard REST API   | cdc       | date                |
| ``average_session_length``     | Dashboard REST API   | cdc       | date                |
| ``session_length_distribution``| Dashboard REST API   | snapshot  | —                   |
| ``sessions_per_user``          | Dashboard REST API   | cdc       | date                |
| ``cohorts``                    | Behavioral Cohorts   | snapshot  | —                   |
| ``annotations``                | Chart Annotations    | snapshot  | —                   |
| ``taxonomy_events``            | Taxonomy API         | snapshot  | —                   |
| ``taxonomy_user_properties``   | Taxonomy API         | snapshot  | —                   |

Authentication is HTTP Basic (``api_key`` : ``secret_key``).  The base URL
switches between the standard and EU data-residency hosts via the
``data_region`` option.

Response-shape tolerance: the Export API returns a ZIP of gzipped NDJSON
and the Dashboard endpoints return a ``{series, xValues}`` time series.
Each reader also accepts a plain JSON array of already-shaped rows so the
connector is exercisable against the in-process source simulator (which
serves JSON built from the table schema).
"""

import gzip
import io
import json
import time
import zipfile
from datetime import datetime, timedelta, timezone
from typing import Iterator

import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import RequestException
from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.amplitude.amplitude_schemas import (
    BASE_URLS,
    DEFAULT_TIMEOUT,
    EXPORT_TIMEOUT,
    INITIAL_BACKOFF,
    MAX_RETRIES,
    RETRIABLE_STATUS_CODES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
    TABLES,
)


class AmplitudeLakeflowConnect(LakeflowConnect):
    """LakeflowConnect implementation for Amplitude."""

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        self._api_key = options.get("api_key")
        self._secret_key = options.get("secret_key")
        if not self._api_key or not self._secret_key:
            raise ValueError(
                "Amplitude connector requires 'api_key' and 'secret_key' in options"
            )

        region = options.get("data_region", "standard").lower()
        if region not in BASE_URLS:
            raise ValueError(
                f"Unsupported data_region {region!r}; expected one of {sorted(BASE_URLS)}"
            )
        self._base_url = BASE_URLS[region]

        # Optional connection-level lower bound for the events backfill.
        self._start_date = options.get("start_date")

        # Freeze the upper bound at init time so incremental reads return a
        # stable cursor across microbatches within one Trigger.AvailableNow
        # trigger, guaranteeing termination.  A later trigger creates a fresh
        # instance with a newer cap and picks up any data that arrived since.
        now_utc = datetime.now(timezone.utc)
        self._init_ts = now_utc.isoformat()
        self._init_date = now_utc.strftime("%Y-%m-%d")

    # ----- HTTP -----------------------------------------------------------

    @property
    def _auth(self) -> HTTPBasicAuth:
        return HTTPBasicAuth(self._api_key, self._secret_key)

    def _request_with_retry(self, path: str, **kwargs) -> requests.Response:
        """GET ``path`` with exponential-backoff retries on transient errors.

        A fresh ``requests`` call is used per request (rather than a cached
        ``Session``) so the connector instance carries no non-picklable state
        when shipped to Spark executors.
        """
        url = f"{self._base_url}{path}"
        kwargs.setdefault("timeout", DEFAULT_TIMEOUT)
        backoff = INITIAL_BACKOFF
        resp = None
        last_exc: RequestException | None = None
        for attempt in range(MAX_RETRIES):
            try:
                resp = requests.get(url, auth=self._auth, **kwargs)
            except RequestException as exc:
                last_exc = exc
            else:
                if resp.status_code not in RETRIABLE_STATUS_CODES:
                    return resp
            if attempt < MAX_RETRIES - 1:
                # Respect server throttling hints when available.
                sleep_for = backoff
                if resp is not None:
                    retry_after = resp.headers.get("Retry-After", "").strip()
                    if retry_after:
                        try:
                            sleep_for = max(sleep_for, float(retry_after))
                        except ValueError:
                            pass
                time.sleep(sleep_for)
                backoff *= 2
        if resp is None and last_exc is not None:
            raise RuntimeError(
                f"Amplitude request failed after {MAX_RETRIES} attempts for {path}: {last_exc}"
            ) from last_exc
        return resp

    # ----- interface ------------------------------------------------------

    def list_tables(self) -> list[str]:
        """Static object list — Amplitude does not expose discovery."""
        return list(TABLES)

    def get_table_schema(
        self, table_name: str, table_options: dict[str, str]
    ) -> StructType:
        self._validate_table(table_name)
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(
        self, table_name: str, table_options: dict[str, str]
    ) -> dict:
        self._validate_table(table_name)
        return dict(TABLE_METADATA[table_name])

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        self._validate_table(table_name)
        start_offset = start_offset or {}

        if table_name == "events":
            return self._read_events(start_offset, table_options)
        if table_name == "events_list":
            return self._read_events_list()
        if table_name == "active_users_counts":
            return self._read_user_counts(start_offset, table_options)
        if table_name == "average_session_length":
            return self._read_session_length(start_offset, table_options)
        if table_name == "session_length_distribution":
            return self._read_session_length_distribution(table_options)
        if table_name == "sessions_per_user":
            return self._read_sessions_per_user(start_offset, table_options)
        if table_name == "cohorts":
            return self._read_cohorts()
        if table_name == "annotations":
            return self._read_annotations()
        if table_name == "taxonomy_events":
            return self._read_taxonomy_events()
        if table_name == "taxonomy_user_properties":
            return self._read_taxonomy_user_properties()
        raise ValueError(f"Unsupported table: {table_name!r}")

    # ----- helpers --------------------------------------------------------

    def _validate_table(self, table_name: str) -> None:
        if table_name not in TABLES:
            raise ValueError(
                f"Table '{table_name}' is not supported. Supported tables: {TABLES}"
            )

    @staticmethod
    def _parse_dt(value: str) -> datetime:
        """Parse an ISO-8601 timestamp, tolerating a trailing ``Z``.

        Always returns a timezone-aware UTC datetime so it can be compared
        against the init-time cap.
        """
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    # ----- events (Export API, append) ------------------------------------

    def _read_events(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Sliding hourly-window read over the Export API.

        One window per call: query ``[since, window_end]``, parse every event
        in the response, then advance the cursor to ``window_end`` (capped at
        the init-time snapshot).  Append-only tables must never truncate a
        server response, so the whole window is returned; ``window_hours``
        bounds the work per call instead of ``max_records_per_batch``.
        """
        init_dt = self._parse_dt(self._init_ts)

        cursor = start_offset.get("cursor")
        if cursor:
            since_dt = self._parse_dt(cursor)
        elif self._start_date:
            since_dt = self._parse_dt(self._start_date)
        else:
            # Default backfill: only the most recent window.  Set ``start_date``
            # (connection option) for deeper history.
            window_hours_default = self._positive_int_option(
                table_options, "window_hours", 24
            )
            since_dt = init_dt - timedelta(hours=window_hours_default)

        # Caught up to the init-time cap — signal "no more data".
        if since_dt >= init_dt:
            return iter([]), start_offset

        window_hours = self._positive_int_option(table_options, "window_hours", 24)
        window_end_dt = min(since_dt + timedelta(hours=window_hours), init_dt)

        params = {
            "start": since_dt.strftime("%Y%m%dT%H"),
            "end": window_end_dt.strftime("%Y%m%dT%H"),
        }
        resp = self._request_with_retry(
            "/api/2/export", params=params, stream=True, timeout=EXPORT_TIMEOUT
        )

        # 404 = no data in the requested window (not an error).
        if resp.status_code == 404:
            records = []
        elif resp.status_code != 200:
            raise RuntimeError(
                f"Amplitude Export API error: {resp.status_code} {resp.text[:500]}"
            )
        else:
            records = self._parse_export_body(resp.content)

        # The Export API treats ``end`` as *inclusive* at hourly resolution, so
        # the next window must start one hour past ``window_end_dt`` — otherwise
        # the boundary hour is fetched in both windows and, because ``events`` is
        # append-only (no load-time dedup), every record in it is ingested twice.
        end_offset = {"cursor": (window_end_dt + timedelta(hours=1)).isoformat()}
        return iter(records), end_offset

    @staticmethod
    def _parse_export_body(content: bytes) -> list[dict]:
        """Parse the Export API body into a list of event dicts.

        Real API: a ZIP archive of gzipped NDJSON files.  Simulator: a plain
        JSON array (or NDJSON) of event rows.  Both shapes are handled.
        """
        if not content:
            return []

        # Real Export API — ZIP of gzipped NDJSON.
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                records: list[dict] = []
                for name in zf.namelist():
                    with zf.open(name) as entry:
                        with gzip.open(entry, "rt", encoding="utf-8") as fh:
                            for line in fh:
                                line = line.strip()
                                if line:
                                    records.append(json.loads(line))
                return records
        except zipfile.BadZipFile:
            pass

        # Fallback (simulator / non-zip): JSON array, wrapped array, or NDJSON.
        text = content.decode("utf-8", errors="replace").strip()
        if not text:
            return []
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            return [json.loads(line) for line in text.splitlines() if line.strip()]

        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for key in ("events", "data", "records"):
                value = data.get(key)
                if isinstance(value, list):
                    return value
        return []

    # ----- events_list (Dashboard REST, snapshot) -------------------------

    def _read_events_list(self) -> tuple[Iterator[dict], dict]:
        resp = self._request_with_retry("/api/2/events/list")
        if resp.status_code != 200:
            raise RuntimeError(
                f"Amplitude events/list error: {resp.status_code} {resp.text[:500]}"
            )
        records = self._unwrap_list(resp.json(), "data")
        return iter(records), {}

    # ----- active_users_counts / average_session_length (cdc by date) -----

    def _read_user_counts(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        return self._read_dashboard_series(
            "/api/2/users", start_offset, table_options, flatten=self._flatten_user_counts
        )

    def _read_session_length(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        return self._read_dashboard_series(
            "/api/2/sessions/average",
            start_offset,
            table_options,
            flatten=self._flatten_session_length,
        )

    def _read_dashboard_series(
        self, path: str, start_offset: dict, table_options: dict[str, str], flatten
    ) -> tuple[Iterator[dict], dict]:
        """Sliding day-window read for the date-series Dashboard endpoints.

        One window per call.  The cursor is a ``YYYY-MM-DD`` date string; it
        advances to the window end (capped at the init-time date) so the read
        always makes forward progress and terminates.

        Admission control: these endpoints return a bounded date range per
        request (``window_days`` rows maximum).  ``max_records_per_batch`` is
        intentionally not read here — the window size is the natural batch
        bound, and the Amplitude Dashboard API does not support server-side
        record limits.  This is a deliberate divergence from the standard
        Strategy-B contract.
        """
        cursor = start_offset.get("cursor")
        since_date = cursor or self._start_date or self._default_metric_start(table_options)
        # Normalise to YYYY-MM-DD.
        since_date = since_date[:10]

        if since_date >= self._init_date:
            return iter([]), start_offset

        window_days = self._positive_int_option(table_options, "window_days", 30)
        since_dt = datetime.strptime(since_date, "%Y-%m-%d")
        window_end_dt = min(
            since_dt + timedelta(days=window_days),
            datetime.strptime(self._init_date, "%Y-%m-%d"),
        )
        window_end_date = window_end_dt.strftime("%Y-%m-%d")

        params = {
            "start": since_dt.strftime("%Y%m%d"),
            "end": window_end_dt.strftime("%Y%m%d"),
        }
        if path == "/api/2/users":
            params["m"] = table_options.get("m", "active")
            params["i"] = table_options.get("i", "1")
            if "g" in table_options:
                params["g"] = table_options["g"]

        resp = self._request_with_retry(path, params=params)
        if resp.status_code != 200:
            raise RuntimeError(
                f"Amplitude dashboard error for {path}: {resp.status_code} {resp.text[:500]}"
            )

        records = flatten(resp.json())
        end_offset = {"cursor": window_end_date}
        return iter(records), end_offset

    def _default_metric_start(self, table_options: dict[str, str]) -> str:
        """Default lower bound for date-series tables: one window back."""
        window_days = self._positive_int_option(table_options, "window_days", 30)
        init_dt = datetime.strptime(self._init_date, "%Y-%m-%d")
        return (init_dt - timedelta(days=window_days)).strftime("%Y-%m-%d")

    @staticmethod
    def _positive_int_option(
        table_options: dict[str, str], key: str, default_value: int
    ) -> int:
        """Parse a positive integer option and fail fast on invalid values."""
        raw_value = table_options.get(key, str(default_value))
        try:
            value = int(raw_value)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"Amplitude option '{key}' must be a positive integer; got {raw_value!r}"
            ) from exc
        if value <= 0:
            raise ValueError(
                f"Amplitude option '{key}' must be a positive integer; got {raw_value!r}"
            )
        return value

    @staticmethod
    def _coerce_int(value) -> int | None:
        if value is None:
            return None
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, (int, float)):
            return int(value)
        try:
            return int(str(value).strip())
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _coerce_float(value) -> float | None:
        if value is None:
            return None
        if isinstance(value, bool):
            return float(value)
        if isinstance(value, (int, float)):
            return float(value)
        try:
            return float(str(value).strip())
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _series_payload(body):
        """Return the ``data`` payload, tolerating both API and simulator shapes."""
        if isinstance(body, dict) and "data" in body:
            return body["data"]
        return body

    @classmethod
    def _flatten_user_counts(cls, body) -> list[dict]:
        payload = cls._series_payload(body)
        # Simulator path: already a flat list of {date, count, segment} rows.
        if isinstance(payload, list):
            return payload
        if not isinstance(payload, dict):
            return []

        series = payload.get("series") or []
        x_values = payload.get("xValues") or []
        meta = payload.get("seriesMeta") or []

        rows: list[dict] = []
        for s_idx, segment_series in enumerate(series):
            segment = cls._segment_label(meta, s_idx)
            for d_idx, date in enumerate(x_values):
                if d_idx < len(segment_series):
                    rows.append(
                        {
                            "date": date,
                            "count": cls._coerce_int(segment_series[d_idx]),
                            "segment": segment,
                        }
                    )
        return rows

    @classmethod
    def _flatten_session_length_distribution(cls, body) -> list[dict]:
        """Flatten the sessions/length histogram to one row per bucket."""
        payload = cls._series_payload(body)
        if isinstance(payload, list):
            return payload
        if not isinstance(payload, dict):
            return []
        series = payload.get("series") or []
        x_values = payload.get("xValues") or []
        if not series:
            return []
        first_series = series[0]
        return [
            {"bucket": bucket, "count": cls._coerce_int(first_series[d_idx])}
            for d_idx, bucket in enumerate(x_values)
            if d_idx < len(first_series)
        ]

    @classmethod
    def _flatten_sessions_per_user(cls, body) -> list[dict]:
        """Flatten the sessions/peruser time series to one row per date."""
        payload = cls._series_payload(body)
        if isinstance(payload, list):
            return payload
        if not isinstance(payload, dict):
            return []
        series = payload.get("series") or []
        x_values = payload.get("xValues") or []
        if not series:
            return []
        first_series = series[0]
        return [
            {"date": date, "avg_sessions": cls._coerce_float(first_series[d_idx])}
            for d_idx, date in enumerate(x_values)
            if d_idx < len(first_series)
        ]

    @classmethod
    def _flatten_session_length(cls, body) -> list[dict]:
        payload = cls._series_payload(body)
        if isinstance(payload, list):
            return payload
        if not isinstance(payload, dict):
            return []

        series = payload.get("series") or []
        x_values = payload.get("xValues") or []
        if not series:
            return []

        first_series = series[0]
        rows: list[dict] = []
        for d_idx, date in enumerate(x_values):
            if d_idx < len(first_series):
                rows.append(
                    {"date": date, "length": cls._coerce_float(first_series[d_idx])}
                )
        return rows

    @staticmethod
    def _segment_label(meta: list, idx: int) -> str:
        """Resolve a segment label from ``seriesMeta``.

        ``seriesMeta`` entries are either group-by labels (strings) or
        ``{"segmentIndex": n}`` dicts for the default (un-grouped) case.
        """
        if idx < len(meta):
            entry = meta[idx]
            if isinstance(entry, str):
                return entry
        return "Totals"

    # ----- session_length_distribution (Dashboard REST, snapshot) ---------

    def _read_session_length_distribution(
        self, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Snapshot of session length histogram over the most recent window.

        The Dashboard `/api/2/sessions/length` endpoint aggregates over the
        requested date range and returns one count per pre-defined bucket
        (e.g. "0s-60s", "60s-300s").  There is no per-day breakdown, so this
        is modelled as a snapshot keyed on ``bucket``.  ``window_days``
        controls the lookback period (default 30 days).
        """
        window_days = self._positive_int_option(table_options, "window_days", 30)
        init_dt = datetime.strptime(self._init_date, "%Y-%m-%d")
        start_dt = init_dt - timedelta(days=window_days)
        params = {
            "start": start_dt.strftime("%Y%m%d"),
            "end": init_dt.strftime("%Y%m%d"),
        }
        resp = self._request_with_retry("/api/2/sessions/length", params=params)
        if resp.status_code != 200:
            raise RuntimeError(
                f"Amplitude sessions/length error: {resp.status_code} {resp.text[:500]}"
            )
        records = self._flatten_session_length_distribution(resp.json())
        return iter(records), {}

    # ----- sessions_per_user (Dashboard REST, cdc) ------------------------

    def _read_sessions_per_user(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        return self._read_dashboard_series(
            "/api/2/sessions/peruser",
            start_offset,
            table_options,
            flatten=self._flatten_sessions_per_user,
        )

    # ----- cohorts (Behavioral Cohorts API, snapshot) ---------------------

    def _read_cohorts(self) -> tuple[Iterator[dict], dict]:
        resp = self._request_with_retry("/api/3/cohorts")
        if resp.status_code != 200:
            raise RuntimeError(
                f"Amplitude cohorts error: {resp.status_code} {resp.text[:500]}"
            )
        records = self._unwrap_list(resp.json(), "cohorts")
        for record in records:
            # ``definition`` is an opaque nested blob; serialise to a JSON
            # string to match the StringType column.
            definition = record.get("definition")
            if isinstance(definition, (dict, list)):
                record["definition"] = json.dumps(definition)
        return iter(records), {}

    # ----- annotations (Chart Annotations API, snapshot) ------------------

    def _read_annotations(self) -> tuple[Iterator[dict], dict]:
        resp = self._request_with_retry("/api/3/annotations")
        if resp.status_code != 200:
            raise RuntimeError(
                f"Amplitude annotations error: {resp.status_code} {resp.text[:500]}"
            )
        records = self._unwrap_list(resp.json(), "data")
        return iter(records), {}

    # ----- taxonomy_events (Taxonomy API, snapshot) -----------------------

    def _read_taxonomy_events(self) -> tuple[Iterator[dict], dict]:
        resp = self._request_with_retry("/api/2/taxonomy/event")
        if resp.status_code != 200:
            raise RuntimeError(
                f"Amplitude taxonomy/event error: {resp.status_code} {resp.text[:500]}"
            )
        raw = self._unwrap_list(resp.json(), "data")
        records = [self._flatten_taxonomy_event(r) for r in raw]
        return iter(records), {}

    @staticmethod
    def _flatten_taxonomy_event(record: dict) -> dict:
        """Flatten the nested ``category`` struct and normalise nulls."""
        category = record.get("category") or {}
        return {
            "event_type": record.get("event_type"),
            "category_name": category.get("name") if isinstance(category, dict) else None,
            "description": record.get("description"),
            "display_name": record.get("display_name"),
            "is_active": record.get("is_active"),
            "is_hidden_from_dropdowns": record.get("is_hidden_from_dropdowns"),
            "is_hidden_from_persona_results": record.get("is_hidden_from_persona_results"),
            "is_hidden_from_pathfinder": record.get("is_hidden_from_pathfinder"),
            "is_hidden_from_timeline": record.get("is_hidden_from_timeline"),
            "tags": record.get("tags") or [],
            "owner": record.get("owner"),
        }

    # ----- taxonomy_user_properties (Taxonomy API, snapshot) -------------

    def _read_taxonomy_user_properties(self) -> tuple[Iterator[dict], dict]:
        resp = self._request_with_retry("/api/2/taxonomy/user-property")
        if resp.status_code != 200:
            raise RuntimeError(
                f"Amplitude taxonomy/user-property error: {resp.status_code} {resp.text[:500]}"
            )
        records = self._unwrap_list(resp.json(), "data")
        return iter(records), {}

    @staticmethod
    def _unwrap_list(body, key: str) -> list[dict]:
        """Extract a record list from a wrapped or bare-list response."""
        if isinstance(body, list):
            return body
        if isinstance(body, dict):
            value = body.get(key)
            if isinstance(value, list):
                return value
        return []
