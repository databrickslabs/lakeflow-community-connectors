"""HL7 v2 community connector — ingests HL7 messages from multiple source types.

Supported source modes (configured via ``source_type`` connection option):

* ``gcp`` (default) — fetches messages from a Google Cloud Healthcare API
  HL7v2 store via REST.
* ``volume`` — reads HL7 v2 files directly from a Unity Catalog Volume path
  (e.g. ``/Volumes/catalog/schema/hl7_inbound/``).  Spark executors have
  Unity Catalog Volume FUSE access at runtime, so the connector uses plain
  filesystem I/O (``os.scandir`` + ``open()``) — no SDK, no HTTP, no auth
  options.  Permissions follow UC ``READ VOLUME`` grants on the configured
  Volume.  This mirrors the pattern used by the ``dicomweb`` connector for
  writing DICOM files to a Volume (which uses the same FUSE access with
  ``WRITE FILES``); this connector is the read-side equivalent.

Each HL7 segment type becomes its own table (msh, pid, pv1, obr, obx, …).

Schemas follow the HL7 v2.9 specification (the latest version, which is a
superset of all prior versions).

Incremental cursor: ``createTime`` (RFC3339 timestamp).  For GCP mode this
is the API-reported ``createTime``; for ``volume`` mode this is the file's
last-modified time, formatted as RFC3339.  The connector uses a sliding
time-window strategy to bound each micro-batch.
"""

from __future__ import annotations

import base64
import json
import os
import time
from datetime import datetime, timedelta, timezone
from fnmatch import fnmatch
from typing import Iterator

import requests
from google.auth.transport import requests as google_auth_requests
from google.oauth2 import service_account as google_sa
from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.hl7_v2.hl7_v2_extractors import (
    _EXTRACTORS,
    _SINGLE_SEGMENT_TABLES,
    _extract_al1,
    _extract_dg1,
    _extract_evn,
    _extract_ft1,
    _extract_generic,
    _extract_gt1,
    _extract_iam,
    _extract_in1,
    _extract_msh,
    _extract_mrg,
    _extract_nk1,
    _extract_nte,
    _extract_obr,
    _extract_obx,
    _extract_orc,
    _extract_pd1,
    _extract_pid,
    _extract_pr1,
    _extract_pv1,
    _extract_pv2,
    _extract_rxa,
    _extract_sch,
    _extract_spm,
    _extract_txa,
    _metadata,
    _split_messages,
)
from databricks.labs.community_connector.sources.hl7_v2.hl7_v2_parser import (
    HL7Message,
    HL7Segment,
    parse_message,
)
from databricks.labs.community_connector.sources.hl7_v2.hl7_v2_schemas import (
    SEGMENT_SCHEMAS,
    SEGMENT_TABLES,
    get_schema,
)

# Re-exports — preserve the historical import surface so tests and
# downstream callers can still do ``from .hl7_v2 import _extract_pid``,
# ``_split_messages``, etc.  Don't remove without auditing test imports.
__all__ = [
    "HL7V2LakeflowConnect",
    "_EXTRACTORS",
    "_SINGLE_SEGMENT_TABLES",
    "_extract_al1",
    "_extract_dg1",
    "_extract_evn",
    "_extract_ft1",
    "_extract_generic",
    "_extract_gt1",
    "_extract_iam",
    "_extract_in1",
    "_extract_msh",
    "_extract_mrg",
    "_extract_nk1",
    "_extract_nte",
    "_extract_obr",
    "_extract_obx",
    "_extract_orc",
    "_extract_pd1",
    "_extract_pid",
    "_extract_pr1",
    "_extract_pv1",
    "_extract_pv2",
    "_extract_rxa",
    "_extract_sch",
    "_extract_spm",
    "_extract_txa",
    "_split_messages",
]


_DEFAULT_WINDOW_SECONDS = 86_400
_RETRIABLE_STATUS_CODES = (429, 500, 503)
_MAX_RETRIES = 3
_INITIAL_BACKOFF = 1
_REQUEST_TIMEOUT = 30
_MAX_PAGE_SIZE = 1000


# ---------------------------------------------------------------------------
# Connector
# ---------------------------------------------------------------------------


class HL7V2LakeflowConnect(LakeflowConnect):
    """LakeflowConnect implementation for HL7 v2 messages.

    Supports two source modes controlled by the ``source_type`` option:

    * ``gcp`` (default) — fetches from a Google Cloud Healthcare API HL7v2 store.
    * ``volume`` — reads HL7 v2 files directly from a Unity Catalog Volume
      path FUSE-mounted on the executor.  No SDK, no HTTP, no auth options;
      permissions follow UC ``READ VOLUME`` grants on the configured Volume.

    Each HL7 segment type is a separate table.  Incremental loading is driven
    by ``createTime`` using a sliding time-window.

    GCP mode connection options:
        project_id, location, dataset_id, hl7v2_store_id, service_account_json

    Volume mode connection options:
        volume_path (str): Absolute UC Volume path containing HL7 files,
            e.g. ``/Volumes/cat/sch/vol/hl7_inbound/``.
        file_glob (str): Optional shell-style glob applied to file names
            (default ``"*"``).  Example: ``"*.hl7"``.
        recursive (str): Optional ``"true"``/``"false"`` flag for descending
            into subdirectories (default ``"false"``).

    Table options (both modes):
        segment_type (str): Override segment type for custom/Z-segments.
        window_seconds (str): Duration of the sliding time-window in seconds
            (default 86400).  Smaller values produce smaller batches.
        start_timestamp (str): RFC3339 timestamp to start reading from when no
            prior offset exists and auto-discovery is not possible.
        max_records_per_batch (str): Hard upper bound on rows yielded by a
            single ``read_table`` call.  Once this many output rows are
            produced, the iterator stops early and the cursor advances only
            up to the last source ``createTime`` actually consumed, so the
            next batch resumes from there.  Use this to bound memory when
            ``window_seconds`` is large or messages are dense.
    """

    _GCP_REQUIRED_KEYS = (
        "project_id",
        "location",
        "dataset_id",
        "hl7v2_store_id",
        "service_account_json",
    )

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        self._source_type = options.get("source_type", "gcp").lower()
        self._init_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        self._oldest_create_time: str | None = None

        if self._source_type == "volume":
            self._init_volume(options)
        elif self._source_type == "gcp":
            self._init_gcp(options)
        else:
            raise ValueError(
                f"Unsupported source_type '{self._source_type}'. "
                "Must be 'gcp' or 'volume'."
            )

    def _init_gcp(self, options: dict[str, str]) -> None:
        for key in self._GCP_REQUIRED_KEYS:
            if key not in options:
                raise ValueError(f"'{key}' is required in connector options for source_type 'gcp'.")

        self._base_url = (
            f"https://healthcare.googleapis.com/v1"
            f"/projects/{options['project_id']}"
            f"/locations/{options['location']}"
            f"/datasets/{options['dataset_id']}"
            f"/hl7V2Stores/{options['hl7v2_store_id']}/messages"
        )

        raw_sa = options["service_account_json"]
        sa_info = self._parse_service_account_json(raw_sa)
        self._creds = google_sa.Credentials.from_service_account_info(
            sa_info, scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        self._google_request = google_auth_requests.Request()
        self._session = requests.Session()
        self._creds.refresh(self._google_request)

    _VOLUME_REQUIRED_KEYS = ("volume_path",)

    @staticmethod
    def _parse_bool(value: str | bool | None, default: bool = False) -> bool:
        """Parse a UC connection option that arrives as ``"true"``/``"false"``."""
        if isinstance(value, bool):
            return value
        if value is None:
            return default
        return str(value).strip().lower() in ("1", "true", "yes", "y", "on")

    def _init_volume(self, options: dict[str, str]) -> None:
        for key in self._VOLUME_REQUIRED_KEYS:
            if key not in options:
                raise ValueError(
                    f"'{key}' is required in connector options for source_type 'volume'."
                )

        volume_path = options["volume_path"].strip()
        if not volume_path:
            raise ValueError(
                "'volume_path' must be a non-empty absolute UC Volume path, "
                "e.g. '/Volumes/cat/sch/vol/hl7_inbound/'."
            )
        # Allow either ``/Volumes/...`` (production) or any absolute path
        # (tests substitute a temp directory). Reject relative paths
        # explicitly — they almost always indicate a misconfiguration.
        if not os.path.isabs(volume_path):
            raise ValueError(
                f"'volume_path' must be an absolute path; got {volume_path!r}. "
                "Use a path like '/Volumes/cat/sch/vol/hl7_inbound/'."
            )

        self._volume_path = volume_path
        self._volume_file_glob = options.get("file_glob", "*") or "*"
        self._volume_recursive = self._parse_bool(options.get("recursive"), default=False)

    @staticmethod
    def _parse_service_account_json(raw: str | dict) -> dict:
        """Parse a service-account JSON value that may arrive in several forms.

        UC connection options can deliver the value as:
        * A ``dict`` (already parsed by the framework)
        * A well-formed JSON string
        * A double-serialised JSON string (``'"{\\"type\\":…}"'``)
        * A base64-encoded JSON string (some UI flows encode binary-like values)
        """
        if isinstance(raw, dict):
            return raw

        raw = raw.strip()

        # Attempt 1: direct parse
        try:
            parsed = json.loads(raw, strict=False)
            if isinstance(parsed, dict):
                return parsed
            # Double-serialised — json.loads returned a string, parse again
            if isinstance(parsed, str):
                inner = json.loads(parsed, strict=False)
                if isinstance(inner, dict):
                    return inner
        except (json.JSONDecodeError, TypeError):
            pass

        # Attempt 2: base64-decode then parse
        try:
            decoded = base64.b64decode(raw, validate=True).decode("utf-8")
            parsed = json.loads(decoded, strict=False)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            pass

        # Attempt 3: find a JSON object embedded in the string
        brace_start = raw.find("{")
        brace_end = raw.rfind("}")
        if brace_start != -1 and brace_end > brace_start:
            try:
                parsed = json.loads(raw[brace_start:brace_end + 1], strict=False)
                if isinstance(parsed, dict):
                    return parsed
            except json.JSONDecodeError:
                pass

        preview = raw[:120] + ("…" if len(raw) > 120 else "")
        raise ValueError(
            f"Could not parse 'service_account_json' as a JSON object. "
            f"Received value (first 120 chars): {preview!r}. "
            f"Ensure the entire contents of the GCP service account JSON key "
            f"file are pasted into the connection parameter — it should start "
            f"with '{{' and end with '}}'."
        )

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    def _get_headers(self) -> dict[str, str]:
        if not self._creds.valid:
            self._creds.refresh(self._google_request)
        return {"Authorization": f"Bearer {self._creds.token}"}

    def _api_get(self, params: dict[str, str]) -> dict:
        """GET the messages endpoint with retry on transient errors."""
        backoff = _INITIAL_BACKOFF
        last_resp = None
        for attempt in range(_MAX_RETRIES):
            resp = self._session.get(
                self._base_url,
                headers=self._get_headers(),
                params=params,
                timeout=_REQUEST_TIMEOUT,
            )
            last_resp = resp
            if resp.status_code not in _RETRIABLE_STATUS_CODES:
                resp.raise_for_status()
                return resp.json()
            if attempt < _MAX_RETRIES - 1:
                time.sleep(backoff)
                backoff *= 2
        last_resp.raise_for_status()
        return last_resp.json()

    # ------------------------------------------------------------------
    # LakeflowConnect interface
    # ------------------------------------------------------------------

    def list_tables(self) -> list[str]:
        return list(SEGMENT_TABLES)

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        self._validate_table(table_name, table_options)
        segment_type = table_options.get("segment_type", table_name)
        return get_schema(segment_type)

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        self._validate_table(table_name, table_options)
        segment_type = table_options.get("segment_type", table_name).lower()
        is_known = segment_type in SEGMENT_SCHEMAS
        is_single = segment_type in _SINGLE_SEGMENT_TABLES
        if is_known and not is_single:
            pks = ["message_id", "set_id"]
        else:
            pks = ["message_id"]
        return {
            "ingestion_type": "append",
            "cursor_field": "create_time",
            "primary_keys": pks,
        }

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Sliding time-window incremental read.

        Fetches all messages whose ``createTime`` falls in
        ``(since, since + window_seconds]``, parses them, and returns rows
        for the requested segment type.  The cursor advances to the window
        end regardless of whether data was found, ensuring forward progress.

        Works identically for both GCP and Volume source modes — only the
        fetch method differs.
        """
        self._validate_table(table_name, table_options)
        segment_type = table_options.get("segment_type", table_name).upper()

        since = start_offset.get("cursor") if start_offset is not None else None
        if not since:
            since = table_options.get("start_timestamp")
        if not since:
            since = self._peek_oldest_create_time()
        if not since:
            print(
                f"[HL7v2] read_table({table_name}): no start cursor resolved "
                f"(start_offset={start_offset}, source_type={self._source_type}). "
                f"Returning empty."
            )
            return iter([]), start_offset or {}

        if since >= self._init_ts:
            return iter([]), start_offset or {}

        window_seconds = int(table_options.get("window_seconds", str(_DEFAULT_WINDOW_SECONDS)))
        max_records_raw = table_options.get("max_records_per_batch")
        max_records = int(max_records_raw) if max_records_raw else None

        since_dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
        window_end_dt = since_dt + timedelta(seconds=window_seconds)
        window_end = min(
            window_end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            self._init_ts,
        )

        if self._source_type == "volume":
            api_messages = self._fetch_messages_from_volume(since, window_end)
        else:
            api_messages = self._fetch_messages_in_window(since, window_end)

        records = self._parse_api_messages(
            api_messages, segment_type, decode_base64=(self._source_type != "volume")
        )

        # Admission control: cap rows yielded per batch.  Cuts only at
        # message boundaries (one HL7 message can produce many rows when
        # the requested segment repeats — e.g. several OBX per ORU), so
        # rows from the same source message stay together.  The cursor
        # rewinds to the createTime of the last fully-consumed message;
        # the next batch resumes from there because the GCP / volume
        # filter is strict ``createTime > since``.
        if max_records is not None and len(records) > max_records:
            cut = max_records
            # Walk forward until create_time changes — keep all rows
            # belonging to the message that straddles ``max_records``.
            straddle_ct = records[cut - 1].get("create_time")
            while cut < len(records) and records[cut].get("create_time") == straddle_ct:
                cut += 1
            records = records[:cut]
            last_ct = records[-1].get("create_time") if records else None
            if last_ct and last_ct < window_end:
                end_offset = {"cursor": last_ct}
                if start_offset is not None and start_offset == end_offset:
                    return iter([]), start_offset
                return iter(records), end_offset

        end_offset = {"cursor": window_end}
        if start_offset is not None and start_offset == end_offset:
            return iter([]), start_offset

        return iter(records), end_offset

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _validate_table(self, table_name: str, table_options: dict) -> None:
        if table_name not in SEGMENT_TABLES and "segment_type" not in table_options:
            raise ValueError(
                f"Unknown table '{table_name}'. "
                f"Supported tables: {SEGMENT_TABLES}. "
                "For custom/Z-segments, provide 'segment_type' in table_options."
            )

    def _peek_oldest_create_time(self) -> str | None:
        """Auto-discover the earliest createTime by fetching the first message.

        The result is cached for the lifetime of this connector instance since
        the oldest message never changes once discovered.
        """
        if self._oldest_create_time is not None:
            return self._oldest_create_time

        if self._source_type == "volume":
            self._oldest_create_time = self._peek_oldest_create_time_volume()
            return self._oldest_create_time

        body = self._api_get({
            "view": "FULL",
            "pageSize": "1",
            "orderBy": "sendTime asc",
        })
        messages = body.get("hl7V2Messages", [])
        if messages:
            ts = messages[0].get("createTime")
            if ts:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                dt -= timedelta(seconds=1)
                ts = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            self._oldest_create_time = ts
        return self._oldest_create_time

    def _fetch_messages_in_window(self, since: str, until: str) -> list[dict]:
        """Fetch all API messages with createTime in (since, until]."""
        filter_str = f'createTime > "{since}" AND createTime <= "{until}"'
        messages: list[dict] = []
        page_token: str | None = None

        while True:
            params: dict[str, str] = {
                "view": "FULL",
                "pageSize": str(_MAX_PAGE_SIZE),
                "filter": filter_str,
                "orderBy": "sendTime asc",
            }
            if page_token:
                params["pageToken"] = page_token

            body = self._api_get(params)
            batch = body.get("hl7V2Messages", [])
            messages.extend(batch)

            page_token = body.get("nextPageToken")
            if not page_token:
                break

        return messages

    @staticmethod
    def _mtime_to_rfc3339(mtime: float) -> str:
        """Convert a POSIX mtime (seconds, float) to an RFC3339 UTC string.

        Truncated to whole seconds to match the GCP createTime cursor format
        (``yyyy-MM-ddTHH:mm:ssZ``) the rest of the connector assumes.
        """
        return datetime.fromtimestamp(int(mtime), tz=timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )

    def _iter_volume_files(self) -> Iterator[os.DirEntry]:
        """Yield ``DirEntry`` for every file under ``self._volume_path``.

        Honors ``self._volume_recursive`` and ``self._volume_file_glob``.  We
        use ``os.scandir`` (and recurse explicitly when asked) so listing
        scales without materializing the full directory in memory — important
        on busy HL7 inbound volumes.

        This relies on the Spark executor having Unity Catalog Volume FUSE
        access at ``/Volumes/...`` — the same runtime affordance the
        ``dicomweb`` connector uses to write DICOM files via plain
        ``open(..., "wb")``.  See
        ``sources/dicomweb/dicomweb.py::_attach_dicom_file`` for the write-
        side precedent.
        """
        glob = self._volume_file_glob

        def _walk(path: str) -> Iterator[os.DirEntry]:
            try:
                it = os.scandir(path)
            except FileNotFoundError:
                print(
                    f"[HL7v2 volume] volume_path not found: {path!r}. "
                    "Verify the path exists and the pipeline has READ VOLUME on it."
                )
                return
            with it as entries:
                for entry in entries:
                    try:
                        if entry.is_dir(follow_symlinks=False):
                            if self._volume_recursive:
                                yield from _walk(entry.path)
                            continue
                        if entry.is_file(follow_symlinks=False) and fnmatch(entry.name, glob):
                            yield entry
                    except OSError:
                        # Skip entries that disappear or become unreadable
                        # mid-scan — common when files are written into the
                        # volume concurrently with the connector running.
                        continue

        yield from _walk(self._volume_path)

    def _fetch_messages_from_volume(self, since: str, until: str) -> list[dict]:
        """Fetch HL7 messages whose file mtime falls in ``(since, until]``.

        Returns a list of dicts with the same shape as the GCP API response
        (``data``, ``createTime``, ``name``) so ``_parse_api_messages`` works
        unchanged.  ``data`` carries the raw HL7 text (not base64); see the
        ``decode_base64`` flag at the call site.
        """
        messages: list[dict] = []
        for entry in self._iter_volume_files():
            try:
                stat = entry.stat()
            except OSError:
                continue
            mtime_iso = self._mtime_to_rfc3339(stat.st_mtime)
            if not (since < mtime_iso <= until):
                continue
            try:
                with open(entry.path, "r", encoding="utf-8", errors="replace") as f:
                    raw = f.read()
            except OSError as exc:
                print(f"[HL7v2 volume] Skipping unreadable file {entry.path!r}: {exc}")
                continue
            if not raw:
                continue
            messages.append({
                "data": raw,
                "createTime": mtime_iso,
                # The HL7 wire-level sendTime lives inside MSH-7 (extracted
                # downstream into ``message_timestamp``); for files on a
                # Volume we use the mtime as the surrogate ``send_time`` so
                # the metadata column is populated rather than empty.
                "sendTime": mtime_iso,
                "name": entry.path,
            })
        # Sort deterministically so per-batch ordering matches GCP mode
        # (which sorts by sendTime asc); ties are broken by path for
        # stability under same-second mtimes.
        messages.sort(key=lambda m: (m["createTime"], m["name"]))
        return messages

    def _peek_oldest_create_time_volume(self) -> str | None:
        """Return the earliest file mtime in the volume as an RFC3339 string.

        Returns the cursor *one second before* the oldest file so the
        strict-greater-than filter (``createTime > since``) still includes it,
        matching the GCP peek semantics.
        """
        oldest: float | None = None
        for entry in self._iter_volume_files():
            try:
                mtime = entry.stat().st_mtime
            except OSError:
                continue
            if oldest is None or mtime < oldest:
                oldest = mtime
        if oldest is None:
            return None
        dt = datetime.fromtimestamp(int(oldest), tz=timezone.utc) - timedelta(seconds=1)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    def _parse_api_messages(
        self, api_messages: list[dict], segment_type: str, *, decode_base64: bool = True
    ) -> list[dict]:
        """Decode, parse, and extract rows from message payloads.

        Args:
            decode_base64: When True (GCP mode), the ``data`` field is
                base64-decoded.  When False (volume mode), ``data`` is
                treated as raw HL7 text.
        """
        records: list[dict] = []

        for msg_data in api_messages:
            send_time = msg_data.get("sendTime", "")
            create_time = msg_data.get("createTime", "") or send_time
            raw_data = msg_data.get("data", "")
            if not raw_data:
                continue
            if decode_base64:
                raw_hl7 = base64.b64decode(raw_data).decode("utf-8", errors="replace")
            else:
                raw_hl7 = raw_data
            source_name = msg_data.get("name", "")

            for msg_text in _split_messages(raw_hl7):
                msg: HL7Message | None = parse_message(msg_text)
                if msg is None:
                    continue
                msh = msg.get_segment("MSH")
                meta = _metadata(msh, source_name, send_time, create_time)

                if segment_type == "MSH":
                    if msh is not None:
                        records.append(meta | _extract_msh(msh) | {"raw_segment": msh.raw_line})
                else:
                    extractor = _EXTRACTORS.get(segment_type.lower(), _extract_generic)
                    is_multi_segment = segment_type.lower() not in _SINGLE_SEGMENT_TABLES
                    for idx, seg in enumerate(msg.get_segments(segment_type), start=1):
                        row = meta | extractor(seg) | {"raw_segment": seg.raw_line}
                        if is_multi_segment and "set_id" not in row:
                            row["set_id"] = idx
                        records.append(row)

        return records
