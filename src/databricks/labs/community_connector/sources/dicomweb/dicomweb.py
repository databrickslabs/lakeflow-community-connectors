"""
DICOMwebLakeflowConnect — main Lakeflow Community Connector class.

Exposes three tables from any DICOMweb-compliant VNA/PACS system:
    studies    QIDO-RS /studies                                 (flat pagination)
    series     QIDO-RS /studies/{uid}/series                    (hierarchical)
    instances  QIDO-RS /studies/{uid}/series/{uid}/instances    (hierarchical)

Incremental strategy
--------------------
Cursor = ISO date string YYYYMMDD tracking the max study_date seen.
QIDO-RS filter: StudyDate={cursor_date minus lookback_days}-{today}
Offset format:  {"study_date": "20231215", "page_offset": 0}

page_offset tracks the study-level QIDO-RS offset within a trigger run.
When max_records_per_batch is hit mid-scan, page_offset preserves the
position so the next microbatch resumes from where it left off.

The series and instances tables use *hierarchical* QIDO-RS endpoints that
are required by servers such as the AWS S3 Static WADO Server.  Studies are
enumerated first (flat, paginated), then series/instances are fetched per
parent UID.

WADO-RS file retrieval (fetch_dicom_files=true)
-----------------------------------------------
Standard servers return a full .dcm file:
    GET /studies/{uid}/series/{uid}/instances/{uid}

Frame-based servers (e.g. AWS S3 Static WADO) return individual image frames:
    GET /studies/{uid}/series/{uid}/instances/{uid}/frames/1

Set wado_mode=auto (default) to auto-detect: the connector tries the full
instance endpoint first; if it returns 404/406/415 it falls back to frames
and caches the detection for the rest of the run.  Set wado_mode=full or
wado_mode=frames to force a specific mode.

DICOM metadata (fetch_metadata=true)
--------------------------------------
When enabled, the connector fetches full DICOM JSON metadata for each series
via the WADO-RS metadata endpoint:
    GET /studies/{uid}/series/{uid}/metadata

The complete JSON object for each instance is stored in the `metadata` column
(VariantType on DBR 15.x+, JSON string on older runtimes).

Usage (Databricks notebook)
---------------------------
    from databricks.labs.community_connector.sources.dicomweb import DICOMwebLakeflowConnect
    connector = DICOMwebLakeflowConnect({
        "base_url": "https://orthanc.uclouvain.be/demo/dicom-web",
        "auth_type": "none",
    })
    records, next_offset = connector.read_table("studies", {}, {})
"""

from __future__ import annotations

import json
import logging
import pathlib
from datetime import date, datetime, timedelta, timezone
from typing import Iterator

from pyspark.sql.types import StructType
from urllib.error import HTTPError

from databricks.labs.community_connector.interface.lakeflow_connect import LakeflowConnect
from databricks.labs.community_connector.sources.dicomweb.dicomweb_client import DICOMwebClient
from databricks.labs.community_connector.sources.dicomweb.dicomweb_parser import (
    parse_instance,
    parse_series,
    parse_study,
)
from databricks.labs.community_connector.sources.dicomweb.dicomweb_schemas import (
    get_schema,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SUPPORTED_TABLES = ("studies", "series", "instances", "diagnostics")
DEFAULT_START_DATE = "19000101"  # Effectively "all history" on first run
DEFAULT_PAGE_SIZE = 100
DEFAULT_LOOKBACK_DAYS = 1
DEFAULT_MAX_RECORDS_PER_BATCH = 500

# WADO-RS retrieval mode
WADO_MODE_AUTO = "auto"
WADO_MODE_FULL = "full"
WADO_MODE_FRAMES = "frames"


# ---------------------------------------------------------------------------
# Main connector
# ---------------------------------------------------------------------------


class DICOMwebLakeflowConnect(LakeflowConnect):
    """Lakeflow connector for DICOMweb VNA/PACS systems."""

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)

        base_url = options.get("base_url")
        if not base_url:
            raise ValueError("Connection option 'base_url' is required")

        self._client = DICOMwebClient(
            base_url=base_url,
            auth_type=options.get("auth_type", "none"),
            username=options.get("username"),
            password=options.get("password"),
            token=options.get("token"),
        )

        # Lineage identifier injected into every record.
        # Defaults to base_url so records are always traceable even when
        # the connection_name option is not explicitly set.
        self._connection_name: str = options.get("connection_name") or base_url

        # Cached WADO-RS mode detected at runtime (only used when wado_mode=auto)
        self._wado_mode_detected: str | None = None

        # Cap cursors at init time so a trigger never chases new data.
        # The next trigger creates a fresh instance and picks up from here.
        self._init_ts: str = date.today().strftime("%Y%m%d")

    # ------------------------------------------------------------------
    # Schema / metadata
    # ------------------------------------------------------------------

    def list_tables(self) -> list[str]:
        return list(SUPPORTED_TABLES)

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        return get_schema(table_name)  # raises ValueError for unknown tables

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        if table_name not in SUPPORTED_TABLES:
            raise ValueError(f"Unknown table '{table_name}'. Valid: {SUPPORTED_TABLES}")
        if table_name == "diagnostics":
            return {
                "primary_keys": ["endpoint"],
                "cursor_field": "probe_timestamp",
                "ingestion_type": "cdc",
            }
        return {
            "primary_keys": [_primary_key(table_name)],
            "cursor_field": "study_date",
            "ingestion_type": "cdc",
        }

    # ------------------------------------------------------------------
    # Data reading
    # ------------------------------------------------------------------

    def read_table(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """
        Incrementally read records from the given table.

        Args:
            table_name:   One of "studies", "series", "instances", "diagnostics".
            start_offset: Previous offset dict, or {} for first run.
            table_options: Per-table options from the pipeline spec.

        Returns:
            (record_iterator, next_offset_dict)
        """
        if table_name not in SUPPORTED_TABLES:
            raise ValueError(f"Unknown table '{table_name}'. Valid: {SUPPORTED_TABLES}")

        # Diagnostics table: runs a capability probe on every trigger, no cursor
        if table_name == "diagnostics":
            probe_iter = self._run_diagnostics_probe()
            next_offset = {"probe_timestamp": datetime.now(tz=timezone.utc).isoformat()}
            return probe_iter, next_offset

        start_offset = start_offset or {}
        date_cursor = start_offset.get("study_date", DEFAULT_START_DATE)
        page_offset = start_offset.get("page_offset", 0)

        # Short-circuit: cursor is at or past today — stream has caught up to init time.
        # The next trigger will create a fresh instance with a new _init_ts.
        if start_offset and date_cursor >= self._init_ts:
            return iter([]), start_offset

        page_size = int(table_options.get("page_size", DEFAULT_PAGE_SIZE))
        max_records = int(table_options.get("max_records_per_batch", str(DEFAULT_MAX_RECORDS_PER_BATCH)))
        lookback_days = int(table_options.get("lookback_days", DEFAULT_LOOKBACK_DAYS))

        today_str = date.today().strftime("%Y%m%d")
        effective_start = _subtract_days(date_cursor, lookback_days)
        date_range = f"{effective_start}-{today_str}"

        logger.info(
            "read_table table=%s date_range=%s page_offset=%d page_size=%d max_records=%d",
            table_name,
            date_range,
            page_offset,
            page_size,
            max_records,
        )

        fetch_files = table_options.get("fetch_dicom_files", "false").lower() == "true"
        volume_path = table_options.get("dicom_volume_path", "")
        fetch_metadata = table_options.get("fetch_metadata", "false").lower() == "true"
        wado_mode = table_options.get("wado_mode", WADO_MODE_AUTO).lower()

        if fetch_files and not volume_path and table_name == "instances":
            raise ValueError("fetch_dicom_files=true requires dicom_volume_path to be set")

        if table_name == "studies":
            records, next_page_offset = self._collect_studies(date_range, page_size, page_offset, max_records)
        elif table_name == "series":
            records, next_page_offset = self._collect_series(date_range, page_size, page_offset, max_records)
        else:
            records, next_page_offset = self._collect_instances(
                date_range, page_size, page_offset, max_records,
                fetch_files, volume_path, fetch_metadata, wado_mode,
            )

        if not records:
            end_offset = {"study_date": today_str, "page_offset": 0}
            if start_offset == end_offset:
                return iter([]), start_offset
            return iter([]), end_offset

        end_offset = {"study_date": today_str, "page_offset": next_page_offset}
        if start_offset == end_offset:
            return iter([]), start_offset
        return iter(records), end_offset

    # ------------------------------------------------------------------
    # Eager record collection (supports max_records_per_batch)
    # ------------------------------------------------------------------

    def _collect_studies(
        self, date_range: str, page_size: int, start_offset: int, max_records: int
    ) -> tuple[list[dict], int]:
        """Collect up to max_records studies via flat QIDO-RS pagination.

        Returns (records, next_page_offset) where next_page_offset is 0 when
        all pages are exhausted, or the study-level offset to resume from when
        the batch limit is hit mid-scan.
        """
        records: list[dict] = []
        offset = start_offset
        while len(records) < max_records:
            batch = self._client.query_studies(date_range, limit=page_size, offset=offset)
            if not batch:
                return records, 0
            for raw in batch:
                record = parse_study(raw)
                record["connection_name"] = self._connection_name
                records.append(record)
            if len(batch) < page_size:
                return records, 0  # last partial page — all data consumed
            offset += page_size
        return records, offset  # hit batch limit; next call resumes from this offset

    def _collect_series(
        self, date_range: str, page_size: int, start_offset: int, max_records: int
    ) -> tuple[list[dict], int]:
        """Hierarchical series collection: enumerate studies, then fetch series per study.

        Returns (records, next_study_page_offset).
        """
        records: list[dict] = []
        study_offset = start_offset
        while len(records) < max_records:
            studies = self._client.query_studies(date_range, limit=page_size, offset=study_offset)
            if not studies:
                return records, 0
            for study_raw in studies:
                study = parse_study(study_raw)
                study_uid = study.get("study_instance_uid")
                if not study_uid:
                    continue
                for series_raw in self._client.query_series_for_study(study_uid):
                    record = parse_series(series_raw)
                    # Propagate study_date / study_instance_uid from parent if missing
                    if not record.get("study_date"):
                        record["study_date"] = study.get("study_date")
                    if not record.get("study_instance_uid"):
                        record["study_instance_uid"] = study_uid
                    record["connection_name"] = self._connection_name
                    records.append(record)
            if len(studies) < page_size:
                return records, 0
            study_offset += page_size
        return records, study_offset

    def _collect_instances(
        self,
        date_range: str,
        page_size: int,
        start_offset: int,
        max_records: int,
        fetch_files: bool,
        volume_path: str,
        fetch_metadata: bool,
        wado_mode: str,
    ) -> tuple[list[dict], int]:
        """Hierarchical instances collection: enumerate studies → series → instances.

        Optionally fetches DICOM JSON metadata (fetch_metadata=true) and writes
        DICOM files to a UC Volume (fetch_dicom_files=true).

        Returns (records, next_study_page_offset).
        """
        records: list[dict] = []
        study_offset = start_offset
        while len(records) < max_records:
            studies = self._client.query_studies(date_range, limit=page_size, offset=study_offset)
            if not studies:
                return records, 0
            for study_raw in studies:
                study = parse_study(study_raw)
                study_uid = study.get("study_instance_uid")
                if not study_uid:
                    continue
                for series_raw in self._client.query_series_for_study(study_uid):
                    series = parse_series(series_raw)
                    series_uid = series.get("series_instance_uid")
                    if not series_uid:
                        continue

                    instances_raw = self._client.query_instances_for_series(study_uid, series_uid)

                    # Optionally fetch full DICOM JSON for all instances in this series
                    sop_to_meta: dict[str, str] = {}
                    if fetch_metadata:
                        sop_to_meta = self._build_metadata_map(study_uid, series_uid)

                    for inst_raw in instances_raw:
                        record = parse_instance(inst_raw)
                        # Propagate parent UIDs / dates if missing in the response
                        if not record.get("study_date"):
                            record["study_date"] = study.get("study_date")
                        if not record.get("study_instance_uid"):
                            record["study_instance_uid"] = study_uid
                        if not record.get("series_instance_uid"):
                            record["series_instance_uid"] = series_uid
                        # Attach full DICOM JSON metadata
                        if fetch_metadata:
                            sop_uid = record.get("sop_instance_uid")
                            record["metadata"] = sop_to_meta.get(sop_uid) if sop_uid else None
                        # Attach DICOM file or frame
                        if fetch_files:
                            record = self._attach_dicom_file(record, volume_path, wado_mode)
                        record["connection_name"] = self._connection_name
                        records.append(record)
            if len(studies) < page_size:
                return records, 0
            study_offset += page_size
        return records, study_offset

    # ------------------------------------------------------------------
    # WADO-RS helpers
    # ------------------------------------------------------------------

    def _attach_dicom_file(self, record: dict, volume_path: str, wado_mode: str) -> dict:
        """
        Retrieve the DICOM content via WADO-RS and write it to the Volume.

        Supports two retrieval modes:
        - full  (wado_mode=full):   GET .../instances/{uid}        → .dcm
        - frames (wado_mode=frames): GET .../instances/{uid}/frames/1 → .jpg

        When wado_mode=auto (default), the connector tries the full endpoint
        first.  If the server responds with 404/406/415 it switches to frame
        retrieval and caches the detected mode for the rest of the run.
        """
        study_uid = record.get("study_instance_uid")
        series_uid = record.get("series_instance_uid")
        sop_uid = record.get("sop_instance_uid")

        if not all([study_uid, series_uid, sop_uid]):
            logger.warning("Skipping WADO-RS: missing UIDs in record %s", record)
            return record

        try:
            effective_mode = self._resolve_wado_mode(wado_mode)
            if effective_mode == WADO_MODE_FRAMES:
                file_bytes = self._client.retrieve_instance_frames(study_uid, series_uid, sop_uid)
                ext = ".jpg"
            else:
                # Try full DICOM retrieval; auto-detect fallback to frames on error
                try:
                    file_bytes = self._client.retrieve_instance(study_uid, series_uid, sop_uid)
                    ext = ".dcm"
                    if wado_mode == WADO_MODE_AUTO and self._wado_mode_detected is None:
                        self._wado_mode_detected = WADO_MODE_FULL
                        logger.info("WADO-RS auto-detected: full DICOM retrieval")
                except HTTPError as exc:
                    if (
                        wado_mode == WADO_MODE_AUTO
                        and exc.code in (404, 406, 415)
                    ):
                        logger.info(
                            "WADO-RS full instance returned HTTP %d — auto-switching to frame retrieval",
                            exc.code,
                        )
                        self._wado_mode_detected = WADO_MODE_FRAMES
                        file_bytes = self._client.retrieve_instance_frames(study_uid, series_uid, sop_uid)
                        ext = ".jpg"
                    else:
                        raise

            dest_path = pathlib.Path(volume_path) / study_uid / series_uid / f"{sop_uid}{ext}"
            try:
                dest_path.parent.mkdir(parents=True, exist_ok=True)
            except OSError:
                # UC Volume FUSE mounts do not support mkdir via POSIX syscalls;
                # the write_bytes() call below works regardless.
                pass
            dest_path.write_bytes(file_bytes)
            record["dicom_file_path"] = str(dest_path)
            logger.debug("Wrote %d bytes → %s", len(file_bytes), dest_path)
        except Exception as exc:
            logger.error("WADO-RS retrieval failed for %s: %s", sop_uid, exc)
            record["dicom_file_path"] = None

        return record

    def _resolve_wado_mode(self, wado_mode: str) -> str:
        """Return the effective WADO mode, consulting the cached detection result."""
        if wado_mode == WADO_MODE_AUTO:
            return self._wado_mode_detected or WADO_MODE_FULL
        return wado_mode

    def _build_metadata_map(self, study_uid: str, series_uid: str) -> dict[str, str]:
        """
        Fetch WADO-RS series metadata and return a {sop_instance_uid: value} map.

        On DBR 15.x+ (METADATA_IS_VARIANT=True) values are VariantVal objects;
        on older runtimes they are JSON strings.  Returns an empty dict on any
        error so the instance record is still yielded without metadata.
        """
        try:
            meta_list = self._client.retrieve_series_metadata(study_uid, series_uid)
            sop_to_meta: dict[str, str] = {}
            for meta_obj in meta_list:
                tag_obj = meta_obj.get("00080018")  # sop_instance_uid tag
                if tag_obj and tag_obj.get("Value"):
                    sop_uid = str(tag_obj["Value"][0])
                    # VariantType: pass the Python dict — Spark's convert_variant
                    # handles dict → VARIANT binary encoding internally.
                    # StringType fallback: pass a JSON string for older runtimes.
                    sop_to_meta[sop_uid] = json.dumps(meta_obj)
            return sop_to_meta
        except Exception as exc:
            logger.warning("Failed to fetch series metadata %s/%s: %s", study_uid, series_uid, exc)
            return {}

    def _run_diagnostics_probe(self) -> Iterator[dict]:
        """
        Probe all standard DICOMweb endpoints and yield one record per endpoint.

        Automatically discovers sample UIDs (study → series → instance) from the
        server so that hierarchical endpoints can be tested with real paths.
        Endpoints are never mutated — only GET requests are issued.
        """
        probe_timestamp = datetime.now(tz=timezone.utc).isoformat()

        # ---- Discover sample UIDs for hierarchical probes -----------------
        study_uid = series_uid = sop_uid = None
        try:
            studies = self._client.query_studies("19000101-99991231", limit=1, offset=0)
            if studies:
                study_uid = parse_study(studies[0]).get("study_instance_uid")
        except Exception as exc:
            logger.warning("Diagnostics: could not fetch a study UID: %s", exc)

        if study_uid:
            try:
                series_list = self._client.query_series_for_study(study_uid)
                if series_list:
                    series_uid = parse_series(series_list[0]).get("series_instance_uid")
            except Exception as exc:
                logger.warning("Diagnostics: could not fetch a series UID: %s", exc)

        if study_uid and series_uid:
            try:
                instances = self._client.query_instances_for_series(study_uid, series_uid)
                if instances:
                    sop_uid = parse_instance(instances[0]).get("sop_instance_uid")
            except Exception as exc:
                logger.warning("Diagnostics: could not fetch a SOP UID: %s", exc)

        # ---- Probe definitions --------------------------------------------
        # Each entry: (endpoint_pattern, resolved_path_or_None, category, description, accept)
        _probes: list[tuple[str, str | None, str, str, str | None]] = [
            # QIDO-RS — query
            (
                "/studies",
                "/studies?limit=1",
                "QIDO-RS",
                "Search studies (flat pagination)",
                None,
            ),
            (
                "/studies/{uid}/series",
                f"/studies/{study_uid}/series" if study_uid else None,
                "QIDO-RS",
                "Search series for a study (hierarchical)",
                None,
            ),
            (
                "/studies/{uid}/series/{uid}/instances",
                f"/studies/{study_uid}/series/{series_uid}/instances" if study_uid and series_uid else None,
                "QIDO-RS",
                "Search instances for a series (hierarchical)",
                None,
            ),
            # WADO-RS — metadata
            (
                "/studies/{uid}/series/{uid}/metadata",
                f"/studies/{study_uid}/series/{series_uid}/metadata" if study_uid and series_uid else None,
                "WADO-RS",
                "Series metadata — full DICOM JSON for all instances in a series",
                "application/dicom+json",
            ),
            (
                "/studies/{uid}/series/{uid}/instances/{uid}/metadata",
                (
                    f"/studies/{study_uid}/series/{series_uid}/instances/{sop_uid}/metadata"
                    if study_uid and series_uid and sop_uid
                    else None
                ),
                "WADO-RS",
                "Instance metadata — full DICOM JSON for a single instance",
                "application/dicom+json",
            ),
            # WADO-RS — retrieve
            (
                "/studies/{uid}/series/{uid}/instances/{uid}",
                (
                    f"/studies/{study_uid}/series/{series_uid}/instances/{sop_uid}"
                    if study_uid and series_uid and sop_uid
                    else None
                ),
                "WADO-RS",
                "Retrieve full DICOM instance (.dcm, multipart/related)",
                'multipart/related; type="application/dicom"',
            ),
            (
                "/studies/{uid}/series/{uid}/instances/{uid}/frames/{n}",
                (
                    f"/studies/{study_uid}/series/{series_uid}/instances/{sop_uid}/frames/1"
                    if study_uid and series_uid and sop_uid
                    else None
                ),
                "WADO-RS",
                "Retrieve pixel frame (image/jpeg or image/jls)",
                "image/jpeg, image/jls, application/octet-stream",
            ),
            (
                "/studies/{uid}/series/{uid}/instances/{uid}/rendered",
                (
                    f"/studies/{study_uid}/series/{series_uid}/instances/{sop_uid}/rendered"
                    if study_uid and series_uid and sop_uid
                    else None
                ),
                "WADO-RS",
                "Retrieve rendered instance (viewport-ready PNG/JPEG)",
                "image/jpeg, image/png",
            ),
            (
                "/studies/{uid}/series/{uid}/rendered",
                f"/studies/{study_uid}/series/{series_uid}/rendered" if study_uid and series_uid else None,
                "WADO-RS",
                "Retrieve rendered series (all frames as viewport-ready images)",
                "image/jpeg, image/png",
            ),
            (
                "/studies/{uid}",
                f"/studies/{study_uid}" if study_uid else None,
                "WADO-RS",
                "Retrieve entire study (all instances, multipart/related)",
                'multipart/related; type="application/dicom"',
            ),
        ]

        for endpoint_pattern, path, category, description, accept in _probes:
            if path is None:
                yield {
                    "endpoint": endpoint_pattern,
                    "category": category,
                    "description": description,
                    "supported": "unknown",
                    "status_code": None,
                    "content_type": None,
                    "latency_ms": None,
                    "notes": "Could not probe — no sample UID available from the server",
                    "probe_timestamp": probe_timestamp,
                    "connection_name": self._connection_name,
                }
                continue

            result = self._client.probe_endpoint(path, accept=accept)
            status = result["status_code"]

            if result["error"]:
                supported = "error"
                notes = result["error"]
            elif status in (200, 204, 206):
                supported = "yes"
                notes = f"Content-Type: {result['content_type']}"
            elif status == 400:
                supported = "partial"
                notes = "Bad Request (400) — endpoint exists but query parameters may be required"
            elif status == 403:
                supported = "no"
                notes = "Access Denied (403) — endpoint blocked by server or CDN policy"
            elif status == 404:
                supported = "no"
                notes = "Not Found (404) — endpoint not implemented on this server"
            elif status == 406:
                supported = "no"
                notes = "Not Acceptable (406) — requested media type not supported"
            else:
                supported = "no"
                notes = f"HTTP {status}"

            yield {
                "endpoint": endpoint_pattern,
                "category": category,
                "description": description,
                "supported": supported,
                "status_code": status,
                "content_type": result["content_type"],
                "latency_ms": result["latency_ms"],
                "notes": notes,
                "probe_timestamp": probe_timestamp,
                "connection_name": self._connection_name,
            }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _primary_key(table_name: str) -> str:
    pk_map = {
        "studies": "study_instance_uid",
        "series": "series_instance_uid",
        "instances": "sop_instance_uid",
        "diagnostics": "endpoint",
    }
    return pk_map[table_name]


def _subtract_days(date_str: str, days: int) -> str:
    """Subtract `days` from a YYYYMMDD date string; clamp at DEFAULT_START_DATE."""
    if date_str == DEFAULT_START_DATE or days == 0:
        return date_str
    try:
        d = date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))
        d = d - timedelta(days=days)
        return d.strftime("%Y%m%d")
    except (ValueError, IndexError):
        return date_str
