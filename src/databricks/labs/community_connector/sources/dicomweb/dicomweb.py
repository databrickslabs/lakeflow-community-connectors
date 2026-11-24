"""
DICOMwebLakeflowConnect — main Lakeflow Community Connector class.

Implements ``LakeflowConnect`` + ``SupportsPartitionedStream`` for DICOMweb
VNA/PACS systems.

Tables
------
    studies      QIDO-RS /studies                                 (flat pagination)
    series       QIDO-RS /studies/{uid}/series                    (hierarchical)
    instances    QIDO-RS /studies/{uid}/series/{uid}/instances    (hierarchical)
    diagnostics  Capability probe of all QIDO-RS / WADO-RS endpoints

studies, series, and instances use the partitioned streaming path
(``SupportsPartitionedStream``).  diagnostics uses ``simpleStreamReader``
via ``read_table``.

Streaming strategy (partitioned path)
-------------------------------------
Offset = ``{"study_date": "YYYYMMDD"}``.  ``latest_offset`` returns
today's date; Spark advances the start offset each micro-batch.

``get_partitions`` runs on the driver and decides how the work for one
micro-batch (or batch read) is split across executors.  Granularity
varies by table to match the QIDO-RS hierarchy:

* ``studies`` — single partition.  QIDO-RS ``/studies`` is a flat,
  paginated stream; sharding by sub-window would require a separate
  date-range planner and is left as future work.
* ``series``  — bin-pack studies across ``num_partitions`` bins,
  weighted by ``number_of_study_related_series``.  Each task fetches
  ``/studies/{uid}/series`` for its assigned studies in parallel.
* ``instances`` — discover the full ``(study_uid, series_uid)`` set on
  the driver, then bin-pack at the **series** level (uniform weight)
  across ``num_partitions`` bins.  This gives finer parallelism than
  study-level binning whenever the dataset has more series than
  studies, which is the common case for CT/MR datasets.  Each task
  fetches ``/studies/{uid}/series/{uid}/instances`` for its assigned
  series and runs WADO-RS retrieval/metadata fetch inline.

``read_partition`` runs on executors and fetches data via QIDO-RS,
optionally downloading DICOM files (WADO-RS) and metadata.

WADO-RS file retrieval (fetch_dicom_files=true)
-----------------------------------------------
wado_mode=auto (default) tries the full instance endpoint first; if the
server returns 404/406/415 it falls back to frame retrieval.

``max_concurrent_downloads`` (default 8) controls a per-partition
thread pool that parallelises WADO-RS retrieval inside a single Spark
task.  This is on top of inter-partition parallelism from
``num_partitions``: with N partitions and M concurrent downloads each,
total in-flight WADO-RS requests are up to ``N × M``. Tune both
together to respect the source server's rate limits; set to ``1`` to
disable intra-task parallelism for rate-limited or fragile servers.

DICOM metadata (fetch_metadata=true)
--------------------------------------
Fetches full DICOM JSON per series via WADO-RS metadata endpoint and
stores it in the ``metadata`` column (VariantType).

QIDO-RS query filters
---------------------
Three optional table_options accept a JSON-encoded dict whose keys/
values are merged verbatim into the QIDO-RS query string at the
matching hierarchy level:

* ``study_qido_filters``    → ``/studies``
* ``series_qido_filters``   → ``/studies/{uid}/series``
* ``instance_qido_filters`` → ``/studies/{uid}/series/{uid}/instances``

Anything DICOMweb / QIDO-RS accepts is passable: matching attributes
(``ModalitiesInStudy``, ``Modality``, ``PatientID``, ``AccessionNumber``,
``BodyPartExamined``, ...), behaviour flags (``fuzzymatching=true``),
extra fields (``includefield=...``), or server-specific extensions.
Multi-valued matching is supported by passing a JSON list (rendered as
repeated query params, e.g. ``Modality=CT&Modality=MR``).

Filters are independent and additive across levels — use them together
to shrink the working set as early as possible (the driver-side
enumeration of studies/series for the ``instances`` table benefits from
``study_qido_filters`` and ``series_qido_filters`` as well).

Reserved keys (``StudyDate``, ``limit``, ``offset``) are managed by the
connector and rejected with a clear error if present in any filter
dict, since overriding them would silently break the cursor or paging.
"""

import json
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from typing import Iterator
from urllib.error import HTTPError

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface.lakeflow_connect import LakeflowConnect
from databricks.labs.community_connector.interface.supports_partition import (
    SupportsPartitionedStream,
)
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
DEFAULT_NUM_PARTITIONS = 8
DEFAULT_MAX_CONCURRENT_DOWNLOADS = 8

# WADO-RS retrieval mode
WADO_MODE_AUTO = "auto"
WADO_MODE_FULL = "full"
WADO_MODE_FRAMES = "frames"

_DICOM_UID_RE = re.compile(r"^[0-9.]+$")

# QIDO-RS query parameters managed by the connector itself; user-supplied
# qido_filters dicts are rejected if they contain any of these keys (matched
# case-insensitively) to prevent silent overrides of the cursor / paging glue.
_RESERVED_QIDO_KEYS = frozenset({"studydate", "limit", "offset"})

# Per-table option names that hold JSON-encoded filter dicts.
_QIDO_FILTER_OPTIONS = (
    "study_qido_filters",
    "series_qido_filters",
    "instance_qido_filters",
)


def _parse_qido_filters(table_options: dict[str, str], option_name: str) -> dict:
    """Parse a JSON-encoded ``qido_filters`` option into a dict.

    Returns ``{}`` when the option is absent or empty. Raises ``ValueError``
    with an actionable message if the option is malformed JSON, the parsed
    value is not a dict, or contains a reserved key.

    Reserved keys (``StudyDate``, ``limit``, ``offset``) are managed by the
    connector — letting the user override them would silently break the
    streaming cursor or pagination.
    """
    if option_name not in _QIDO_FILTER_OPTIONS:
        raise AssertionError(f"Unknown qido_filters option name: {option_name}")

    raw = table_options.get(option_name, "").strip()
    if not raw:
        return {}

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"table_options['{option_name}'] must be a JSON object, "
            f"got malformed JSON: {exc.msg} (at column {exc.colno}). "
            f"Example: '{{\"Modality\": \"CT\"}}'"
        ) from exc

    if not isinstance(parsed, dict):
        raise ValueError(
            f"table_options['{option_name}'] must be a JSON object "
            f"(got {type(parsed).__name__}). "
            f"Example: '{{\"Modality\": \"CT\"}}'"
        )

    reserved_hits = [k for k in parsed if k.lower() in _RESERVED_QIDO_KEYS]
    if reserved_hits:
        raise ValueError(
            f"table_options['{option_name}'] cannot override connector-managed "
            f"QIDO-RS parameters: {sorted(reserved_hits)}. "
            f"Reserved keys (case-insensitive): {sorted(_RESERVED_QIDO_KEYS)}. "
            f"StudyDate is set via 'starting_date'/'window_days'; limit/offset "
            f"are paging cursors managed by 'page_size'."
        )

    return parsed


def _is_valid_dicom_uid(uid: object) -> bool:
    """Return True if uid is a non-empty string of digits and dots only.

    DICOM UIDs are constrained by PS3.5 §9 to digits and dots. The check
    rejects path-traversal payloads (``..``, absolute paths, separators)
    before any UID is joined into a Volume filesystem path.
    """
    return isinstance(uid, str) and bool(_DICOM_UID_RE.match(uid)) and ".." not in uid


# ---------------------------------------------------------------------------
# Main connector
# ---------------------------------------------------------------------------


class DICOMwebLakeflowConnect(LakeflowConnect, SupportsPartitionedStream):
    """Lakeflow connector for DICOMweb VNA/PACS systems."""

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)

        base_url = options.get("base_url")
        if not base_url:
            raise ValueError("Connection option 'base_url' is required")

        self._client = DICOMwebClient(
            base_url=base_url,
            auth={
                "username": options.get("username"),
                "password": options.get("password"),
                "token": options.get("token"),
            },
        )

        # Lineage identifier injected into every record.
        # Defaults to base_url so records are always traceable even when
        # the connection_name option is not explicitly set.
        self._connection_name: str = options.get("connection_name") or base_url

        # Cached WADO-RS mode detected at runtime (only used when wado_mode=auto)
        self._wado_mode_detected: str | None = None

        # Freeze the upper bounds at init time so latest_offset and read_table
        # return stable values across all microbatches in a single
        # Trigger.AvailableNow trigger.  Without this, datetime.now() would
        # advance between calls and prevent termination (the trigger
        # terminates when the same offset is returned twice in a row).  Data
        # arriving after these values is picked up by the next trigger.
        now = datetime.now(tz=timezone.utc)
        self._init_date = now.strftime("%Y%m%d")
        self._init_ts = now.isoformat()

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
        if table_name not in SUPPORTED_TABLES:
            raise ValueError(f"Unknown table '{table_name}'. Valid: {SUPPORTED_TABLES}")

        # Only diagnostics uses read_table (via simpleStreamReader).
        # studies/series/instances go through the partitioned path.
        if table_name == "diagnostics":
            # Short-circuit on the second call in a single trigger so
            # Trigger.AvailableNow terminates (end_offset == start_offset).
            if start_offset and start_offset.get("probe_timestamp") == self._init_ts:
                return iter([]), start_offset
            probe_iter = self._run_diagnostics_probe()
            return probe_iter, {"probe_timestamp": self._init_ts}

        raise ValueError(
            f"Table '{table_name}' uses partitioned reads; read_table is not supported."
        )

    # ------------------------------------------------------------------
    # SupportsPartitionedStream
    # ------------------------------------------------------------------

    def is_partitioned(self, table_name: str) -> bool:
        return table_name in ("studies", "series", "instances")

    def latest_offset(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict | None = None,
    ) -> dict:
        window_days = int(table_options.get("window_days", "0"))
        if window_days > 0:
            cursor = start_offset.get("study_date") if start_offset else None
            if not cursor:
                # On the first micro-batch there is no prior offset, so the
                # user must tell us where to start.  Without a bound, the
                # stream would walk forward one window at a time from
                # DEFAULT_START_DATE (1900) through ~125 years of empty
                # windows.
                starting_date = table_options.get("starting_date")
                if not starting_date:
                    raise ValueError(
                        f"table_options['starting_date'] is required when "
                        f"window_days > 0 (got window_days={window_days})"
                    )
                cursor = starting_date
            next_end = _add_days(cursor, window_days)
            return {"study_date": min(next_end, self._init_date)}
        return {"study_date": self._init_date}

    def get_partitions(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict | None = None,
        end_offset: dict | None = None,
    ) -> list[dict]:
        if start_offset is None and end_offset is None:
            # Batch mode: partition the entire table
            start_date = table_options.get("starting_date", DEFAULT_START_DATE)
            date_range = f"{start_date}-{self._init_date}"
            return self._build_partitions(table_name, date_range, table_options)

        # Stream mode: derive date range from the offsets Spark passes in.
        # On the very first call start_offset is {} (from initialOffset);
        # fall back to the user-supplied starting_date, which latest_offset
        # already validated when window_days > 0.
        start_date = (
            (start_offset or {}).get("study_date")
            or table_options.get("starting_date", DEFAULT_START_DATE)
        )
        end_date = end_offset["study_date"]
        if start_date >= end_date:
            return []

        lookback_days = int(table_options.get("lookback_days", str(DEFAULT_LOOKBACK_DAYS)))
        date_range = f"{_subtract_days(start_date, lookback_days)}-{end_date}"
        return self._build_partitions(table_name, date_range, table_options)

    def _build_partitions(
        self, table_name: str, date_range: str, table_options: dict[str, str]
    ) -> list[dict]:
        """Dispatch to the per-table partitioner, factoring out the offset glue."""
        if table_name == "instances":
            return self._partition_instances(date_range, table_options)
        if table_name == "series":
            return self._partition_series(date_range, table_options)
        # studies: single partition — QIDO-RS pagination is single-stream.
        return [{"date_range": date_range}]

    def read_partition(
        self, table_name: str, partition: dict, table_options: dict[str, str]
    ) -> Iterator[dict]:
        if table_name == "studies":
            return self._read_studies_partition(partition, table_options)
        elif table_name == "series":
            return self._read_series_partition(partition, table_options)
        elif table_name == "instances":
            return self._read_instances_partition(partition, table_options)
        raise ValueError(f"Unsupported table for partitioned read: {table_name}")

    def _enumerate_studies(
        self,
        date_range: str,
        page_size: int,
        extra_filters: dict | None = None,
    ) -> Iterator[dict]:
        """Page through QIDO-RS /studies and yield parsed study dicts."""
        offset = 0
        while True:
            batch = self._client.query_studies(
                date_range,
                limit=page_size,
                offset=offset,
                extra_filters=extra_filters,
            )
            if not batch:
                return
            for raw in batch:
                yield parse_study(raw)
            if len(batch) < page_size:
                return
            offset += page_size

    def _partition_series(self, date_range: str, table_options: dict[str, str]) -> list[dict]:
        """Discover studies in the date range and bin-pack them across partitions.

        Each task fetches ``/studies/{uid}/series`` for its assigned studies in
        parallel. Bins are weighted by ``number_of_study_related_series`` so a
        study with many series doesn't get bundled with several other heavy
        studies.

        ``study_qido_filters`` narrows the set of studies discovered on the
        driver — useful to limit ingestion scope without pulling everything.
        """
        num_partitions = int(table_options.get("num_partitions", str(DEFAULT_NUM_PARTITIONS)))
        page_size = int(table_options.get("page_size", DEFAULT_PAGE_SIZE))
        study_filters = _parse_qido_filters(table_options, "study_qido_filters")

        bins: list[list[dict]] = [[] for _ in range(num_partitions)]
        bin_counts = [0] * num_partitions
        for study in self._enumerate_studies(date_range, page_size, study_filters):
            uid = study.get("study_instance_uid")
            if not uid:
                continue
            count = study.get("number_of_study_related_series") or 1
            min_idx = bin_counts.index(min(bin_counts))
            bins[min_idx].append({"uid": uid, "study_date": study.get("study_date")})
            bin_counts[min_idx] += count

        return [{"studies": b} for b in bins if b]

    def _partition_instances(self, date_range: str, table_options: dict[str, str]) -> list[dict]:
        """Discover all (study, series) pairs in the date range and bin-pack series.

        Series are the unit of parallelism here (rather than studies) because
        WADO-RS retrieval scales per-series and a real-world dataset typically
        has many more series than studies — finer bins keep all executors busy.

        Driver cost: 1 ``query_studies`` paginated walk + one
        ``query_series_for_study`` call per study.  Acceptable for the common
        case; for very large datasets the user can keep ``window_days`` small
        to amortise this across micro-batches.

        Both ``study_qido_filters`` and ``series_qido_filters`` are applied at
        their respective enumeration steps to shrink the working set early.
        """
        num_partitions = int(table_options.get("num_partitions", str(DEFAULT_NUM_PARTITIONS)))
        page_size = int(table_options.get("page_size", DEFAULT_PAGE_SIZE))
        study_filters = _parse_qido_filters(table_options, "study_qido_filters")
        series_filters = _parse_qido_filters(table_options, "series_qido_filters")

        bins: list[list[dict]] = [[] for _ in range(num_partitions)]
        bin_counts = [0] * num_partitions
        for study in self._enumerate_studies(date_range, page_size, study_filters):
            study_uid = study.get("study_instance_uid")
            if not study_uid:
                continue
            study_date = study.get("study_date")
            for series_raw in self._client.query_series_for_study(
                study_uid, extra_filters=series_filters
            ):
                series = parse_series(series_raw)
                series_uid = series.get("series_instance_uid")
                if not series_uid:
                    continue
                min_idx = bin_counts.index(min(bin_counts))
                bins[min_idx].append(
                    {
                        "study_uid": study_uid,
                        "series_uid": series_uid,
                        "study_date": study_date,
                    }
                )
                bin_counts[min_idx] += 1

        return [{"series": b} for b in bins if b]

    def _read_studies_partition(
        self, partition: dict, table_options: dict[str, str]
    ) -> Iterator[dict]:
        date_range = partition["date_range"]
        page_size = int(table_options.get("page_size", DEFAULT_PAGE_SIZE))
        study_filters = _parse_qido_filters(table_options, "study_qido_filters")
        offset = 0
        while True:
            batch = self._client.query_studies(
                date_range, limit=page_size, offset=offset, extra_filters=study_filters
            )
            if not batch:
                break
            for raw in batch:
                record = parse_study(raw)
                record["connection_name"] = self._connection_name
                yield record
            if len(batch) < page_size:
                break
            offset += page_size

    def _read_series_partition(
        self, partition: dict, table_options: dict[str, str]
    ) -> Iterator[dict]:
        """Read series for the studies assigned to this partition."""
        series_filters = _parse_qido_filters(table_options, "series_qido_filters")
        for study_info in partition["studies"]:
            study_uid = study_info["uid"]
            study_date = study_info.get("study_date")
            for series_raw in self._client.query_series_for_study(
                study_uid, extra_filters=series_filters
            ):
                record = parse_series(series_raw)
                if not record.get("study_date"):
                    record["study_date"] = study_date
                if not record.get("study_instance_uid"):
                    record["study_instance_uid"] = study_uid
                record["connection_name"] = self._connection_name
                yield record

    def _read_instances_partition(
        self, partition: dict, table_options: dict[str, str]
    ) -> Iterator[dict]:
        """Read instances for the (study, series) pairs assigned to this partition.

        WADO-RS file retrieval is the dominant cost when ``fetch_dicom_files=true``.
        ``max_concurrent_downloads`` (default 8) controls a per-partition thread
        pool that downloads multiple files in parallel; set to 1 to disable.
        Records are yielded as their downloads complete; emission order within
        a partition is not guaranteed, which is fine because ``apply_changes``
        keys on ``sop_instance_uid``.
        """
        fetch_files = table_options.get("fetch_dicom_files", "false").lower() == "true"
        volume_path = table_options.get("dicom_volume_path", "")
        fetch_metadata = table_options.get("fetch_metadata", "false").lower() == "true"
        wado_mode = table_options.get("wado_mode", WADO_MODE_AUTO).lower()
        max_workers = max(
            1,
            int(table_options.get(
                "max_concurrent_downloads", str(DEFAULT_MAX_CONCURRENT_DOWNLOADS)
            )),
        )

        if fetch_files and not volume_path:
            raise ValueError("fetch_dicom_files=true requires dicom_volume_path to be set")

        instance_filters = _parse_qido_filters(table_options, "instance_qido_filters")

        # Reuse one pool across every series in this partition so we don't pay
        # the spawn/teardown cost per series.  Pool is only created when we
        # actually need it — concurrent downloads imply fetch_files=true.
        pool: ThreadPoolExecutor | None = None
        if fetch_files and max_workers > 1:
            pool = ThreadPoolExecutor(
                max_workers=max_workers, thread_name_prefix="dicomweb-wado"
            )
            logger.info(
                "WADO-RS thread pool enabled (max_concurrent_downloads=%d)", max_workers
            )

        try:
            for series_info in partition["series"]:
                yield from self._read_series_instances(
                    series_info,
                    fetch_files=fetch_files,
                    volume_path=volume_path,
                    fetch_metadata=fetch_metadata,
                    wado_mode=wado_mode,
                    instance_filters=instance_filters,
                    pool=pool,
                )
        finally:
            if pool is not None:
                pool.shutdown(wait=True)

    # pylint: disable=too-many-arguments
    def _read_series_instances(
        self,
        series_info: dict,
        *,
        fetch_files: bool,
        volume_path: str,
        fetch_metadata: bool,
        wado_mode: str,
        instance_filters: dict,
        pool: ThreadPoolExecutor | None,
    ) -> Iterator[dict]:
        """Yield instance records for one (study, series) pair."""
        study_uid = series_info["study_uid"]
        series_uid = series_info["series_uid"]
        study_date = series_info.get("study_date")

        instances_raw = self._client.query_instances_for_series(
            study_uid, series_uid, extra_filters=instance_filters
        )
        sop_to_meta: dict[str, str] = (
            self._build_metadata_map(study_uid, series_uid) if fetch_metadata else {}
        )

        base_records: list[dict] = []
        for inst_raw in instances_raw:
            record = parse_instance(inst_raw)
            if not record.get("study_date"):
                record["study_date"] = study_date
            if not record.get("study_instance_uid"):
                record["study_instance_uid"] = study_uid
            if not record.get("series_instance_uid"):
                record["series_instance_uid"] = series_uid
            if fetch_metadata:
                sop_uid = record.get("sop_instance_uid")
                record["metadata"] = sop_to_meta.get(sop_uid) if sop_uid else None
            base_records.append(record)

        if not fetch_files:
            for record in base_records:
                record["connection_name"] = self._connection_name
                yield record
            return

        if pool is None:
            for record in base_records:
                record = self._attach_dicom_file(record, volume_path, wado_mode)
                record["connection_name"] = self._connection_name
                yield record
            return

        yield from self._download_records_concurrently(
            pool, base_records, volume_path, wado_mode
        )

    def _download_records_concurrently(
        self,
        pool: ThreadPoolExecutor,
        base_records: list[dict],
        volume_path: str,
        wado_mode: str,
    ) -> Iterator[dict]:
        """Submit WADO-RS downloads to the pool and yield records as they complete.

        Handles the ``wado_mode=auto`` race: the first record is downloaded
        synchronously so ``self._wado_mode_detected`` stabilises before any
        parallel calls observe it. Without this, two threads racing on the
        first `auto`-detect could each issue a `full` GET and one would do
        the 404→`frames` fallback work twice.
        """
        if not base_records:
            return

        remaining: list[dict] = base_records
        if wado_mode == WADO_MODE_AUTO and self._wado_mode_detected is None:
            first = self._attach_dicom_file(base_records[0], volume_path, wado_mode)
            first["connection_name"] = self._connection_name
            yield first
            remaining = base_records[1:]

        if not remaining:
            return

        futures = {
            pool.submit(self._attach_dicom_file, rec, volume_path, wado_mode): rec
            for rec in remaining
        }
        for fut in as_completed(futures):
            try:
                record = fut.result()
            except Exception as exc:  # pylint: disable=broad-except
                # _attach_dicom_file already swallows per-instance errors and
                # returns the record with dicom_file_path=None, so this branch
                # is defensive only (e.g. a thread-pool-level failure).
                record = futures[fut]
                logger.error("WADO-RS concurrent retrieval failed: %s", exc)
                record["dicom_file_path"] = None
            record["connection_name"] = self._connection_name
            yield record

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

        # DICOM UIDs are constrained to digits and dots by the standard
        # (PS3.5 §9). Reject anything else before joining into the Volume
        # path to prevent a malformed server response from escaping the
        # configured volume_path via "..", absolute paths, or path
        # separators.
        for uid in (study_uid, series_uid, sop_uid):
            if not _is_valid_dicom_uid(uid):
                logger.warning("Skipping WADO-RS: invalid UID %r in record %s", uid, record)
                record["dicom_file_path"] = None
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
                    if wado_mode == WADO_MODE_AUTO and exc.code in (404, 406, 415):
                        logger.info(
                            "WADO-RS full instance returned HTTP %d "
                            "— auto-switching to frame retrieval",
                            exc.code,
                        )
                        self._wado_mode_detected = WADO_MODE_FRAMES
                        file_bytes = self._client.retrieve_instance_frames(
                            study_uid, series_uid, sop_uid
                        )
                        ext = ".jpg"
                    else:
                        raise
        except Exception as exc:
            logger.error("WADO-RS retrieval failed for %s: %s", sop_uid, exc)
            record["dicom_file_path"] = None
            return record

        dest_path_str = os.path.join(volume_path, study_uid, series_uid, f"{sop_uid}{ext}")
        os.makedirs(os.path.dirname(dest_path_str), exist_ok=True)
        with open(dest_path_str, "wb") as _f:
            _f.write(file_bytes)
        record["dicom_file_path"] = dest_path_str
        logger.debug("Wrote %d bytes → %s", len(file_bytes), dest_path_str)

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

    def _discover_sample_uids(self) -> tuple:
        """Discover sample study/series/instance UIDs for diagnostic probes."""
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

        return study_uid, series_uid, sop_uid

    def _run_diagnostics_probe(self) -> Iterator[dict]:
        """
        Probe all standard DICOMweb endpoints and yield one record per endpoint.

        Automatically discovers sample UIDs (study → series → instance) from the
        server so that hierarchical endpoints can be tested with real paths.
        Endpoints are never mutated — only GET requests are issued.
        """
        probe_timestamp = datetime.now(tz=timezone.utc).isoformat()
        study_uid, series_uid, sop_uid = self._discover_sample_uids()
        probes = _build_probe_definitions(study_uid, series_uid, sop_uid)

        for endpoint_pattern, path, category, description, accept in probes:
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
            supported, notes = _interpret_probe_status(result)
            yield {
                "endpoint": endpoint_pattern,
                "category": category,
                "description": description,
                "supported": supported,
                "status_code": result["status_code"],
                "content_type": result["content_type"],
                "latency_ms": result["latency_ms"],
                "notes": notes,
                "probe_timestamp": probe_timestamp,
                "connection_name": self._connection_name,
            }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_probe_definitions(
    study_uid: str | None, series_uid: str | None, sop_uid: str | None
) -> list[tuple[str, str | None, str, str, str | None]]:
    """Build the list of DICOMweb probe endpoint definitions."""
    has_study = study_uid is not None
    has_series = has_study and series_uid is not None
    has_instance = has_series and sop_uid is not None
    return [
        (
            "/studies",
            "/studies?limit=1",
            "QIDO-RS",
            "Search studies (flat pagination)",
            None,
        ),
        (
            "/studies/{uid}/series",
            f"/studies/{study_uid}/series" if has_study else None,
            "QIDO-RS",
            "Search series for a study (hierarchical)",
            None,
        ),
        (
            "/studies/{uid}/series/{uid}/instances",
            f"/studies/{study_uid}/series/{series_uid}/instances" if has_series else None,
            "QIDO-RS",
            "Search instances for a series (hierarchical)",
            None,
        ),
        (
            "/studies/{uid}/series/{uid}/metadata",
            f"/studies/{study_uid}/series/{series_uid}/metadata" if has_series else None,
            "WADO-RS",
            "Series metadata — full DICOM JSON for all instances in a series",
            "application/dicom+json",
        ),
        (
            "/studies/{uid}/series/{uid}/instances/{uid}/metadata",
            (
                f"/studies/{study_uid}/series/{series_uid}/instances/{sop_uid}/metadata"
                if has_instance
                else None
            ),
            "WADO-RS",
            "Instance metadata — full DICOM JSON for a single instance",
            "application/dicom+json",
        ),
        (
            "/studies/{uid}/series/{uid}/instances/{uid}",
            (
                f"/studies/{study_uid}/series/{series_uid}/instances/{sop_uid}"
                if has_instance
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
                if has_instance
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
                if has_instance
                else None
            ),
            "WADO-RS",
            "Retrieve rendered instance (viewport-ready PNG/JPEG)",
            "image/jpeg, image/png",
        ),
        (
            "/studies/{uid}/series/{uid}/rendered",
            f"/studies/{study_uid}/series/{series_uid}/rendered" if has_series else None,
            "WADO-RS",
            "Retrieve rendered series (all frames as viewport-ready images)",
            "image/jpeg, image/png",
        ),
        (
            "/studies/{uid}",
            f"/studies/{study_uid}" if has_study else None,
            "WADO-RS",
            "Retrieve entire study (all instances, multipart/related)",
            'multipart/related; type="application/dicom"',
        ),
    ]


_STATUS_NOTES = {
    400: ("partial", "Bad Request (400) — endpoint exists but query parameters may be required"),
    403: ("no", "Access Denied (403) — endpoint blocked by server or CDN policy"),
    404: ("no", "Not Found (404) — endpoint not implemented on this server"),
    406: ("no", "Not Acceptable (406) — requested media type not supported"),
}


def _interpret_probe_status(result: dict) -> tuple[str, str]:
    """Interpret a probe result into (supported, notes)."""
    if result["error"]:
        return "error", result["error"]
    status = result["status_code"]
    if status in (200, 204, 206):
        return "yes", f"Content-Type: {result['content_type']}"
    return _STATUS_NOTES.get(status, ("no", f"HTTP {status}"))


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


def _add_days(date_str: str, days: int) -> str:
    """Add `days` to a YYYYMMDD date string."""
    try:
        d = date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))
        d = d + timedelta(days=days)
        return d.strftime("%Y%m%d")
    except (ValueError, IndexError):
        return date_str
