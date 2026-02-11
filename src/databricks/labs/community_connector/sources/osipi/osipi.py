# pylint: disable=too-many-lines
# OSIPI Lakeflow Community Connector (Python Data Source)
#
# Implements the LakeflowConnect interface expected by the Lakeflow Community Connectors template.
#
# This source reads from an OSI PI Web API-compatible endpoint
# and exposes multiple logical tables via the `tableName` option.
#
# Authentication
# -------------
# Recommended: provide an OAuth/OIDC access token via `access_token` (Authorization: Bearer ...).
#
# Alternative: Basic authentication via `username`/`password` when enabled on the PI Web API host.
#
# Options supported:
# - pi_base_url / pi_web_api_url (required): base URL, e.g. https://<piwebapi-host>
# - access_token: (optional) bearer token
# - username/password: (optional) basic auth (if enabled)
#
# Table options (passed via table_configuration / table_options):
# - pi_points:
#   - dataserver_webid (optional)
#   - nameFilter (optional)
#   - maxCount (optional, default 1000)
#   - startIndex (optional, default 0)
#   - maxTotalCount (optional, default 100000) safety cap for pagination
# - pi_timeseries:
#   - tag_webids (optional csv); if missing will sample points (default_tags)
#   - default_tags (optional int, default 50)
#   - lookback_minutes (optional int, default 60) for first run
#   - maxCount (optional int, default 1000)
#   - prefer_streamset (optional bool, default true) to use StreamSet GetRecordedAdHoc
# - pi_event_frames:
#   - lookback_days (optional int, default 30) for first run
#   - maxCount (optional int, default 1000)
#   - startIndex (optional int, default 0)
#   - searchMode (optional str, default Overlapped)
# - pi_current_value:
#   - tag_webids (optional csv); if missing will sample points (default_tags)
#   - default_tags (optional int, default 50)
#   - time (optional time string) for Stream GetValue; default is current
# - pi_summary:
#   - tag_webids (optional csv); if missing will sample points (default_tags)
#   - default_tags (optional int, default 50)
#   - startTime / endTime (optional; defaults per PI Web API)
#   - summaryType (optional csv; defaults to Total)
#   - calculationBasis / timeType / summaryDuration / sampleType / sampleInterval (optional)
# - pi_streamset_recorded:
#   - tag_webids (optional csv); if missing will sample points (default_tags)
#   - default_tags (optional int, default 50)
#   - lookback_minutes (optional int, default 60) for first run
#   - maxCount (optional int, default 1000)
# - pi_element_attributes:
#   - element_webids (optional csv); if missing will sample elements (default_elements)
#   - default_elements (optional int, default 10)
#   - nameFilter (optional)
#   - maxCount (optional int, default 1000)
#   - startIndex (optional int, default 0)
# - pi_eventframe_attributes:
#   - event_frame_webids (optional csv); if missing will sample event frames (default_event_frames)
#   - default_event_frames (optional int, default 10)
#   - nameFilter (optional)
#   - maxCount (optional int, default 1000)
#   - startIndex (optional int, default 0)

from datetime import datetime, timedelta
from typing import Any, Dict, Iterator, List, Optional, Tuple

import requests
from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.osipi.osipi_constants import (
    SUPPORTED_TABLES,
    TABLE_AF_HIERARCHY,
    TABLE_AF_TABLE_ROWS,
    TABLE_AF_TABLES,
    TABLE_ANALYSES,
    TABLE_ANALYSIS_TEMPLATES,
    TABLE_ASSET_DATABASES,
    TABLE_ASSET_SERVERS,
    TABLE_ATTRIBUTE_TEMPLATES,
    TABLE_CALCULATED,
    TABLE_CATEGORIES,
    TABLE_CURRENT_VALUE,
    TABLE_DATASERVERS,
    TABLE_ELEMENT_ATTRIBUTES,
    TABLE_ELEMENT_TEMPLATE_ATTRIBUTES,
    TABLE_ELEMENT_TEMPLATES,
    TABLE_END,
    TABLE_EVENT_FRAMES,
    TABLE_EVENTFRAME_ACKS,
    TABLE_EVENTFRAME_ANNOTATIONS,
    TABLE_EVENTFRAME_ATTRIBUTES,
    TABLE_EVENTFRAME_REFERENCED_ELEMENTS,
    TABLE_EVENTFRAME_TEMPLATE_ATTRIBUTES,
    TABLE_EVENTFRAME_TEMPLATES,
    TABLE_INTERPOLATED,
    TABLE_LINKS,
    TABLE_PLOT,
    TABLE_POINT_ATTRIBUTES,
    TABLE_POINT_TYPE_CATALOG,
    TABLE_POINTS,
    TABLE_RECORDED_AT_TIME,
    TABLE_STREAMSET_END,
    TABLE_STREAMSET_INTERPOLATED,
    TABLE_STREAMSET_PLOT,
    TABLE_STREAMSET_RECORDED,
    TABLE_STREAMSET_SUMMARY,
    TABLE_SUMMARY,
    TABLE_TIMESERIES,
    TABLE_UNITS_OF_MEASURE,
    TABLE_VALUE_AT_TIME,
    TABLES_ASSET_FRAMEWORK,
    TABLES_DISCOVERY_INVENTORY,
    TABLES_EVENT_FRAMES,
    TABLES_GOVERNANCE_DIAGNOSTICS,
    TABLES_TIME_SERIES,
)
from databricks.labs.community_connector.sources.osipi.osipi_http import (
    PiWebApiClient,
    build_streamset_params,
    compute_time_range,
    paginate_time_series,
)
from databricks.labs.community_connector.sources.osipi.osipi_schemas import (
    TABLE_METADATA,
    TABLE_SCHEMAS,
)
from databricks.labs.community_connector.sources.osipi.osipi_utils import (
    as_bool,
    chunks,
    isoformat_z,
    parse_pi_time,
    parse_ts,
    try_float,
    utcnow,
)


class OsipiLakeflowConnect(LakeflowConnect):
    """OSI PI Lakeflow Community Connector.

    This connector reads from an OSI PI Web API-compatible endpoint
    and exposes multiple logical tables for discovery, time-series,
    asset framework, and event frame data.
    """

    # Re-export table constants for backwards compatibility
    TABLE_DATASERVERS = TABLE_DATASERVERS
    TABLE_POINTS = TABLE_POINTS
    TABLE_POINT_ATTRIBUTES = TABLE_POINT_ATTRIBUTES
    TABLE_TIMESERIES = TABLE_TIMESERIES
    TABLE_INTERPOLATED = TABLE_INTERPOLATED
    TABLE_PLOT = TABLE_PLOT
    TABLE_AF_HIERARCHY = TABLE_AF_HIERARCHY
    TABLE_EVENT_FRAMES = TABLE_EVENT_FRAMES
    TABLE_CURRENT_VALUE = TABLE_CURRENT_VALUE
    TABLE_SUMMARY = TABLE_SUMMARY
    TABLE_STREAMSET_RECORDED = TABLE_STREAMSET_RECORDED
    TABLE_STREAMSET_INTERPOLATED = TABLE_STREAMSET_INTERPOLATED
    TABLE_STREAMSET_SUMMARY = TABLE_STREAMSET_SUMMARY
    TABLE_ELEMENT_ATTRIBUTES = TABLE_ELEMENT_ATTRIBUTES
    TABLE_EVENTFRAME_ATTRIBUTES = TABLE_EVENTFRAME_ATTRIBUTES
    TABLE_ASSET_SERVERS = TABLE_ASSET_SERVERS
    TABLE_ASSET_DATABASES = TABLE_ASSET_DATABASES
    TABLE_ELEMENT_TEMPLATES = TABLE_ELEMENT_TEMPLATES
    TABLE_CATEGORIES = TABLE_CATEGORIES
    TABLE_ATTRIBUTE_TEMPLATES = TABLE_ATTRIBUTE_TEMPLATES
    TABLE_ANALYSES = TABLE_ANALYSES
    TABLE_EVENTFRAME_TEMPLATES = TABLE_EVENTFRAME_TEMPLATES
    TABLE_END = TABLE_END
    TABLE_VALUE_AT_TIME = TABLE_VALUE_AT_TIME
    TABLE_STREAMSET_PLOT = TABLE_STREAMSET_PLOT
    TABLE_UNITS_OF_MEASURE = TABLE_UNITS_OF_MEASURE
    TABLE_ANALYSIS_TEMPLATES = TABLE_ANALYSIS_TEMPLATES
    TABLE_EVENTFRAME_TEMPLATE_ATTRIBUTES = TABLE_EVENTFRAME_TEMPLATE_ATTRIBUTES
    TABLE_STREAMSET_END = TABLE_STREAMSET_END
    TABLE_ELEMENT_TEMPLATE_ATTRIBUTES = TABLE_ELEMENT_TEMPLATE_ATTRIBUTES
    TABLE_EVENTFRAME_REFERENCED_ELEMENTS = TABLE_EVENTFRAME_REFERENCED_ELEMENTS
    TABLE_AF_TABLES = TABLE_AF_TABLES
    TABLE_AF_TABLE_ROWS = TABLE_AF_TABLE_ROWS
    TABLE_EVENTFRAME_ACKS = TABLE_EVENTFRAME_ACKS
    TABLE_EVENTFRAME_ANNOTATIONS = TABLE_EVENTFRAME_ANNOTATIONS
    TABLE_RECORDED_AT_TIME = TABLE_RECORDED_AT_TIME
    TABLE_CALCULATED = TABLE_CALCULATED
    TABLE_POINT_TYPE_CATALOG = TABLE_POINT_TYPE_CATALOG
    TABLE_LINKS = TABLE_LINKS

    # Re-export table groups for backwards compatibility
    TABLES_DISCOVERY_INVENTORY = TABLES_DISCOVERY_INVENTORY
    TABLES_TIME_SERIES = TABLES_TIME_SERIES
    TABLES_ASSET_FRAMEWORK = TABLES_ASSET_FRAMEWORK
    TABLES_EVENT_FRAMES = TABLES_EVENT_FRAMES
    TABLES_GOVERNANCE_DIAGNOSTICS = TABLES_GOVERNANCE_DIAGNOSTICS

    def __init__(self, options: Dict[str, str]) -> None:
        """
        IMPORTANT: This connector runs inside Spark Python Data Source workers.

        Do NOT reference SparkSession/SparkContext here (or anywhere in the connector).
        All configuration must come from `options`, including UC Connection-injected
        options when `.option("databricks.connection", <name>)` is used.
        """
        # DEBUG: Log all received options to verify UC Connection injection
        print(f"[OSIPI DEBUG] __init__ received options keys: {list(options.keys())}")
        print(f"[OSIPI DEBUG] sourceName: {options.get('sourceName')}")
        print(f"[OSIPI DEBUG] pi_base_url: {options.get('pi_base_url')}")
        print(f"[OSIPI DEBUG] host: {options.get('host')}")
        print(f"[OSIPI DEBUG] bearer_token present: {'bearer_token' in options}")
        print(f"[OSIPI DEBUG] access_token present: {'access_token' in options}")
        print(f"[OSIPI DEBUG] bearer_value present: {'bearer_value' in options}")

        self.options = options

        # Initialize HTTP client (handles auth, base_url resolution, SSL config)
        self._client = PiWebApiClient(options)

    def list_tables(self) -> List[str]:
        """Return a list of all supported table names."""
        return list(SUPPORTED_TABLES)

    def get_table_schema(self, table_name: str, table_options: Dict[str, str]) -> StructType:
        """Return the schema for a given table."""
        schema = TABLE_SCHEMAS.get(table_name)
        if schema is None:
            raise ValueError(f"Unknown table: {table_name}")
        return schema

    def read_table_metadata(self, table_name: str, table_options: Dict[str, str]) -> Dict:
        """Return metadata for a given table."""
        meta = TABLE_METADATA.get(table_name)
        if meta is None:
            raise ValueError(f"Unknown table: {table_name}")
        return dict(meta)

    def read_table(
        self, table_name: str, start_offset: dict, table_options: Dict[str, str]
    ) -> Tuple[Iterator[dict], dict]:
        """Read data from a table."""
        self._client.ensure_auth()

        # Snapshot offset semantics:
        # For snapshot tables, once we return an "end" offset, subsequent reads with the same offset
        # must return no rows to avoid duplicates in streaming mode.
        try:
            meta = self.read_table_metadata(table_name, table_options)
            if (
                meta.get("ingestion_type") == "snapshot"
                and isinstance(start_offset, dict)
                and start_offset.get("offset") == "done"
            ):
                return iter(()), dict(start_offset)
        except Exception:
            # Never fail the read due to metadata lookup; fall back to existing behavior.
            pass

        dispatch = {
            # Discovery & inventory
            TABLE_DATASERVERS: lambda: (iter(self._read_dataservers()), {"offset": "done"}),
            TABLE_POINTS: lambda: (iter(self._read_points(table_options)), {"offset": "done"}),
            TABLE_POINT_ATTRIBUTES: lambda: (
                iter(self._read_point_attributes(table_options)),
                {"offset": "done"},
            ),
            TABLE_POINT_TYPE_CATALOG: lambda: (
                iter(self._read_point_type_catalog(table_options)),
                {"offset": "done"},
            ),
            # Time-series
            TABLE_TIMESERIES: lambda: self._read_timeseries(start_offset, table_options),
            TABLE_STREAMSET_RECORDED: lambda: self._read_streamset_recorded(
                start_offset, table_options
            ),
            TABLE_INTERPOLATED: lambda: self._read_interpolated(start_offset, table_options),
            TABLE_STREAMSET_INTERPOLATED: lambda: self._read_streamset_interpolated(
                start_offset, table_options
            ),
            TABLE_PLOT: lambda: self._read_plot(start_offset, table_options),
            TABLE_STREAMSET_PLOT: lambda: self._read_streamset_plot(start_offset, table_options),
            TABLE_SUMMARY: lambda: (iter(self._read_summary(table_options)), {"offset": "done"}),
            TABLE_STREAMSET_SUMMARY: lambda: self._read_streamset_summary(
                start_offset, table_options
            ),
            TABLE_CURRENT_VALUE: lambda: (
                iter(self._read_current_value(table_options)),
                {"offset": "done"},
            ),
            TABLE_VALUE_AT_TIME: lambda: (
                iter(self._read_value_at_time(table_options)),
                {"offset": "done"},
            ),
            TABLE_RECORDED_AT_TIME: lambda: (
                iter(self._read_recorded_at_time(table_options)),
                {"offset": "done"},
            ),
            TABLE_END: lambda: (iter(self._read_end(table_options)), {"offset": "done"}),
            TABLE_STREAMSET_END: lambda: (
                iter(self._read_streamset_end(table_options)),
                {"offset": "done"},
            ),
            TABLE_CALCULATED: lambda: self._read_calculated(start_offset, table_options),
            # Asset Framework (AF)
            TABLE_ASSET_SERVERS: lambda: (
                iter(self._read_assetservers_table()),
                {"offset": "done"},
            ),
            TABLE_ASSET_DATABASES: lambda: (
                iter(self._read_assetdatabases_table()),
                {"offset": "done"},
            ),
            TABLE_AF_HIERARCHY: lambda: (iter(self._read_af_hierarchy()), {"offset": "done"}),
            TABLE_ELEMENT_ATTRIBUTES: lambda: (
                iter(self._read_element_attributes(table_options)),
                {"offset": "done"},
            ),
            TABLE_ELEMENT_TEMPLATES: lambda: (
                iter(self._read_element_templates_table(table_options)),
                {"offset": "done"},
            ),
            TABLE_ELEMENT_TEMPLATE_ATTRIBUTES: lambda: (
                iter(self._read_element_template_attributes_table(table_options)),
                {"offset": "done"},
            ),
            TABLE_ATTRIBUTE_TEMPLATES: lambda: (
                iter(self._read_attribute_templates_table(table_options)),
                {"offset": "done"},
            ),
            TABLE_CATEGORIES: lambda: (
                iter(self._read_categories_table(table_options)),
                {"offset": "done"},
            ),
            TABLE_ANALYSES: lambda: (
                iter(self._read_analyses_table(table_options)),
                {"offset": "done"},
            ),
            TABLE_ANALYSIS_TEMPLATES: lambda: (
                iter(self._read_analysis_templates_table(table_options)),
                {"offset": "done"},
            ),
            TABLE_AF_TABLES: lambda: (
                iter(self._read_af_tables_table(table_options)),
                {"offset": "done"},
            ),
            TABLE_AF_TABLE_ROWS: lambda: (
                iter(self._read_af_table_rows_table(table_options)),
                {"offset": "done"},
            ),
            TABLE_UNITS_OF_MEASURE: lambda: (
                iter(self._read_units_of_measure_table()),
                {"offset": "done"},
            ),
            # Event Frames
            TABLE_EVENT_FRAMES: lambda: self._read_event_frames(start_offset, table_options),
            TABLE_EVENTFRAME_ATTRIBUTES: lambda: (
                iter(self._read_eventframe_attributes(table_options)),
                {"offset": "done"},
            ),
            TABLE_EVENTFRAME_TEMPLATES: lambda: (
                iter(self._read_eventframe_templates_table(table_options)),
                {"offset": "done"},
            ),
            TABLE_EVENTFRAME_TEMPLATE_ATTRIBUTES: lambda: (
                iter(self._read_eventframe_template_attributes_table(table_options)),
                {"offset": "done"},
            ),
            TABLE_EVENTFRAME_REFERENCED_ELEMENTS: lambda: self._read_eventframe_referenced_elements(
                start_offset, table_options
            ),
            TABLE_EVENTFRAME_ACKS: lambda: (
                iter(self._read_eventframe_acknowledgements_table(table_options)),
                {"offset": "done"},
            ),
            TABLE_EVENTFRAME_ANNOTATIONS: lambda: (
                iter(self._read_eventframe_annotations_table(table_options)),
                {"offset": "done"},
            ),
            # Governance & diagnostics
            TABLE_LINKS: lambda: (iter(self._read_links(table_options)), {"offset": "done"}),
        }

        handler = dispatch.get(table_name)
        if handler is None:
            raise ValueError(f"Unknown table: {table_name}")
        return handler()

    # =========================================================================
    # Discovery & Inventory readers
    # =========================================================================

    def _read_dataservers(self) -> List[dict]:
        """Read PI data servers."""
        data = self._client.get_json("/piwebapi/dataservers")
        items = data.get("Items", []) or []
        return [{"webid": i.get("WebId"), "name": i.get("Name")} for i in items if i.get("WebId")]

    def _read_points(self, table_options: Dict[str, str]) -> List[dict]:
        """Read PI points (tags)."""
        dataservers = self._read_dataservers()
        if not dataservers:
            return []
        server_webid = table_options.get("dataserver_webid") or dataservers[0]["webid"]

        page_size = int(table_options.get("maxCount", 1000))
        start_index = int(table_options.get("startIndex", 0))
        max_total = int(table_options.get("maxTotalCount", 100000))
        name_filter = table_options.get("nameFilter")

        out: List[dict] = []
        while start_index < max_total:
            params: Dict[str, str] = {"maxCount": str(page_size), "startIndex": str(start_index)}
            if name_filter:
                params["nameFilter"] = str(name_filter)

            data = self._client.get_json(
                f"/piwebapi/dataservers/{server_webid}/points", params=params
            )
            items = data.get("Items", []) or []

            for p in items:
                out.append(
                    {
                        "webid": p.get("WebId"),
                        "name": p.get("Name"),
                        "descriptor": p.get("Descriptor", ""),
                        "engineering_units": p.get("EngineeringUnits", ""),
                        "path": p.get("Path", ""),
                        "dataserver_webid": server_webid,
                    }
                )

            if len(items) < page_size:
                break
            start_index += page_size

        return [r for r in out if r.get("webid")]

    def _resolve_tag_webids(self, table_options: Dict[str, str]) -> List[str]:
        """Resolve tag WebIDs from options or by sampling points."""
        tag_webids_csv = table_options.get("tag_webids") or self.options.get("tag_webids") or ""
        tag_webids = [t.strip() for t in str(tag_webids_csv).split(",") if t.strip()]
        if tag_webids:
            return tag_webids
        pts = self._read_points(table_options)
        return [p["webid"] for p in pts[: int(table_options.get("default_tags", 50))]]

    def _read_point_attributes(self, table_options: Dict[str, str]) -> List[dict]:
        """Read PI point attributes."""
        point_webids_csv = (
            table_options.get("point_webids") or self.options.get("point_webids") or ""
        ).strip()
        point_webids = [t.strip() for t in point_webids_csv.split(",") if t.strip()]
        if not point_webids:
            default_points = int(table_options.get("default_points", 10))
            pts = self._read_points(table_options)
            point_webids = [p["webid"] for p in pts[:default_points] if p.get("webid")]

        params: Dict[str, str] = {}
        selected_fields = table_options.get("selectedFields")
        if selected_fields:
            params["selectedFields"] = selected_fields

        out: List[dict] = []
        ingest_ts = utcnow()

        for wid in point_webids:
            try:
                data = self._client.get_json(
                    f"/piwebapi/points/{wid}/attributes", params=params or None
                )
                for item in data.get("Items") or []:
                    out.append(
                        {
                            "point_webid": wid,
                            "name": item.get("Name"),
                            "value": None if item.get("Value") is None else str(item.get("Value")),
                            "type": item.get("Type") or item.get("ValueType") or "",
                            "ingestion_timestamp": ingest_ts,
                        }
                    )
            except Exception:
                continue

        return out

    def _read_point_type_catalog(self, table_options: Dict[str, str]) -> List[dict]:
        """Derive a point type catalog by scanning points."""
        points = self._read_points(table_options)
        counts: Dict[Tuple[str, str], int] = {}
        for p in points:
            desc = (p.get("descriptor") or "").strip()
            eu = (p.get("engineering_units") or "").strip()
            pt = (desc.split(" ", 1)[0] if desc else "Unknown") or "Unknown"
            key = (pt, eu)
            counts[key] = counts.get(key, 0) + 1
        out: List[dict] = []
        for (pt, eu), n in sorted(counts.items(), key=lambda x: (-x[1], x[0][0], x[0][1])):
            out.append({"point_type": pt, "engineering_units": eu, "count_points": n})
        return out

    # =========================================================================
    # Time-series readers
    # =========================================================================

    def _read_timeseries(
        self, start_offset: dict, table_options: Dict[str, str]
    ) -> Tuple[Iterator[dict], dict]:
        """Read recorded time-series values."""
        tag_webids = self._resolve_tag_webids(table_options)
        start_str, end_str = compute_time_range(
            start_offset, table_options, apply_window_seconds=True
        )
        max_count = int(table_options.get("maxCount", 1000))
        ingest_ts = utcnow()

        tags_per_request = int(table_options.get("tags_per_request", 0) or 0)
        groups = chunks(tag_webids, tags_per_request) if tags_per_request else [tag_webids]

        prefer_streamset = as_bool(table_options.get("prefer_streamset", True), default=True)
        selected_fields = table_options.get("selectedFields")

        next_offset = {"offset": end_str}

        if prefer_streamset and len(tag_webids) > 1:
            def iterator() -> Iterator[dict]:
                for group in groups:
                    if not group:
                        continue

                    def get_data(start: str, end: str) -> dict:
                        params = build_streamset_params(
                            group,
                            start_str=start,
                            end_str=end,
                            max_count=max_count,
                            selected_fields=selected_fields,
                        )
                        return self._client.get_json("/piwebapi/streamsets/recorded", params=params)

                    for stream in paginate_time_series(get_data, start_str, end_str, max_count):
                        webid = stream.get("WebId")
                        if not webid:
                            continue
                        for item in stream.get("Items", []) or []:
                            ts = item.get("Timestamp")
                            if not ts:
                                continue
                            yield {
                                "tag_webid": webid,
                                "timestamp": parse_ts(ts),
                                "value": try_float(item.get("Value")),
                                "good": as_bool(item.get("Good"), default=True),
                                "questionable": as_bool(item.get("Questionable"), default=False),
                                "substituted": as_bool(item.get("Substituted"), default=False),
                                "annotated": as_bool(item.get("Annotated"), default=False),
                                "units": item.get("UnitsAbbreviation", ""),
                                "ingestion_timestamp": ingest_ts,
                            }

            return iterator(), next_offset

        # Fallback: Batch execute
        def batch_iterator() -> Iterator[dict]:
            for group in groups:
                if not group:
                    continue
                reqs = [
                    {
                        "Method": "GET",
                        "Resource": f"/piwebapi/streams/{webid}/recorded",
                        "Parameters": {
                            "startTime": start_str,
                            "endTime": end_str,
                            "maxCount": str(max_count),
                        },
                    }
                    for webid in group
                ]
                responses = self._client.batch_execute(reqs)
                for idx, (_rid, resp) in enumerate(responses):
                    if resp.get("Status") != 200:
                        continue
                    webid = group[idx] if idx < len(group) else None
                    if not webid:
                        continue
                    content = resp.get("Content", {}) or {}
                    for item in content.get("Items", []) or []:
                        ts = item.get("Timestamp")
                        if not ts:
                            continue
                        yield {
                            "tag_webid": webid,
                            "timestamp": parse_ts(ts),
                            "value": try_float(item.get("Value")),
                            "good": as_bool(item.get("Good"), default=True),
                            "questionable": as_bool(item.get("Questionable"), default=False),
                            "substituted": as_bool(item.get("Substituted"), default=False),
                            "annotated": as_bool(item.get("Annotated"), default=False),
                            "units": item.get("UnitsAbbreviation", ""),
                            "ingestion_timestamp": ingest_ts,
                        }

        return batch_iterator(), next_offset

    def _read_streamset_recorded(
        self, start_offset: dict, table_options: Dict[str, str]
    ) -> Tuple[Iterator[dict], dict]:
        """Read recorded values via StreamSet endpoint."""
        tag_webids = self._resolve_tag_webids(table_options)
        start_str, end_str = compute_time_range(
            start_offset, table_options, apply_window_seconds=True
        )
        max_count = int(table_options.get("maxCount", 1000))
        ingest_ts = utcnow()

        tags_per_request = int(table_options.get("tags_per_request", 0) or 0)
        groups = chunks(tag_webids, tags_per_request) if tags_per_request else [tag_webids]
        selected_fields = table_options.get("selectedFields")

        def iterator() -> Iterator[dict]:
            for group in groups:
                if not group:
                    continue

                def get_data(start: str, end: str) -> dict:
                    params = build_streamset_params(
                        group,
                        start_str=start,
                        end_str=end,
                        max_count=max_count,
                        selected_fields=selected_fields,
                    )
                    return self._client.get_json("/piwebapi/streamsets/recorded", params=params)

                for stream in paginate_time_series(get_data, start_str, end_str, max_count):
                    webid = stream.get("WebId")
                    if not webid:
                        continue
                    for item in stream.get("Items", []) or []:
                        ts = item.get("Timestamp")
                        if not ts:
                            continue
                        yield {
                            "tag_webid": webid,
                            "timestamp": parse_ts(ts),
                            "value": try_float(item.get("Value")),
                            "good": as_bool(item.get("Good"), default=True),
                            "questionable": as_bool(item.get("Questionable"), default=False),
                            "substituted": as_bool(item.get("Substituted"), default=False),
                            "annotated": as_bool(item.get("Annotated"), default=False),
                            "units": item.get("UnitsAbbreviation", ""),
                            "ingestion_timestamp": ingest_ts,
                        }

        return iterator(), {"offset": end_str}

    def _read_interpolated(
        self, start_offset: dict, table_options: Dict[str, str]
    ) -> Tuple[Iterator[dict], dict]:
        """Read interpolated values."""
        tag_webids = self._resolve_tag_webids(table_options)
        start_str, end_str = compute_time_range(
            start_offset, table_options, apply_window_seconds=False
        )

        interval = (
            table_options.get("interval") or table_options.get("sampleInterval") or "1m"
        ).strip()
        max_count = int(table_options.get("maxCount", 1000))
        ingest_ts = utcnow()

        tags_per_request = int(table_options.get("tags_per_request", 0) or 0)
        groups = chunks(tag_webids, tags_per_request) if tags_per_request else [tag_webids]
        selected_fields = table_options.get("selectedFields")

        next_offset = {"offset": end_str}

        def emit_items(wid: str, items: List[dict]) -> Iterator[dict]:
            for item in items or []:
                ts = item.get("Timestamp")
                if not ts:
                    continue
                yield {
                    "tag_webid": wid,
                    "timestamp": parse_ts(ts),
                    "value": try_float(item.get("Value")),
                    "good": as_bool(item.get("Good"), default=True),
                    "questionable": as_bool(item.get("Questionable"), default=False),
                    "substituted": as_bool(item.get("Substituted"), default=False),
                    "annotated": as_bool(item.get("Annotated"), default=False),
                    "units": item.get("UnitsAbbreviation", ""),
                    "ingestion_timestamp": ingest_ts,
                }

        def iterator() -> Iterator[dict]:  # pylint: disable=too-many-branches
            for group in groups:
                if not group:
                    continue
                # Prefer streamsets/interpolated when multiple tags
                if len(group) > 1:
                    # Define a function that makes the API call for pagination
                    def get_data(start: str, end: str) -> dict:
                        params = build_streamset_params(
                            group,
                            start_str=start,
                            end_str=end,
                            interval=interval,
                            max_count=max_count,
                            selected_fields=selected_fields,
                        )
                        return self._client.get_json(
                            "/piwebapi/streamsets/interpolated", params=params
                        )

                    try:
                        for stream in paginate_time_series(get_data, start_str, end_str, max_count):
                            wid = stream.get("WebId")
                            if not wid:
                                continue
                            yield from emit_items(wid, stream.get("Items", []) or [])
                        continue
                    except requests.exceptions.HTTPError as e:
                        if getattr(e.response, "status_code", None) == 404:
                            pass
                        else:
                            raise

                # Fallback: per-tag interpolated via batch
                reqs = [
                    {
                        "Method": "GET",
                        "Resource": f"/piwebapi/streams/{wid}/interpolated",
                        "Parameters": {
                            "startTime": start_str,
                            "endTime": end_str,
                            "interval": interval,
                            "maxCount": str(max_count),
                        },
                    }
                    for wid in group
                ]
                try:
                    responses = self._client.batch_execute(reqs)
                except requests.exceptions.HTTPError as e:
                    if getattr(e.response, "status_code", None) == 404:
                        return
                    raise
                for idx, (_rid, resp) in enumerate(responses):
                    if resp.get("Status") != 200:
                        continue
                    wid = group[idx] if idx < len(group) else None
                    if not wid:
                        continue
                    content = resp.get("Content", {}) or {}
                    yield from emit_items(wid, content.get("Items", []) or [])

        return iterator(), next_offset

    def _read_streamset_interpolated(
        self, start_offset: dict, table_options: Dict[str, str]
    ) -> Tuple[Iterator[dict], dict]:
        """Read interpolated values via StreamSet endpoint."""
        opts = dict(table_options)
        opts["tags_per_request"] = str(max(1, int(opts.get("tags_per_request", 0) or 0)))
        return self._read_interpolated(start_offset, opts)

    def _read_plot(
        self, start_offset: dict, table_options: Dict[str, str]
    ) -> Tuple[Iterator[dict], dict]:
        """Read plot values."""
        tag_webids = self._resolve_tag_webids(table_options)
        start_str, end_str = compute_time_range(
            start_offset, table_options, apply_window_seconds=False
        )
        intervals = int(table_options.get("intervals", 300) or 300)
        ingest_ts = utcnow()

        def iterator() -> Iterator[dict]:
            for wid in tag_webids:
                try:
                    data = self._client.get_json(
                        f"/piwebapi/streams/{wid}/plot",
                        params={
                            "startTime": start_str,
                            "endTime": end_str,
                            "intervals": str(intervals),
                        },
                    )
                except requests.exceptions.HTTPError as e:
                    if getattr(e.response, "status_code", None) == 404:
                        return
                    raise

                for item in data.get("Items") or []:
                    ts = item.get("Timestamp")
                    if not ts:
                        continue
                    yield {
                        "tag_webid": wid,
                        "timestamp": parse_ts(ts),
                        "value": try_float(item.get("Value")),
                        "good": as_bool(item.get("Good"), default=True),
                        "questionable": as_bool(item.get("Questionable"), default=False),
                        "substituted": as_bool(item.get("Substituted"), default=False),
                        "annotated": as_bool(item.get("Annotated"), default=False),
                        "units": item.get("UnitsAbbreviation", ""),
                        "ingestion_timestamp": ingest_ts,
                    }

        return iterator(), {"offset": end_str}

    def _read_streamset_plot(  # pylint: disable=too-many-statements
        self, start_offset: dict, table_options: Dict[str, str]
    ) -> Tuple[Iterator[dict], dict]:
        """Read plot values via StreamSet endpoint."""
        tag_webids = self._resolve_tag_webids(table_options)
        now = utcnow()

        end_opt = table_options.get("endTime") or table_options.get("end_time") or "*"
        end_dt = parse_pi_time(end_opt, now=now)

        start_dt: Optional[datetime] = None
        if start_offset and isinstance(start_offset, dict):
            off = start_offset.get("offset")
            if isinstance(off, str) and off:
                try:
                    start_dt = parse_ts(off)
                except Exception:
                    start_dt = None

        if start_dt is None:
            start_opt = table_options.get("startTime") or table_options.get("start_time")
            if start_opt:
                start_dt = parse_pi_time(str(start_opt), now=end_dt)
            else:
                lookback_minutes = int(table_options.get("lookback_minutes", 60))
                start_dt = end_dt - timedelta(minutes=lookback_minutes)

        start_str = isoformat_z(start_dt)
        end_str = isoformat_z(end_dt)
        intervals = int(table_options.get("intervals", 300) or 300)
        ingest_ts = utcnow()
        next_offset = {"offset": end_str}

        tags_per_request = int(table_options.get("tags_per_request", 0) or 0)
        groups = chunks(tag_webids, tags_per_request) if tags_per_request else [tag_webids]

        def iterator() -> Iterator[dict]:  # pylint: disable=too-many-branches
            for group in groups:
                if not group:
                    continue
                params: List[Tuple[str, str]] = [("webId", w) for w in group]
                params += [
                    ("startTime", start_str),
                    ("endTime", end_str),
                    ("intervals", str(intervals)),
                ]
                try:
                    data = self._client.get_json("/piwebapi/streamsets/plot", params=params)
                except requests.exceptions.HTTPError as e:
                    if getattr(e.response, "status_code", None) == 404:
                        data = None
                    else:
                        raise

                if data:
                    for stream in data.get("Items", []) or []:
                        wid = stream.get("WebId")
                        if not wid:
                            continue
                        for item in stream.get("Items", []) or []:
                            ts = item.get("Timestamp")
                            if not ts:
                                continue
                            yield {
                                "tag_webid": wid,
                                "timestamp": parse_ts(ts),
                                "value": try_float(item.get("Value")),
                                "good": as_bool(item.get("Good"), default=True),
                                "questionable": as_bool(item.get("Questionable"), default=False),
                                "substituted": as_bool(item.get("Substituted"), default=False),
                                "annotated": as_bool(item.get("Annotated"), default=False),
                                "units": item.get("UnitsAbbreviation", ""),
                                "ingestion_timestamp": ingest_ts,
                            }
                    continue

                for wid in group:
                    try:
                        pdata = self._client.get_json(
                            f"/piwebapi/streams/{wid}/plot",
                            params={
                                "startTime": start_str,
                                "endTime": end_str,
                                "intervals": str(intervals),
                            },
                        )
                    except requests.exceptions.HTTPError as e:
                        if getattr(e.response, "status_code", None) == 404:
                            continue
                        raise
                    for item in pdata.get("Items", []) or []:
                        ts = item.get("Timestamp")
                        if not ts:
                            continue
                        yield {
                            "tag_webid": wid,
                            "timestamp": parse_ts(ts),
                            "value": try_float(item.get("Value")),
                            "good": as_bool(item.get("Good"), default=True),
                            "questionable": as_bool(item.get("Questionable"), default=False),
                            "substituted": as_bool(item.get("Substituted"), default=False),
                            "annotated": as_bool(item.get("Annotated"), default=False),
                            "units": item.get("UnitsAbbreviation", ""),
                            "ingestion_timestamp": ingest_ts,
                        }

        return iterator(), next_offset

    def _read_summary(self, table_options: Dict[str, str]) -> List[dict]:
        """Read summary values."""
        tag_webids = self._resolve_tag_webids(table_options)
        start_time = table_options.get("startTime")
        end_time = table_options.get("endTime")
        summary_types_csv = table_options.get("summaryType", "Total")
        summary_types = [s.strip() for s in str(summary_types_csv).split(",") if s.strip()]
        if not summary_types:
            summary_types = ["Total"]

        passthrough_keys = (
            "calculationBasis",
            "timeType",
            "summaryDuration",
            "sampleType",
            "sampleInterval",
            "timeZone",
            "filterExpression",
        )
        passthrough = {
            k: str(table_options.get(k))
            for k in passthrough_keys
            if table_options.get(k) is not None
        }

        ingest_ts = utcnow()
        out: List[dict] = []

        for w in tag_webids:
            params: List[Tuple[str, str]] = []
            if start_time:
                params.append(("startTime", str(start_time)))
            if end_time:
                params.append(("endTime", str(end_time)))
            for st in summary_types:
                params.append(("summaryType", st))
            for k, v in passthrough.items():
                params.append((k, v))

            data = self._client.get_json(f"/piwebapi/streams/{w}/summary", params=params)
            for item in data.get("Items", []) or []:
                stype = item.get("Type") or ""
                v = item.get("Value", {}) or {}
                ts = v.get("Timestamp")
                out.append(
                    {
                        "tag_webid": w,
                        "summary_type": str(stype),
                        "timestamp": parse_ts(ts) if ts else None,
                        "value": try_float(v.get("Value")),
                        "good": as_bool(v.get("Good"), default=True),
                        "questionable": as_bool(v.get("Questionable"), default=False),
                        "substituted": as_bool(v.get("Substituted"), default=False),
                        "annotated": as_bool(v.get("Annotated"), default=False),
                        "units": v.get("UnitsAbbreviation", ""),
                        "ingestion_timestamp": ingest_ts,
                    }
                )

        return out

    def _read_streamset_summary(
        self, start_offset: dict, table_options: Dict[str, str]
    ) -> Tuple[Iterator[dict], dict]:
        """Read multi-tag summary via StreamSet endpoint."""
        tag_webids = self._resolve_tag_webids(table_options)
        start_str, end_str = compute_time_range(
            start_offset, table_options, apply_window_seconds=False
        )

        summary_type = (table_options.get("summaryType") or "Total").strip()
        calculation_basis = (table_options.get("calculationBasis") or "TimeWeighted").strip()
        summary_duration = (table_options.get("summaryDuration") or "1h").strip()
        selected_fields = table_options.get("selectedFields")

        tags_per_request = int(table_options.get("tags_per_request", 0) or 0)
        groups = chunks(tag_webids, tags_per_request) if tags_per_request else [tag_webids]

        ingest_ts = utcnow()

        def iterator() -> Iterator[dict]:
            for group in groups:
                if not group:
                    continue
                params = build_streamset_params(
                    group,
                    start_str=start_str,
                    end_str=end_str,
                    selected_fields=selected_fields,
                )
                params += [
                    ("summaryType", summary_type),
                    ("calculationBasis", calculation_basis),
                    ("summaryDuration", summary_duration),
                ]

                try:
                    data = self._client.get_json("/piwebapi/streamsets/summary", params=params)
                except requests.exceptions.HTTPError as e:
                    if getattr(e.response, "status_code", None) == 404:
                        return
                    raise

                for stream in data.get("Items", []) or []:
                    wid = stream.get("WebId")
                    if not wid:
                        continue
                    for item in stream.get("Items", []) or []:
                        stype = item.get("Type")
                        v = item.get("Value", {}) or {}
                        ts = v.get("Timestamp")
                        if not ts:
                            continue
                        yield {
                            "tag_webid": wid,
                            "summary_type": str(stype),
                            "timestamp": parse_ts(ts),
                            "value": try_float(v.get("Value")),
                            "good": as_bool(v.get("Good"), default=True),
                            "questionable": as_bool(v.get("Questionable"), default=False),
                            "substituted": as_bool(v.get("Substituted"), default=False),
                            "annotated": as_bool(v.get("Annotated"), default=False),
                            "units": v.get("UnitsAbbreviation", ""),
                            "ingestion_timestamp": ingest_ts,
                        }

        return iterator(), {"offset": end_str}

    def _read_current_value(self, table_options: Dict[str, str]) -> List[dict]:
        """Read current values."""
        tag_webids = self._resolve_tag_webids(table_options)
        tags_per_request = int(table_options.get("tags_per_request", 0) or 0)
        tag_webid_groups = (
            chunks(tag_webids, tags_per_request) if tags_per_request else [tag_webids]
        )
        time_param = table_options.get("time")

        ingest_ts = utcnow()
        out: List[dict] = []

        for group in tag_webid_groups:
            if not group:
                continue
            group_reqs: List[dict] = []
            for w in group:
                params: Dict[str, str] = {}
                if time_param:
                    params["time"] = str(time_param)
                group_reqs.append(
                    {
                        "Method": "GET",
                        "Resource": f"/piwebapi/streams/{w}/value",
                        "Parameters": params,
                    }
                )
            responses = self._client.batch_execute(group_reqs)
            for idx, (_rid, resp) in enumerate(responses):
                if resp.get("Status") != 200:
                    continue
                webid = group[idx] if idx < len(group) else None
                if not webid:
                    continue
                v = resp.get("Content", {}) or {}
                ts = v.get("Timestamp")
                out.append(
                    {
                        "tag_webid": webid,
                        "timestamp": parse_ts(ts) if ts else None,
                        "value": try_float(v.get("Value")),
                        "good": as_bool(v.get("Good"), default=True),
                        "questionable": as_bool(v.get("Questionable"), default=False),
                        "substituted": as_bool(v.get("Substituted"), default=False),
                        "annotated": as_bool(v.get("Annotated"), default=False),
                        "units": v.get("UnitsAbbreviation", ""),
                        "ingestion_timestamp": ingest_ts,
                    }
                )

        return out

    def _read_value_at_time(self, table_options: Dict[str, str]) -> List[dict]:
        """Read value at specified time."""
        tag_webids = self._resolve_tag_webids(table_options)
        time_param = table_options.get("time") or "*"
        ingest_ts = utcnow()
        tags_per_request = int(table_options.get("tags_per_request", 0) or 0)
        groups = chunks(tag_webids, tags_per_request) if tags_per_request else [tag_webids]

        out: List[dict] = []
        for group in groups:
            if not group:
                continue
            reqs: List[dict] = [
                {
                    "Method": "GET",
                    "Resource": f"/piwebapi/streams/{w}/value",
                    "Parameters": {"time": str(time_param)},
                }
                for w in group
            ]
            responses = self._client.batch_execute(reqs)
            for idx, (_rid, resp) in enumerate(responses):
                if resp.get("Status") != 200:
                    continue
                wid = group[idx] if idx < len(group) else None
                if not wid:
                    continue
                v = resp.get("Content", {}) or {}
                ts = v.get("Timestamp")
                out.append(
                    {
                        "tag_webid": wid,
                        "timestamp": parse_ts(ts) if ts else None,
                        "value": try_float(v.get("Value")),
                        "good": as_bool(v.get("Good"), default=True),
                        "questionable": as_bool(v.get("Questionable"), default=False),
                        "substituted": as_bool(v.get("Substituted"), default=False),
                        "annotated": as_bool(v.get("Annotated"), default=False),
                        "units": v.get("UnitsAbbreviation", ""),
                        "ingestion_timestamp": ingest_ts,
                    }
                )
        return out

    def _read_recorded_at_time(self, table_options: Dict[str, str]) -> List[dict]:
        """Read recorded value at specified time."""
        tag_webids = self._resolve_tag_webids(table_options)
        time_param = table_options.get("time") or "*"
        ingest_ts = utcnow()

        out: List[dict] = []
        for wid in tag_webids:
            try:
                data = self._client.get_json(
                    f"/piwebapi/streams/{wid}/recordedattime", params={"time": str(time_param)}
                )
            except requests.exceptions.HTTPError as e:
                if getattr(e.response, "status_code", None) == 404:
                    try:
                        data = self._client.get_json(
                            f"/piwebapi/streams/{wid}/value", params={"time": str(time_param)}
                        )
                    except requests.exceptions.HTTPError as e2:
                        if getattr(e2.response, "status_code", None) == 404:
                            continue
                        continue
                else:
                    continue

            ts = data.get("Timestamp")
            try:
                qt = parse_pi_time(str(time_param), now=utcnow())
            except Exception:
                qt = None
            out.append(
                {
                    "tag_webid": wid,
                    "query_time": qt,
                    "timestamp": parse_ts(ts) if ts else None,
                    "value": try_float(data.get("Value")),
                    "good": as_bool(data.get("Good"), default=True),
                    "questionable": as_bool(data.get("Questionable"), default=False),
                    "substituted": as_bool(data.get("Substituted"), default=False),
                    "annotated": as_bool(data.get("Annotated"), default=False),
                    "units": data.get("UnitsAbbreviation", ""),
                    "ingestion_timestamp": ingest_ts,
                }
            )
        return out

    def _read_end(self, table_options: Dict[str, str]) -> List[dict]:
        """Read end values (latest recorded value)."""
        tag_webids = self._resolve_tag_webids(table_options)
        ingest_ts = utcnow()
        out: List[dict] = []
        for wid in tag_webids:
            try:
                v = self._client.get_json(f"/piwebapi/streams/{wid}/end")
            except requests.exceptions.HTTPError as e:
                if getattr(e.response, "status_code", None) == 404:
                    continue
                raise
            ts = v.get("Timestamp")
            out.append(
                {
                    "tag_webid": wid,
                    "timestamp": parse_ts(ts) if ts else None,
                    "value": try_float(v.get("Value")),
                    "good": as_bool(v.get("Good"), default=True),
                    "questionable": as_bool(v.get("Questionable"), default=False),
                    "substituted": as_bool(v.get("Substituted"), default=False),
                    "annotated": as_bool(v.get("Annotated"), default=False),
                    "units": v.get("UnitsAbbreviation", ""),
                    "ingestion_timestamp": ingest_ts,
                }
            )
        return out

    def _read_streamset_end(self, table_options: Dict[str, str]) -> List[dict]:
        """Read end values via StreamSet endpoint."""
        tag_webids = self._resolve_tag_webids(table_options)
        ingest_ts = utcnow()
        tags_per_request = int(table_options.get("tags_per_request", 0) or 0)
        groups = chunks(tag_webids, tags_per_request) if tags_per_request else [tag_webids]

        out: List[dict] = []
        for group in groups:
            if not group:
                continue
            params: List[Tuple[str, str]] = [("webId", w) for w in group]
            try:
                data = self._client.get_json("/piwebapi/streamsets/end", params=params)
            except requests.exceptions.HTTPError as e:
                if getattr(e.response, "status_code", None) == 404:
                    data = None
                else:
                    raise
            if data:
                for stream in data.get("Items", []) or []:
                    wid = stream.get("WebId")
                    if not wid:
                        continue
                    v = stream.get("Value", {}) or {}
                    ts = v.get("Timestamp")
                    out.append(
                        {
                            "tag_webid": wid,
                            "timestamp": parse_ts(ts) if ts else None,
                            "value": try_float(v.get("Value")),
                            "good": as_bool(v.get("Good"), default=True),
                            "questionable": as_bool(v.get("Questionable"), default=False),
                            "substituted": as_bool(v.get("Substituted"), default=False),
                            "annotated": as_bool(v.get("Annotated"), default=False),
                            "units": v.get("UnitsAbbreviation", ""),
                            "ingestion_timestamp": ingest_ts,
                        }
                    )
                continue

            for wid in group:
                try:
                    v = self._client.get_json(f"/piwebapi/streams/{wid}/end")
                except requests.exceptions.HTTPError as e:
                    if getattr(e.response, "status_code", None) == 404:
                        continue
                    raise
                ts = v.get("Timestamp")
                out.append(
                    {
                        "tag_webid": wid,
                        "timestamp": parse_ts(ts) if ts else None,
                        "value": try_float(v.get("Value")),
                        "good": as_bool(v.get("Good"), default=True),
                        "questionable": as_bool(v.get("Questionable"), default=False),
                        "substituted": as_bool(v.get("Substituted"), default=False),
                        "annotated": as_bool(v.get("Annotated"), default=False),
                        "units": v.get("UnitsAbbreviation", ""),
                        "ingestion_timestamp": ingest_ts,
                    }
                )
        return out

    def _read_calculated(
        self, start_offset: dict, table_options: Dict[str, str]
    ) -> Tuple[Iterator[dict], dict]:
        """Read calculated values."""
        tag_webids = self._resolve_tag_webids(table_options)
        calc_type = (
            table_options.get("calculationType")
            or table_options.get("calculation_type")
            or "Average"
        ).strip()
        now = utcnow()

        end_opt = table_options.get("endTime") or table_options.get("end_time") or "*"
        end_dt = parse_pi_time(end_opt, now=now)

        start_dt: Optional[datetime] = None
        if start_offset and isinstance(start_offset, dict):
            off = start_offset.get("offset")
            if isinstance(off, str) and off:
                try:
                    start_dt = parse_ts(off)
                except Exception:
                    start_dt = None

        if start_dt is None:
            start_opt = table_options.get("startTime") or table_options.get("start_time")
            if start_opt:
                start_dt = parse_pi_time(str(start_opt), now=end_dt)
            else:
                lookback_minutes = int(table_options.get("lookback_minutes", 60))
                start_dt = end_dt - timedelta(minutes=lookback_minutes)

        start_str = isoformat_z(start_dt)
        end_str = isoformat_z(end_dt)
        interval = (
            table_options.get("interval") or table_options.get("sampleInterval") or "1m"
        ).strip()
        ingest_ts = utcnow()
        next_offset = {"offset": end_str}

        def iterator() -> Iterator[dict]:
            for wid in tag_webids:
                try:
                    data = self._client.get_json(
                        f"/piwebapi/streams/{wid}/calculated",
                        params={
                            "startTime": start_str,
                            "endTime": end_str,
                            "interval": interval,
                            "calculationType": calc_type,
                        },
                    )
                except requests.exceptions.HTTPError as e:
                    if getattr(e.response, "status_code", None) == 404:
                        try:
                            data = self._client.get_json(
                                f"/piwebapi/streams/{wid}/plot",
                                params={
                                    "startTime": start_str,
                                    "endTime": end_str,
                                    "intervals": "120",
                                },
                            )
                        except requests.exceptions.HTTPError as e2:
                            if getattr(e2.response, "status_code", None) == 404:
                                continue
                            continue
                    else:
                        continue

                for item in data.get("Items") or []:
                    ts = item.get("Timestamp")
                    if not ts:
                        continue
                    yield {
                        "tag_webid": wid,
                        "timestamp": parse_ts(ts),
                        "value": try_float(item.get("Value")),
                        "units": item.get("UnitsAbbreviation", "") or item.get("Units", ""),
                        "calculation_type": calc_type,
                        "ingestion_timestamp": ingest_ts,
                    }

        return iterator(), next_offset

    # =========================================================================
    # Asset Framework readers
    # =========================================================================

    def _read_assetservers(self) -> List[dict]:
        """Read asset servers."""
        data = self._client.get_json("/piwebapi/assetservers")
        return data.get("Items", []) or []

    def _read_assetdatabases(self, assetserver_webid: str) -> List[dict]:
        """Read asset databases for a given asset server."""
        data = self._client.get_json(f"/piwebapi/assetservers/{assetserver_webid}/assetdatabases")
        return data.get("Items", []) or []

    def _read_assetservers_table(self) -> List[dict]:
        """Read asset servers as table rows."""
        try:
            items = self._read_assetservers()
        except requests.exceptions.HTTPError as e:
            if getattr(e.response, "status_code", None) == 404:
                return []
            raise
        out: List[dict] = []
        for s in items or []:
            wid = s.get("WebId")
            if not wid:
                continue
            out.append({"webid": wid, "name": s.get("Name", ""), "path": s.get("Path", "")})
        return out

    def _read_assetdatabases_table(self) -> List[dict]:
        """Read asset databases as table rows."""
        try:
            assetservers = self._read_assetservers()
        except requests.exceptions.HTTPError as e:
            if getattr(e.response, "status_code", None) == 404:
                return []
            raise

        out: List[dict] = []
        for srv in assetservers or []:
            srv_wid = srv.get("WebId")
            if not srv_wid:
                continue
            try:
                dbs = self._read_assetdatabases(srv_wid)
            except requests.exceptions.HTTPError as e:
                if getattr(e.response, "status_code", None) == 404:
                    continue
                raise
            for db in dbs or []:
                db_wid = db.get("WebId")
                if not db_wid:
                    continue
                out.append(
                    {
                        "webid": db_wid,
                        "name": db.get("Name", ""),
                        "path": db.get("Path", ""),
                        "assetserver_webid": srv_wid,
                    }
                )
        return out

    def _read_af_hierarchy(self) -> List[dict]:
        """Read AF element hierarchy."""
        assetservers = self._read_assetservers()
        if not assetservers:
            return []

        out: List[dict] = []
        ingest_ts = utcnow()

        def walk(elements: List[dict], parent_webid: str, depth: int):
            for e in elements:
                webid = e.get("WebId")
                if not webid:
                    continue
                out.append(
                    {
                        "element_webid": webid,
                        "name": e.get("Name", ""),
                        "template_name": e.get("TemplateName", ""),
                        "description": e.get("Description", ""),
                        "path": e.get("Path", ""),
                        "parent_webid": parent_webid or "",
                        "depth": depth,
                        "category_names": e.get("CategoryNames") or [],
                        "ingestion_timestamp": ingest_ts,
                    }
                )
                children = e.get("Elements") or []
                if children:
                    walk(children, webid, depth + 1)

        for srv in assetservers:
            srv_webid = srv.get("WebId")
            if not srv_webid:
                continue
            for db in self._read_assetdatabases(srv_webid):
                db_webid = db.get("WebId")
                if not db_webid:
                    continue
                roots = (
                    self._client.get_json(
                        f"/piwebapi/assetdatabases/{db_webid}/elements",
                        params={"searchFullHierarchy": "true"},
                    ).get("Items", [])
                    or []
                )
                walk(roots, parent_webid="", depth=0)

        return out

    def _read_element_attributes(self, table_options: Dict[str, str]) -> List[dict]:
        """Read element attributes."""
        element_csv = table_options.get("element_webids", "")
        element_webids = [e.strip() for e in str(element_csv).split(",") if e.strip()]
        if not element_webids:
            af = self._read_af_hierarchy()
            element_webids = [
                r.get("element_webid")
                for r in af[: int(table_options.get("default_elements", 10))]
                if r.get("element_webid")
            ]

        name_filter = table_options.get("nameFilter")
        page_size = int(table_options.get("maxCount", 1000))
        start_index = int(table_options.get("startIndex", 0))

        ingest_ts = utcnow()
        out: List[dict] = []
        for ew in element_webids:
            params: Dict[str, str] = {"maxCount": str(page_size), "startIndex": str(start_index)}
            if name_filter:
                params["nameFilter"] = str(name_filter)
            data = self._client.get_json(f"/piwebapi/elements/{ew}/attributes", params=params)
            for a in data.get("Items", []) or []:
                aw = a.get("WebId")
                if not aw:
                    continue
                out.append(
                    {
                        "element_webid": ew,
                        "attribute_webid": aw,
                        "name": a.get("Name", ""),
                        "description": a.get("Description", ""),
                        "path": a.get("Path", ""),
                        "type": a.get("Type", ""),
                        "default_units_name": a.get("DefaultUnitsName", ""),
                        "data_reference_plugin": a.get("DataReferencePlugIn", ""),
                        "is_configuration_item": as_bool(
                            a.get("IsConfigurationItem"), default=False
                        ),
                        "ingestion_timestamp": ingest_ts,
                    }
                )
        return out

    def _read_element_templates_table(self, table_options: Dict[str, str]) -> List[dict]:
        """Read element templates."""
        out: List[dict] = []
        db_wid_opt = (table_options.get("assetdatabase_webid") or "").strip()
        if db_wid_opt:
            db_wids = [db_wid_opt]
        else:
            db_wids = [d.get("webid") for d in self._read_assetdatabases_table() if d.get("webid")]

        for db_wid in db_wids:
            try:
                data = self._client.get_json(f"/piwebapi/assetdatabases/{db_wid}/elementtemplates")
            except requests.exceptions.HTTPError as e:
                if getattr(e.response, "status_code", None) == 404:
                    continue
                raise
            for it in data.get("Items") or []:
                wid = it.get("WebId")
                if not wid:
                    continue
                out.append(
                    {
                        "webid": wid,
                        "name": it.get("Name", ""),
                        "description": it.get("Description", ""),
                        "path": it.get("Path", ""),
                        "assetdatabase_webid": db_wid,
                    }
                )
        return out

    def _read_element_template_attributes_table(self, table_options: Dict[str, str]) -> List[dict]:
        """Read element template attributes."""
        out: List[dict] = []
        db_wid_opt = (table_options.get("assetdatabase_webid") or "").strip()
        tpl_wid_opt = (table_options.get("elementtemplate_webid") or "").strip()

        template_pairs: List[Tuple[str, str]] = []
        if tpl_wid_opt:
            template_pairs = [(tpl_wid_opt, db_wid_opt)]
        else:
            templates = self._read_element_templates_table(
                {"assetdatabase_webid": db_wid_opt} if db_wid_opt else {}
            )
            for t in templates:
                tw = t.get("webid")
                if not tw:
                    continue
                template_pairs.append((tw, t.get("assetdatabase_webid") or db_wid_opt))

        for tpl_wid, db_wid in template_pairs:
            try:
                data = self._client.get_json(
                    f"/piwebapi/elementtemplates/{tpl_wid}/attributetemplates"
                )
            except requests.exceptions.HTTPError as e:
                if getattr(e.response, "status_code", None) == 404:
                    continue
                raise
            for it in data.get("Items") or []:
                wid = it.get("WebId")
                if not wid:
                    continue
                out.append(
                    {
                        "webid": wid,
                        "name": it.get("Name", ""),
                        "description": it.get("Description", ""),
                        "path": it.get("Path", ""),
                        "type": it.get("Type", "") or it.get("AttributeType", ""),
                        "default_units_name": it.get("DefaultUnitsName", "")
                        or it.get("DefaultUnits", ""),
                        "data_reference_plugin": it.get("DataReferencePlugin", "")
                        or it.get("DataReference", ""),
                        "is_configuration_item": as_bool(
                            it.get("IsConfigurationItem"), default=False
                        ),
                        "elementtemplate_webid": tpl_wid,
                        "assetdatabase_webid": db_wid or "",
                    }
                )
        return out

    def _read_categories_table(self, table_options: Dict[str, str]) -> List[dict]:
        """Read AF categories."""
        out: List[dict] = []
        db_wid_opt = (table_options.get("assetdatabase_webid") or "").strip()
        if db_wid_opt:
            db_wids = [db_wid_opt]
        else:
            db_wids = [d.get("webid") for d in self._read_assetdatabases_table() if d.get("webid")]

        for db_wid in db_wids:
            try:
                data = self._client.get_json(f"/piwebapi/assetdatabases/{db_wid}/categories")
            except requests.exceptions.HTTPError as e:
                if getattr(e.response, "status_code", None) == 404:
                    continue
                raise
            for it in data.get("Items") or []:
                wid = it.get("WebId")
                if not wid:
                    continue
                out.append(
                    {
                        "webid": wid,
                        "name": it.get("Name", ""),
                        "description": it.get("Description", ""),
                        "category_type": it.get("CategoryType", "") or it.get("Type", ""),
                        "assetdatabase_webid": db_wid,
                    }
                )
        return out

    def _read_attribute_templates_table(self, table_options: Dict[str, str]) -> List[dict]:
        """Read AF attribute templates for element templates."""
        out: List[dict] = []
        db_wid_opt = (table_options.get("assetdatabase_webid") or "").strip()

        templates = self._read_element_templates_table(
            {"assetdatabase_webid": db_wid_opt} if db_wid_opt else {}
        )
        for tpl in templates:
            tpl_wid = tpl.get("webid")
            if not tpl_wid:
                continue
            db_wid = tpl.get("assetdatabase_webid") or db_wid_opt or ""
            try:
                data = self._client.get_json(
                    f"/piwebapi/elementtemplates/{tpl_wid}/attributetemplates"
                )
            except requests.exceptions.HTTPError as e:
                if getattr(e.response, "status_code", None) == 404:
                    continue
                raise
            for it in data.get("Items") or []:
                wid = it.get("WebId")
                if not wid:
                    continue
                out.append(
                    {
                        "webid": wid,
                        "name": it.get("Name", ""),
                        "description": it.get("Description", ""),
                        "path": it.get("Path", ""),
                        "type": it.get("Type", "") or it.get("AttributeType", ""),
                        "elementtemplate_webid": tpl_wid,
                        "assetdatabase_webid": db_wid,
                    }
                )
        return out

    def _read_analyses_table(self, table_options: Dict[str, str]) -> List[dict]:
        """Read AF analyses."""
        out: List[dict] = []
        db_wid_opt = (table_options.get("assetdatabase_webid") or "").strip()
        if db_wid_opt:
            db_wids = [db_wid_opt]
        else:
            db_wids = [d.get("webid") for d in self._read_assetdatabases_table() if d.get("webid")]

        for db_wid in db_wids:
            try:
                data = self._client.get_json(f"/piwebapi/assetdatabases/{db_wid}/analyses")
            except requests.exceptions.HTTPError as e:
                if getattr(e.response, "status_code", None) == 404:
                    continue
                raise
            for it in data.get("Items") or []:
                wid = it.get("WebId")
                if not wid:
                    continue
                out.append(
                    {
                        "webid": wid,
                        "name": it.get("Name", ""),
                        "description": it.get("Description", ""),
                        "path": it.get("Path", ""),
                        "analysis_template_name": it.get("AnalysisTemplateName", "")
                        or it.get("TemplateName", ""),
                        "target_element_webid": it.get("TargetElementWebId", "")
                        or it.get("TargetElement", ""),
                        "assetdatabase_webid": db_wid,
                    }
                )
        return out

    def _read_analysis_templates_table(self, table_options: Dict[str, str]) -> List[dict]:
        """Read analysis templates."""
        out: List[dict] = []
        db_wid_opt = (table_options.get("assetdatabase_webid") or "").strip()
        if db_wid_opt:
            db_wids = [db_wid_opt]
        else:
            db_wids = [d.get("webid") for d in self._read_assetdatabases_table() if d.get("webid")]

        for db_wid in db_wids:
            try:
                data = self._client.get_json(f"/piwebapi/assetdatabases/{db_wid}/analysistemplates")
            except requests.exceptions.HTTPError as e:
                if getattr(e.response, "status_code", None) == 404:
                    continue
                raise
            for it in data.get("Items") or []:
                wid = it.get("WebId")
                if not wid:
                    continue
                out.append(
                    {
                        "webid": wid,
                        "name": it.get("Name", ""),
                        "description": it.get("Description", ""),
                        "path": it.get("Path", ""),
                        "assetdatabase_webid": db_wid,
                    }
                )
        return out

    def _read_af_tables_table(self, table_options: Dict[str, str]) -> List[dict]:
        """Read AF tables."""
        out: List[dict] = []
        db_wid_opt = (table_options.get("assetdatabase_webid") or "").strip()
        if db_wid_opt:
            db_wids = [db_wid_opt]
        else:
            db_wids = [d.get("webid") for d in self._read_assetdatabases_table() if d.get("webid")]

        for db_wid in db_wids:
            try:
                data = self._client.get_json(f"/piwebapi/assetdatabases/{db_wid}/tables")
            except requests.exceptions.HTTPError as e:
                if getattr(e.response, "status_code", None) == 404:
                    continue
                raise
            for it in data.get("Items") or []:
                wid = it.get("WebId")
                if not wid:
                    continue
                out.append(
                    {
                        "webid": wid,
                        "name": it.get("Name", ""),
                        "description": it.get("Description", ""),
                        "path": it.get("Path", ""),
                        "assetdatabase_webid": db_wid,
                    }
                )
        return out

    def _read_af_table_rows_table(self, table_options: Dict[str, str]) -> List[dict]:
        """Read rows from AF tables."""
        start_index = int(table_options.get("startIndex", 0) or 0)
        max_count = int(table_options.get("maxCount", 50) or 50)
        default_tables = int(table_options.get("default_tables", 1) or 1)
        ingest_ts = utcnow()

        table_webid = (table_options.get("table_webid") or "").strip()
        table_webids_csv = (table_options.get("table_webids") or "").strip()
        table_webids: List[str] = []
        if table_webid:
            table_webids = [table_webid]
        elif table_webids_csv:
            table_webids = [s.strip() for s in table_webids_csv.split(",") if s.strip()]
        else:
            tables = self._read_af_tables_table(table_options)
            table_webids = [t.get("webid") for t in tables if t.get("webid")][
                : max(0, default_tables)
            ]

        out: List[dict] = []
        for tw in table_webids:
            try:
                data = self._client.get_json(
                    f"/piwebapi/tables/{tw}/rows",
                    params={"startIndex": str(start_index), "maxCount": str(max_count)},
                )
            except requests.exceptions.HTTPError as e:
                if getattr(e.response, "status_code", None) == 404:
                    continue
                raise
            items = data.get("Items", []) or []
            for i, row in enumerate(items):
                cols = row.get("Columns") or row.get("columns") or {}
                cols_norm = {str(k): ("" if v is None else str(v)) for k, v in (cols or {}).items()}
                ridx = row.get("Index")
                if ridx is None:
                    ridx = start_index + i
                out.append(
                    {
                        "table_webid": tw,
                        "row_index": int(ridx) if str(ridx).isdigit() else None,
                        "columns": cols_norm,
                        "ingestion_timestamp": ingest_ts,
                    }
                )
        return out

    def _read_units_of_measure_table(self) -> List[dict]:
        """Read units of measure."""
        try:
            data = self._client.get_json("/piwebapi/uoms")
        except requests.exceptions.HTTPError as e:
            if getattr(e.response, "status_code", None) == 404:
                return []
            raise
        out: List[dict] = []
        for it in data.get("Items") or []:
            wid = it.get("WebId")
            if not wid:
                continue
            out.append(
                {
                    "webid": wid,
                    "name": it.get("Name", ""),
                    "abbreviation": it.get("Abbreviation", "") or it.get("Symbol", ""),
                    "quantity_type": it.get("QuantityType", "") or it.get("Quantity", ""),
                }
            )
        return out

    # =========================================================================
    # Event Frame readers
    # =========================================================================

    def _read_event_frames(  # pylint: disable=too-many-locals
        self, start_offset: dict, table_options: Dict[str, str]
    ) -> Tuple[Iterator[dict], dict]:
        """Read event frames."""
        now = utcnow()

        start = None
        if start_offset and isinstance(start_offset, dict):
            off = start_offset.get("offset")
            if isinstance(off, str) and off:
                try:
                    start = parse_ts(off)
                except Exception:
                    start = None
        if start is None:
            lookback_days = int(table_options.get("lookback_days", 30))
            start = now - timedelta(days=lookback_days)

        start_str = isoformat_z(start)
        end_str = isoformat_z(now)

        page_size = int(table_options.get("maxCount", 1000))
        base_start_index = int(table_options.get("startIndex", 0))
        search_mode = table_options.get("searchMode", "Overlapped")

        assetservers = self._read_assetservers()
        if not assetservers:
            return iter(()), {"offset": end_str}

        all_events: List[dict] = []

        for srv in assetservers:
            srv_webid = srv.get("WebId")
            if not srv_webid:
                continue
            for db in self._read_assetdatabases(srv_webid):
                db_webid = db.get("WebId")
                if not db_webid:
                    continue

                start_index = base_start_index
                while True:
                    params = {
                        "startTime": start_str,
                        "endTime": end_str,
                        "searchMode": str(search_mode),
                        "startIndex": str(start_index),
                        "maxCount": str(page_size),
                    }
                    resp = self._client.get_json(
                        f"/piwebapi/assetdatabases/{db_webid}/eventframes", params=params
                    )
                    items = resp.get("Items", []) or []
                    all_events.extend(items)
                    if len(items) < page_size:
                        break
                    start_index += page_size

        ingest_ts = utcnow()

        def iterator() -> Iterator[dict]:
            for ef in all_events:
                webid = ef.get("WebId")
                if not webid:
                    continue
                raw_attrs = ef.get("Attributes") or {}
                attrs = {str(k): ("" if v is None else str(v)) for k, v in raw_attrs.items()}
                yield {
                    "event_frame_webid": webid,
                    "name": ef.get("Name", ""),
                    "template_name": ef.get("TemplateName", ""),
                    "start_time": parse_ts(ef.get("StartTime")) if ef.get("StartTime") else None,
                    "end_time": parse_ts(ef.get("EndTime")) if ef.get("EndTime") else None,
                    "primary_referenced_element_webid": ef.get("PrimaryReferencedElementWebId"),
                    "description": ef.get("Description", ""),
                    "category_names": ef.get("CategoryNames") or [],
                    "attributes": attrs,
                    "ingestion_timestamp": ingest_ts,
                }

        return iterator(), {"offset": end_str}

    def _read_eventframe_attributes(self, table_options: Dict[str, str]) -> List[dict]:
        """Read event frame attributes."""
        ef_csv = table_options.get("event_frame_webids", "")
        ef_webids = [e.strip() for e in str(ef_csv).split(",") if e.strip()]
        if not ef_webids:
            records, _ = self._read_event_frames(
                {}, {"lookback_days": table_options.get("lookback_days", 30)}
            )
            tmp = []
            for i, r in enumerate(records):
                tmp.append(r)
                if i >= int(table_options.get("default_event_frames", 10)) - 1:
                    break
            ef_webids = [r.get("event_frame_webid") for r in tmp if r.get("event_frame_webid")]

        name_filter = table_options.get("nameFilter")
        page_size = int(table_options.get("maxCount", 1000))
        start_index = int(table_options.get("startIndex", 0))

        ingest_ts = utcnow()
        out: List[dict] = []
        for efw in ef_webids:
            params: Dict[str, str] = {"maxCount": str(page_size), "startIndex": str(start_index)}
            if name_filter:
                params["nameFilter"] = str(name_filter)
            data = self._client.get_json(f"/piwebapi/eventframes/{efw}/attributes", params=params)
            for a in data.get("Items", []) or []:
                aw = a.get("WebId")
                if not aw:
                    continue
                out.append(
                    {
                        "event_frame_webid": efw,
                        "attribute_webid": aw,
                        "name": a.get("Name", ""),
                        "description": a.get("Description", ""),
                        "path": a.get("Path", ""),
                        "type": a.get("Type", ""),
                        "default_units_name": a.get("DefaultUnitsName", ""),
                        "data_reference_plugin": a.get("DataReferencePlugIn", ""),
                        "is_configuration_item": as_bool(
                            a.get("IsConfigurationItem"), default=False
                        ),
                        "ingestion_timestamp": ingest_ts,
                    }
                )
        return out

    def _read_eventframe_templates_table(self, table_options: Dict[str, str]) -> List[dict]:
        """Read event frame templates."""
        out: List[dict] = []
        db_wid_opt = (table_options.get("assetdatabase_webid") or "").strip()
        if db_wid_opt:
            db_wids = [db_wid_opt]
        else:
            db_wids = [d.get("webid") for d in self._read_assetdatabases_table() if d.get("webid")]

        for db_wid in db_wids:
            try:
                data = self._client.get_json(
                    f"/piwebapi/assetdatabases/{db_wid}/eventframetemplates"
                )
            except requests.exceptions.HTTPError as e:
                if getattr(e.response, "status_code", None) == 404:
                    continue
                raise
            for it in data.get("Items") or []:
                wid = it.get("WebId")
                if not wid:
                    continue
                out.append(
                    {
                        "webid": wid,
                        "name": it.get("Name", ""),
                        "description": it.get("Description", ""),
                        "path": it.get("Path", ""),
                        "assetdatabase_webid": db_wid,
                    }
                )
        return out

    def _read_eventframe_template_attributes_table(
        self, table_options: Dict[str, str]
    ) -> List[dict]:
        """Read event frame template attributes."""
        out: List[dict] = []
        db_wid_opt = (table_options.get("assetdatabase_webid") or "").strip()
        tpl_wid_opt = (table_options.get("eventframe_template_webid") or "").strip()

        template_pairs: List[Tuple[str, str]] = []
        if tpl_wid_opt:
            template_pairs = [(tpl_wid_opt, db_wid_opt)]
        else:
            templates = self._read_eventframe_templates_table(
                {"assetdatabase_webid": db_wid_opt} if db_wid_opt else {}
            )
            for t in templates:
                tw = t.get("webid")
                if not tw:
                    continue
                template_pairs.append((tw, t.get("assetdatabase_webid") or db_wid_opt))

        for tpl_wid, db_wid in template_pairs:
            try:
                data = self._client.get_json(
                    f"/piwebapi/eventframetemplates/{tpl_wid}/attributetemplates"
                )
            except requests.exceptions.HTTPError as e:
                if getattr(e.response, "status_code", None) == 404:
                    continue
                raise
            for it in data.get("Items") or []:
                wid = it.get("WebId")
                if not wid:
                    continue
                out.append(
                    {
                        "webid": wid,
                        "name": it.get("Name", ""),
                        "description": it.get("Description", ""),
                        "path": it.get("Path", ""),
                        "type": it.get("Type", "") or it.get("AttributeType", ""),
                        "eventframe_template_webid": tpl_wid,
                        "assetdatabase_webid": db_wid or "",
                    }
                )
        return out

    def _read_eventframe_referenced_elements(
        self, start_offset: dict, table_options: Dict[str, str]
    ) -> Tuple[Iterator[dict], dict]:
        """Read event frame referenced elements."""
        it, next_offset = self._read_event_frames(start_offset, table_options)
        ingest_ts = utcnow()

        def iterator() -> Iterator[dict]:
            for ef in it:
                ef_wid = ef.get("event_frame_webid")
                if not ef_wid:
                    continue
                st = ef.get("start_time")
                et = ef.get("end_time")
                primary = ef.get("primary_referenced_element_webid")
                if primary:
                    yield {
                        "event_frame_webid": ef_wid,
                        "element_webid": primary,
                        "relationship_type": "primary",
                        "start_time": st,
                        "end_time": et,
                        "ingestion_timestamp": ingest_ts,
                    }

                try:
                    data = self._client.get_json(
                        f"/piwebapi/eventframes/{ef_wid}/referencedelements"
                    )
                except requests.exceptions.HTTPError as e:
                    if getattr(e.response, "status_code", None) == 404:
                        continue
                    raise
                for el in data.get("Items") or []:
                    ew = el.get("WebId")
                    if not ew or ew == primary:
                        continue
                    yield {
                        "event_frame_webid": ef_wid,
                        "element_webid": ew,
                        "relationship_type": "referenced",
                        "start_time": st,
                        "end_time": et,
                        "ingestion_timestamp": ingest_ts,
                    }

        return iterator(), next_offset

    def _read_eventframe_acknowledgements_table(self, table_options: Dict[str, str]) -> List[dict]:
        """Read event frame acknowledgements."""
        ingest_ts = utcnow()
        out: List[dict] = []

        try:
            it, _ = self._read_event_frames({}, table_options)
        except Exception:
            return []

        max_elems = int(table_options.get("default_event_frames", 25) or 25)
        for i, ef in enumerate(it):
            if i >= max_elems:
                break
            ef_wid = ef.get("event_frame_webid")
            if not ef_wid:
                continue
            try:
                data = self._client.get_json(f"/piwebapi/eventframes/{ef_wid}/acknowledgements")
            except requests.exceptions.HTTPError as e:
                if getattr(e.response, "status_code", None) == 404:
                    continue
                continue
            for item in data.get("Items") or []:
                ack_id = item.get("Id") or item.get("WebId") or item.get("AckId")
                if not ack_id:
                    continue
                ts = item.get("Timestamp")
                out.append(
                    {
                        "event_frame_webid": ef_wid,
                        "ack_id": str(ack_id),
                        "ack_timestamp": parse_ts(ts) if ts else None,
                        "ack_user": item.get("User") or item.get("AcknowledgedBy") or "",
                        "comment": item.get("Comment") or "",
                        "ingestion_timestamp": ingest_ts,
                    }
                )
        return out

    def _read_eventframe_annotations_table(self, table_options: Dict[str, str]) -> List[dict]:
        """Read event frame annotations."""
        ingest_ts = utcnow()
        out: List[dict] = []

        try:
            it, _ = self._read_event_frames({}, table_options)
        except Exception:
            return []

        max_elems = int(table_options.get("default_event_frames", 25) or 25)
        for i, ef in enumerate(it):
            if i >= max_elems:
                break
            ef_wid = ef.get("event_frame_webid")
            if not ef_wid:
                continue
            try:
                data = self._client.get_json(f"/piwebapi/eventframes/{ef_wid}/annotations")
            except requests.exceptions.HTTPError as e:
                if getattr(e.response, "status_code", None) == 404:
                    continue
                continue
            for item in data.get("Items") or []:
                ann_id = item.get("Id") or item.get("WebId") or item.get("AnnotationId")
                if not ann_id:
                    continue
                ts = item.get("Timestamp")
                out.append(
                    {
                        "event_frame_webid": ef_wid,
                        "annotation_id": str(ann_id),
                        "annotation_timestamp": parse_ts(ts) if ts else None,
                        "annotation_user": item.get("User") or item.get("CreatedBy") or "",
                        "text": item.get("Text") or item.get("Message") or "",
                        "ingestion_timestamp": ingest_ts,
                    }
                )
        return out

    # =========================================================================
    # Governance & Diagnostics readers
    # =========================================================================

    def _read_links(self, table_options: Dict[str, str]) -> List[dict]:
        """Materialize Links fields by sampling key resources."""
        out: List[dict] = []

        def add(entity_type: str, webid: str, links: Any):
            if not webid or not isinstance(links, dict):
                return
            for rel, href in links.items():
                if href is None:
                    continue
                out.append(
                    {"entity_type": entity_type, "webid": webid, "rel": str(rel), "href": str(href)}
                )

        # Dataservers
        try:
            data = self._client.get_json("/piwebapi/dataservers")
            for it in data.get("Items") or []:
                wid = it.get("WebId")
                add("dataserver", wid, it.get("Links"))
        except Exception:
            pass

        # AssetServers
        try:
            data = self._client.get_json("/piwebapi/assetservers")
            for it in data.get("Items") or []:
                wid = it.get("WebId")
                add("assetserver", wid, it.get("Links"))
        except Exception:
            pass

        # AF Tables
        try:
            for t in self._read_af_tables_table(table_options):
                wid = t.get("webid")
                if wid:
                    out.append(
                        {
                            "entity_type": "aftable",
                            "webid": wid,
                            "rel": "rows",
                            "href": f"/piwebapi/tables/{wid}/rows",
                        }
                    )
        except Exception:
            pass

        return out
