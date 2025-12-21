# OSIPI Lakeflow Community Connector (Python Data Source)
#
# Implements the LakeflowConnect interface expected by the Lakeflow Community Connectors template.
#
# This source reads from an OSI PI Web API-compatible endpoint (including the Databricks App mock server)
# and exposes multiple logical tables via the `tableName` option.
#
# Authentication (Databricks Apps)
# -------------------------------
# Databricks Apps commonly require an OAuth access token minted via the workspace OIDC endpoint:
#
#   POST https://<workspace-host>/oidc/v1/token
#     grant_type=client_credentials
#     scope=all-apis
#
# Options supported:
# - pi_base_url / pi_web_api_url (required): base URL, e.g. https://osipi-webserver-...aws.databricksapps.com
# - workspace_host: https://<workspace-host> (required for OIDC token mint)
# - client_id: service principal applicationId
# - client_secret: service principal secret
# - access_token: (optional) pre-minted bearer token
# - username/password: (optional) basic auth for non-App PI servers
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

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterator, List, Optional, Tuple

import requests
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _isoformat_z(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))

def _parse_pi_time(value: Optional[str], now: Optional[datetime] = None) -> datetime:
    """
    Parse PI Web API time expressions commonly used in query params.

    Supports:
    - "*" (now)
    - "*-10m", "*-2h", "*-7d" (relative to now)
    - ISO timestamps with or without Z suffix
    """
    now_dt = now or _utcnow()
    if value is None or value == "" or value == "*":
        return now_dt

    v = str(value).strip()
    if v.startswith("*-") and len(v) >= 4:
        num = v[2:-1]
        unit = v[-1]
        try:
            n = int(num)
            if unit == "m":
                return now_dt - timedelta(minutes=n)
            if unit == "h":
                return now_dt - timedelta(hours=n)
            if unit == "d":
                return now_dt - timedelta(days=n)
        except Exception:
            pass

    try:
        dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return now_dt


def _chunks(items: List[str], n: int) -> List[List[str]]:
    if n <= 0:
        return [items]
    return [items[i : i + n] for i in range(0, len(items), n)]



def _as_bool(v: Any, default: bool = False) -> bool:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    s = str(v).strip().lower()
    if s in ("true", "t", "1", "yes", "y"):
        return True
    if s in ("false", "f", "0", "no", "n"):
        return False
    return default


def _try_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def _batch_request_dict(requests_list: List[dict]) -> dict:
    """PI Web API docs define the batch request body as a dictionary keyed by ids."""
    return {str(i + 1): req for i, req in enumerate(requests_list)}


def _batch_response_items(resp_json: dict) -> List[Tuple[str, dict]]:
    """Normalize PI Web API batch response into a list of (request_id, response_obj)."""
    if not isinstance(resp_json, dict):
        return []

    # Official PI Web API batch response is a dictionary keyed by request ids.
    # Some mocks use {"Responses": [...]}.
    if "Responses" in resp_json and isinstance(resp_json.get("Responses"), list):
        return [(str(i + 1), r) for i, r in enumerate(resp_json.get("Responses") or [])]

    # Otherwise treat top-level keys as request ids.
    out = []
    for k, v in resp_json.items():
        if isinstance(v, dict) and ("Status" in v or "Content" in v or "Headers" in v):
            out.append((str(k), v))
    # preserve numeric ordering when possible
    def keyfn(x):
        try:
            return int(x[0])
        except Exception:
            return 10**18
    return sorted(out, key=keyfn)


class LakeflowConnect:
    TABLE_DATASERVERS = "pi_dataservers"
    TABLE_POINTS = "pi_points"
    TABLE_POINT_ATTRIBUTES = "pi_point_attributes"
    TABLE_TIMESERIES = "pi_timeseries"
    TABLE_INTERPOLATED = "pi_interpolated"
    TABLE_PLOT = "pi_plot"
    TABLE_AF_HIERARCHY = "pi_af_hierarchy"
    TABLE_EVENT_FRAMES = "pi_event_frames"

    TABLE_CURRENT_VALUE = "pi_current_value"
    TABLE_SUMMARY = "pi_summary"
    TABLE_STREAMSET_RECORDED = "pi_streamset_recorded"
    TABLE_STREAMSET_INTERPOLATED = "pi_streamset_interpolated"
    TABLE_STREAMSET_SUMMARY = "pi_streamset_summary"
    TABLE_ELEMENT_ATTRIBUTES = "pi_element_attributes"
    TABLE_EVENTFRAME_ATTRIBUTES = "pi_eventframe_attributes"
    TABLE_ASSET_SERVERS = "pi_assetservers"
    TABLE_ASSET_DATABASES = "pi_assetdatabases"
    TABLE_ELEMENT_TEMPLATES = "pi_element_templates"

    def __init__(self, options: Dict[str, str]) -> None:
        self.options = options
        self.base_url = (options.get("pi_base_url") or options.get("pi_web_api_url") or "").rstrip("/")
        if not self.base_url:
            raise ValueError("Missing required option: pi_base_url (or pi_web_api_url)")

        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})
        self.verify_ssl = _as_bool(options.get("verify_ssl"), default=True)
        self._auth_resolved = False

    def list_tables(self) -> List[str]:
        return [
            self.TABLE_DATASERVERS,
            self.TABLE_POINTS,
            self.TABLE_POINT_ATTRIBUTES,
            self.TABLE_TIMESERIES,
            self.TABLE_INTERPOLATED,
            self.TABLE_PLOT,
            self.TABLE_AF_HIERARCHY,
            self.TABLE_EVENT_FRAMES,
            self.TABLE_CURRENT_VALUE,
            self.TABLE_SUMMARY,
            self.TABLE_STREAMSET_RECORDED,
            self.TABLE_STREAMSET_INTERPOLATED,
            self.TABLE_STREAMSET_SUMMARY,
            self.TABLE_ELEMENT_ATTRIBUTES,
            self.TABLE_EVENTFRAME_ATTRIBUTES,
            self.TABLE_ASSET_SERVERS,
            self.TABLE_ASSET_DATABASES,
            self.TABLE_ELEMENT_TEMPLATES,
        ]

    def get_table_schema(self, table_name: str, table_options: Dict[str, str]) -> StructType:
        if table_name == self.TABLE_DATASERVERS:
            return StructType([
                StructField("webid", StringType(), False),
                StructField("name", StringType(), True),
            ])

        if table_name == self.TABLE_POINTS:
            return StructType([
                StructField("webid", StringType(), False),
                StructField("name", StringType(), True),
                StructField("descriptor", StringType(), True),
                StructField("engineering_units", StringType(), True),
                StructField("path", StringType(), True),
                StructField("dataserver_webid", StringType(), True),
            ])

        if table_name == self.TABLE_POINT_ATTRIBUTES:
            return StructType([
                StructField("point_webid", StringType(), False),
                StructField("name", StringType(), True),
                StructField("value", StringType(), True),
                StructField("type", StringType(), True),
                StructField("ingestion_timestamp", TimestampType(), False),
            ])

        if table_name in (
            self.TABLE_TIMESERIES,
            self.TABLE_STREAMSET_RECORDED,
            self.TABLE_INTERPOLATED,
            self.TABLE_STREAMSET_INTERPOLATED,
            self.TABLE_PLOT,
        ):
            return StructType([
                StructField("tag_webid", StringType(), False),
                StructField("timestamp", TimestampType(), False),
                StructField("value", DoubleType(), True),
                StructField("good", BooleanType(), True),
                StructField("questionable", BooleanType(), True),
                StructField("substituted", BooleanType(), True),
                StructField("annotated", BooleanType(), True),
                StructField("units", StringType(), True),
                StructField("ingestion_timestamp", TimestampType(), False),
            ])

        if table_name == self.TABLE_CURRENT_VALUE:
            return StructType([
                StructField("tag_webid", StringType(), False),
                StructField("timestamp", TimestampType(), True),
                StructField("value", DoubleType(), True),
                StructField("good", BooleanType(), True),
                StructField("questionable", BooleanType(), True),
                StructField("substituted", BooleanType(), True),
                StructField("annotated", BooleanType(), True),
                StructField("units", StringType(), True),
                StructField("ingestion_timestamp", TimestampType(), False),
            ])

        if table_name == self.TABLE_SUMMARY:
            return StructType([
                StructField("tag_webid", StringType(), False),
                StructField("summary_type", StringType(), False),
                StructField("timestamp", TimestampType(), True),
                StructField("value", DoubleType(), True),
                StructField("good", BooleanType(), True),
                StructField("questionable", BooleanType(), True),
                StructField("substituted", BooleanType(), True),
                StructField("annotated", BooleanType(), True),
                StructField("units", StringType(), True),
                StructField("ingestion_timestamp", TimestampType(), False),
            ])

        if table_name == self.TABLE_STREAMSET_SUMMARY:
            return StructType([
                StructField("tag_webid", StringType(), False),
                StructField("summary_type", StringType(), False),
                StructField("timestamp", TimestampType(), False),
                StructField("value", DoubleType(), True),
                StructField("good", BooleanType(), True),
                StructField("questionable", BooleanType(), True),
                StructField("substituted", BooleanType(), True),
                StructField("annotated", BooleanType(), True),
                StructField("units", StringType(), True),
                StructField("ingestion_timestamp", TimestampType(), False),
            ])

        if table_name == self.TABLE_AF_HIERARCHY:
            return StructType([
                StructField("element_webid", StringType(), False),
                StructField("name", StringType(), True),
                StructField("template_name", StringType(), True),
                StructField("description", StringType(), True),
                StructField("path", StringType(), True),
                StructField("parent_webid", StringType(), True),
                StructField("depth", LongType(), True),
                StructField("category_names", ArrayType(StringType()), True),
                StructField("ingestion_timestamp", TimestampType(), False),
            ])

        if table_name == self.TABLE_EVENT_FRAMES:
            return StructType([
                StructField("event_frame_webid", StringType(), False),
                StructField("name", StringType(), True),
                StructField("template_name", StringType(), True),
                StructField("start_time", TimestampType(), True),
                StructField("end_time", TimestampType(), True),
                StructField("primary_referenced_element_webid", StringType(), True),
                StructField("description", StringType(), True),
                StructField("category_names", ArrayType(StringType()), True),
                StructField("attributes", MapType(StringType(), StringType()), True),
                StructField("ingestion_timestamp", TimestampType(), False),
            ])

        if table_name == self.TABLE_ELEMENT_ATTRIBUTES:
            return StructType([
                StructField("element_webid", StringType(), False),
                StructField("attribute_webid", StringType(), False),
                StructField("name", StringType(), True),
                StructField("description", StringType(), True),
                StructField("path", StringType(), True),
                StructField("type", StringType(), True),
                StructField("default_units_name", StringType(), True),
                StructField("data_reference_plugin", StringType(), True),
                StructField("is_configuration_item", BooleanType(), True),
                StructField("ingestion_timestamp", TimestampType(), False),
            ])

        if table_name == self.TABLE_EVENTFRAME_ATTRIBUTES:
            return StructType([
                StructField("event_frame_webid", StringType(), False),
                StructField("attribute_webid", StringType(), False),
                StructField("name", StringType(), True),
                StructField("description", StringType(), True),
                StructField("path", StringType(), True),
                StructField("type", StringType(), True),
                StructField("default_units_name", StringType(), True),
                StructField("data_reference_plugin", StringType(), True),
                StructField("is_configuration_item", BooleanType(), True),
                StructField("ingestion_timestamp", TimestampType(), False),
            ])

        if table_name == self.TABLE_ASSET_SERVERS:
            return StructType([
                StructField("webid", StringType(), False),
                StructField("name", StringType(), True),
                StructField("path", StringType(), True),
            ])

        if table_name == self.TABLE_ASSET_DATABASES:
            return StructType([
                StructField("webid", StringType(), False),
                StructField("name", StringType(), True),
                StructField("path", StringType(), True),
                StructField("assetserver_webid", StringType(), True),
            ])

        if table_name == self.TABLE_ELEMENT_TEMPLATES:
            return StructType([
                StructField("webid", StringType(), False),
                StructField("name", StringType(), True),
                StructField("description", StringType(), True),
                StructField("path", StringType(), True),
                StructField("assetdatabase_webid", StringType(), True),
            ])

        raise ValueError(f"Unknown table: {table_name}")

    def read_table_metadata(self, table_name: str, table_options: Dict[str, str]) -> Dict:
        if table_name == self.TABLE_DATASERVERS:
            return {"primary_keys": ["webid"], "cursor_field": None, "ingestion_type": "snapshot"}
        if table_name == self.TABLE_POINTS:
            return {"primary_keys": ["webid"], "cursor_field": None, "ingestion_type": "snapshot"}
        if table_name == self.TABLE_POINT_ATTRIBUTES:
            return {"primary_keys": ["point_webid", "name"], "cursor_field": None, "ingestion_type": "snapshot"}
        if table_name in (self.TABLE_TIMESERIES, self.TABLE_STREAMSET_RECORDED, self.TABLE_INTERPOLATED, self.TABLE_STREAMSET_INTERPOLATED, self.TABLE_PLOT):
            return {"primary_keys": ["tag_webid", "timestamp"], "cursor_field": "timestamp", "ingestion_type": "append"}
        if table_name == self.TABLE_CURRENT_VALUE:
            return {"primary_keys": ["tag_webid"], "cursor_field": None, "ingestion_type": "snapshot"}
        if table_name == self.TABLE_SUMMARY:
            return {"primary_keys": ["tag_webid", "summary_type"], "cursor_field": None, "ingestion_type": "snapshot"}
        if table_name == self.TABLE_AF_HIERARCHY:
            return {"primary_keys": ["element_webid"], "cursor_field": None, "ingestion_type": "snapshot"}
        if table_name == self.TABLE_EVENT_FRAMES:
            return {"primary_keys": ["event_frame_webid", "start_time"], "cursor_field": "start_time", "ingestion_type": "append"}
        if table_name == self.TABLE_ELEMENT_ATTRIBUTES:
            return {"primary_keys": ["element_webid", "attribute_webid"], "cursor_field": None, "ingestion_type": "snapshot"}
        if table_name == self.TABLE_EVENTFRAME_ATTRIBUTES:
            return {"primary_keys": ["event_frame_webid", "attribute_webid"], "cursor_field": None, "ingestion_type": "snapshot"}
        if table_name == self.TABLE_STREAMSET_SUMMARY:
            return {"primary_keys": ["tag_webid", "summary_type", "timestamp"], "cursor_field": "timestamp", "ingestion_type": "append"}
        if table_name == self.TABLE_ASSET_SERVERS:
            return {"primary_keys": ["webid"], "cursor_field": None, "ingestion_type": "snapshot"}
        if table_name == self.TABLE_ASSET_DATABASES:
            return {"primary_keys": ["webid"], "cursor_field": None, "ingestion_type": "snapshot"}
        if table_name == self.TABLE_ELEMENT_TEMPLATES:
            return {"primary_keys": ["webid"], "cursor_field": None, "ingestion_type": "snapshot"}
        raise ValueError(f"Unknown table: {table_name}")

    def read_table(self, table_name: str, start_offset: dict, table_options: Dict[str, str]) -> Tuple[Iterator[dict], dict]:
        self._ensure_auth()

        if table_name == self.TABLE_DATASERVERS:
            return iter(self._read_dataservers()), {"offset": "done"}
        if table_name == self.TABLE_POINTS:
            return iter(self._read_points(table_options)), {"offset": "done"}
        if table_name == self.TABLE_POINT_ATTRIBUTES:
            return iter(self._read_point_attributes(table_options)), {"offset": "done"}
        if table_name == self.TABLE_TIMESERIES:
            return self._read_timeseries(start_offset, table_options)
        if table_name == self.TABLE_STREAMSET_RECORDED:
            return self._read_streamset_recorded(start_offset, table_options)
        if table_name == self.TABLE_INTERPOLATED:
            return self._read_interpolated(start_offset, table_options)
        if table_name == self.TABLE_STREAMSET_INTERPOLATED:
            return self._read_streamset_interpolated(start_offset, table_options)
        if table_name == self.TABLE_PLOT:
            return self._read_plot(start_offset, table_options)
        if table_name == self.TABLE_STREAMSET_SUMMARY:
            return self._read_streamset_summary(start_offset, table_options)
        if table_name == self.TABLE_CURRENT_VALUE:
            return iter(self._read_current_value(table_options)), {"offset": "done"}
        if table_name == self.TABLE_SUMMARY:
            return iter(self._read_summary(table_options)), {"offset": "done"}
        if table_name == self.TABLE_AF_HIERARCHY:
            return iter(self._read_af_hierarchy()), {"offset": "done"}
        if table_name == self.TABLE_EVENT_FRAMES:
            return self._read_event_frames(start_offset, table_options)
        if table_name == self.TABLE_ELEMENT_ATTRIBUTES:
            return iter(self._read_element_attributes(table_options)), {"offset": "done"}
        if table_name == self.TABLE_EVENTFRAME_ATTRIBUTES:
            return iter(self._read_eventframe_attributes(table_options)), {"offset": "done"}
        if table_name == self.TABLE_ASSET_SERVERS:
            return iter(self._read_assetservers_table()), {"offset": "done"}
        if table_name == self.TABLE_ASSET_DATABASES:
            return iter(self._read_assetdatabases_table()), {"offset": "done"}
        if table_name == self.TABLE_ELEMENT_TEMPLATES:
            return iter(self._read_element_templates_table(table_options)), {"offset": "done"}

        raise ValueError(f"Unknown table: {table_name}")

    def _ensure_auth(self) -> None:
        if self._auth_resolved:
            return

        access_token = self.options.get("access_token")
        if access_token:
            self.session.headers.update({"Authorization": f"Bearer {access_token}"})
            self._auth_resolved = True
            return

        workspace_host = (self.options.get("workspace_host") or "").rstrip("/")
        client_id = self.options.get("client_id")
        client_secret = self.options.get("client_secret")
        if workspace_host and client_id and client_secret:
            if not workspace_host.startswith("http://") and not workspace_host.startswith("https://"):
                workspace_host = "https://" + workspace_host

            token_url = f"{workspace_host}/oidc/v1/token"
            resp = requests.post(
                token_url,
                data={"grant_type": "client_credentials", "scope": "all-apis"},
                auth=(client_id, client_secret),
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=30,
            )
            resp.raise_for_status()
            token = resp.json().get("access_token")
            if not token:
                raise RuntimeError("OIDC token endpoint did not return access_token")
            self.session.headers.update({"Authorization": f"Bearer {token}"})
            self._auth_resolved = True
            return

        username = self.options.get("username")
        password = self.options.get("password")
        if username and password:
            self.session.auth = (username, password)
            self._auth_resolved = True
            return

        self._auth_resolved = True

    def _get_json(self, path: str, params: Optional[Any] = None) -> dict:
        url = f"{self.base_url}{path}"
        r = self.session.get(url, params=params, timeout=60, verify=self.verify_ssl)
        r.raise_for_status()
        return r.json()

    def _post_json(self, path: str, payload: Any) -> dict:
        url = f"{self.base_url}{path}"
        r = self.session.post(url, json=payload, timeout=120, verify=self.verify_ssl)
        r.raise_for_status()
        return r.json()

    def _batch_execute(self, requests_list: List[dict]) -> List[Tuple[str, dict]]:
        payload = _batch_request_dict(requests_list)
        resp_json = self._post_json("/piwebapi/batch", payload)
        return _batch_response_items(resp_json)

    def _read_dataservers(self) -> List[dict]:
        data = self._get_json("/piwebapi/dataservers")
        items = data.get("Items", []) or []
        return [{"webid": i.get("WebId"), "name": i.get("Name")} for i in items if i.get("WebId")]

    def _read_points(self, table_options: Dict[str, str]) -> List[dict]:
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

            data = self._get_json(f"/piwebapi/dataservers/{server_webid}/points", params=params)
            items = data.get("Items", []) or []

            for p in items:
                out.append({
                    "webid": p.get("WebId"),
                    "name": p.get("Name"),
                    "descriptor": p.get("Descriptor", ""),
                    "engineering_units": p.get("EngineeringUnits", ""),
                    "path": p.get("Path", ""),
                    "dataserver_webid": server_webid,
                })

            if len(items) < page_size:
                break
            start_index += page_size

        return [r for r in out if r.get("webid")]

    def _resolve_tag_webids(self, table_options: Dict[str, str]) -> List[str]:
        tag_webids_csv = table_options.get("tag_webids") or self.options.get("tag_webids") or ""
        tag_webids = [t.strip() for t in str(tag_webids_csv).split(",") if t.strip()]
        if tag_webids:
            return tag_webids
        pts = self._read_points(table_options)
        return [p["webid"] for p in pts[: int(table_options.get("default_tags", 50))]]

    def _read_point_attributes(self, table_options: Dict[str, str]) -> List[dict]:
        # Options:
        # - point_webids: csv of point WebIds
        # - default_points: sample size if point_webids not provided
        point_webids_csv = (table_options.get("point_webids") or self.options.get("point_webids") or "").strip()
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
        ingest_ts = _utcnow()

        for wid in point_webids:
            try:
                data = self._get_json(f"/piwebapi/points/{wid}/attributes", params=params or None)
                for item in (data.get("Items") or []):
                    out.append({
                        "point_webid": wid,
                        "name": item.get("Name"),
                        "value": None if item.get("Value") is None else str(item.get("Value")),
                        "type": item.get("Type") or item.get("ValueType") or "",
                        "ingestion_timestamp": ingest_ts,
                    })
            except Exception:
                continue

        return out


    def _read_timeseries(self, start_offset: dict, table_options: Dict[str, str]) -> Tuple[Iterator[dict], dict]:
        tag_webids = self._resolve_tag_webids(table_options)
        now = _utcnow()

        # Time range controls
        end_opt = table_options.get("endTime") or table_options.get("end_time") or "*"
        end_dt = _parse_pi_time(end_opt, now=now)

        start_dt: Optional[datetime] = None
        if start_offset and isinstance(start_offset, dict):
            off = start_offset.get("offset")
            if isinstance(off, str) and off:
                try:
                    start_dt = _parse_ts(off)
                except Exception:
                    start_dt = None

        if start_dt is None:
            start_opt = table_options.get("startTime") or table_options.get("start_time")
            if start_opt:
                start_dt = _parse_pi_time(str(start_opt), now=end_dt)
            else:
                lookback_minutes = int(table_options.get("lookback_minutes", 60))
                start_dt = end_dt - timedelta(minutes=lookback_minutes)

        window_seconds = int(table_options.get("window_seconds", 0) or 0)
        end_window = min(end_dt, start_dt + timedelta(seconds=window_seconds)) if window_seconds > 0 else end_dt

        start_str = _isoformat_z(start_dt)
        end_str = _isoformat_z(end_window)
        max_count = int(table_options.get("maxCount", 1000))
        ingest_ts = _utcnow()

        tags_per_request = int(table_options.get("tags_per_request", 0) or 0)
        groups = _chunks(tag_webids, tags_per_request) if tags_per_request else [tag_webids]

        prefer_streamset = _as_bool(table_options.get("prefer_streamset", True), default=True)
        selected_fields = table_options.get("selectedFields")

        next_offset = {"offset": end_str}

        # Preferred: StreamSet GetRecordedAdHoc for multi-tag reads.
        if prefer_streamset and len(tag_webids) > 1:
            def iterator() -> Iterator[dict]:
                for group in groups:
                    if not group:
                        continue
                    params: List[Tuple[str, str]] = [("webId", w) for w in group]
                    params += [("startTime", start_str), ("endTime", end_str), ("maxCount", str(max_count))]
                    if selected_fields:
                        params.append(("selectedFields", str(selected_fields)))
                    data = self._get_json("/piwebapi/streamsets/recorded", params=params)
                    for stream in data.get("Items", []) or []:
                        webid = stream.get("WebId")
                        if not webid:
                            continue
                        for item in stream.get("Items", []) or []:
                            ts = item.get("Timestamp")
                            if not ts:
                                continue
                            yield {
                                "tag_webid": webid,
                                "timestamp": _parse_ts(ts),
                                "value": _try_float(item.get("Value")),
                                "good": _as_bool(item.get("Good"), default=True),
                                "questionable": _as_bool(item.get("Questionable"), default=False),
                                "substituted": _as_bool(item.get("Substituted"), default=False),
                                "annotated": _as_bool(item.get("Annotated"), default=False),
                                "units": item.get("UnitsAbbreviation", ""),
                                "ingestion_timestamp": ingest_ts,
                            }

            return iterator(), next_offset

        # Fallback: Batch execute many Stream GetRecorded calls (chunked).
        def iterator() -> Iterator[dict]:
            for group in groups:
                if not group:
                    continue
                reqs = [
                    {
                        "Method": "GET",
                        "Resource": f"/piwebapi/streams/{webid}/recorded",
                        "Parameters": {"startTime": start_str, "endTime": end_str, "maxCount": str(max_count)},
                    }
                    for webid in group
                ]
                responses = self._batch_execute(reqs)
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
                            "timestamp": _parse_ts(ts),
                            "value": _try_float(item.get("Value")),
                            "good": _as_bool(item.get("Good"), default=True),
                            "questionable": _as_bool(item.get("Questionable"), default=False),
                            "substituted": _as_bool(item.get("Substituted"), default=False),
                            "annotated": _as_bool(item.get("Annotated"), default=False),
                            "units": item.get("UnitsAbbreviation", ""),
                            "ingestion_timestamp": ingest_ts,
                        }

        return iterator(), next_offset

    def _read_streamset_recorded(self, start_offset: dict, table_options: Dict[str, str]) -> Tuple[Iterator[dict], dict]:
        # Explicit StreamSet recorded table (same output schema as pi_timeseries)
        tag_webids = self._resolve_tag_webids(table_options)
        now = _utcnow()

        end_opt = table_options.get("endTime") or table_options.get("end_time") or "*"
        end_dt = _parse_pi_time(end_opt, now=now)

        start_dt: Optional[datetime] = None
        if start_offset and isinstance(start_offset, dict):
            off = start_offset.get("offset")
            if isinstance(off, str) and off:
                try:
                    start_dt = _parse_ts(off)
                except Exception:
                    start_dt = None

        if start_dt is None:
            start_opt = table_options.get("startTime") or table_options.get("start_time")
            if start_opt:
                start_dt = _parse_pi_time(str(start_opt), now=end_dt)
            else:
                lookback_minutes = int(table_options.get("lookback_minutes", 60))
                start_dt = end_dt - timedelta(minutes=lookback_minutes)

        window_seconds = int(table_options.get("window_seconds", 0) or 0)
        end_window = min(end_dt, start_dt + timedelta(seconds=window_seconds)) if window_seconds > 0 else end_dt

        start_str = _isoformat_z(start_dt)
        end_str = _isoformat_z(end_window)
        max_count = int(table_options.get("maxCount", 1000))
        ingest_ts = _utcnow()

        tags_per_request = int(table_options.get("tags_per_request", 0) or 0)
        groups = _chunks(tag_webids, tags_per_request) if tags_per_request else [tag_webids]
        selected_fields = table_options.get("selectedFields")

        def iterator() -> Iterator[dict]:
            for group in groups:
                if not group:
                    continue
                params: List[Tuple[str, str]] = [("webId", w) for w in group]
                params += [("startTime", start_str), ("endTime", end_str), ("maxCount", str(max_count))]
                if selected_fields:
                    params.append(("selectedFields", str(selected_fields)))
                data = self._get_json("/piwebapi/streamsets/recorded", params=params)
                for stream in data.get("Items", []) or []:
                    webid = stream.get("WebId")
                    if not webid:
                        continue
                    for item in stream.get("Items", []) or []:
                        ts = item.get("Timestamp")
                        if not ts:
                            continue
                        yield {
                            "tag_webid": webid,
                            "timestamp": _parse_ts(ts),
                            "value": _try_float(item.get("Value")),
                            "good": _as_bool(item.get("Good"), default=True),
                            "questionable": _as_bool(item.get("Questionable"), default=False),
                            "substituted": _as_bool(item.get("Substituted"), default=False),
                            "annotated": _as_bool(item.get("Annotated"), default=False),
                            "units": item.get("UnitsAbbreviation", ""),
                            "ingestion_timestamp": ingest_ts,
                        }

        return iterator(), {"offset": end_str}



    def _read_interpolated(self, start_offset: dict, table_options: Dict[str, str]) -> Tuple[Iterator[dict], dict]:
        """Interpolated values for tags over a window (best-effort).

        If interpolated endpoints are not available on the PI Web API host, this returns an empty iterator.
        """
        tag_webids = self._resolve_tag_webids(table_options)
        now = _utcnow()

        end_opt = table_options.get("endTime") or table_options.get("end_time") or "*"
        end_dt = _parse_pi_time(end_opt, now=now)

        start_dt: Optional[datetime] = None
        if start_offset and isinstance(start_offset, dict):
            off = start_offset.get("offset")
            if isinstance(off, str) and off:
                try:
                    start_dt = _parse_ts(off)
                except Exception:
                    start_dt = None

        if start_dt is None:
            start_opt = table_options.get("startTime") or table_options.get("start_time")
            if start_opt:
                start_dt = _parse_pi_time(str(start_opt), now=end_dt)
            else:
                lookback_minutes = int(table_options.get("lookback_minutes", 60))
                start_dt = end_dt - timedelta(minutes=lookback_minutes)

        start_str = _isoformat_z(start_dt)
        end_str = _isoformat_z(end_dt)

        interval = (table_options.get("interval") or table_options.get("sampleInterval") or "1m").strip()
        max_count = int(table_options.get("maxCount", 1000))
        ingest_ts = _utcnow()

        tags_per_request = int(table_options.get("tags_per_request", 0) or 0)
        groups = _chunks(tag_webids, tags_per_request) if tags_per_request else [tag_webids]
        selected_fields = table_options.get("selectedFields")

        next_offset = {"offset": end_str}

        def emit_items(wid: str, items: List[dict]) -> Iterator[dict]:
            for item in items or []:
                ts = item.get("Timestamp")
                if not ts:
                    continue
                yield {
                    "tag_webid": wid,
                    "timestamp": _parse_ts(ts),
                    "value": _try_float(item.get("Value")),
                    "good": _as_bool(item.get("Good"), default=True),
                    "questionable": _as_bool(item.get("Questionable"), default=False),
                    "substituted": _as_bool(item.get("Substituted"), default=False),
                    "annotated": _as_bool(item.get("Annotated"), default=False),
                    "units": item.get("UnitsAbbreviation", ""),
                    "ingestion_timestamp": ingest_ts,
                }

        # Try StreamSet interpolated first for multi-tag reads.
        def iterator() -> Iterator[dict]:
            for group in groups:
                if not group:
                    continue
                # Prefer streamsets/interpolated when multiple tags
                if len(group) > 1:
                    params: List[Tuple[str, str]] = [("webId", w) for w in group]
                    params += [("startTime", start_str), ("endTime", end_str), ("interval", interval), ("maxCount", str(max_count))]
                    if selected_fields:
                        params.append(("selectedFields", str(selected_fields)))
                    try:
                        data = self._get_json("/piwebapi/streamsets/interpolated", params=params)
                    except requests.exceptions.HTTPError as e:
                        if getattr(e.response, 'status_code', None) == 404:
                            data = None
                        else:
                            raise
                    if data:
                        for stream in data.get("Items", []) or []:
                            wid = stream.get("WebId")
                            if not wid:
                                continue
                            yield from emit_items(wid, stream.get("Items", []) or [])
                        continue

                # Fallback: per-tag interpolated via batch
                reqs = [
                    {
                        "Method": "GET",
                        "Resource": f"/piwebapi/streams/{wid}/interpolated",
                        "Parameters": {"startTime": start_str, "endTime": end_str, "interval": interval, "maxCount": str(max_count)},
                    }
                    for wid in group
                ]
                try:
                    responses = self._batch_execute(reqs)
                except requests.exceptions.HTTPError as e:
                    if getattr(e.response, 'status_code', None) == 404:
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


    def _read_streamset_interpolated(self, start_offset: dict, table_options: Dict[str, str]) -> Tuple[Iterator[dict], dict]:
        """Explicit StreamSet interpolated table (same output schema as pi_interpolated)."""
        # Force streamset path by grouping all tags together.
        opts = dict(table_options)
        opts["tags_per_request"] = str(max(1, int(opts.get("tags_per_request", 0) or 0)))
        return self._read_interpolated(start_offset, opts)


    def _read_plot(self, start_offset: dict, table_options: Dict[str, str]) -> Tuple[Iterator[dict], dict]:
        """Plot values for tags over a window (best-effort).

        If plot endpoints are not available on the PI Web API host, this returns an empty iterator.
        """
        tag_webids = self._resolve_tag_webids(table_options)
        now = _utcnow()

        end_opt = table_options.get("endTime") or table_options.get("end_time") or "*"
        end_dt = _parse_pi_time(end_opt, now=now)

        start_dt: Optional[datetime] = None
        if start_offset and isinstance(start_offset, dict):
            off = start_offset.get("offset")
            if isinstance(off, str) and off:
                try:
                    start_dt = _parse_ts(off)
                except Exception:
                    start_dt = None

        if start_dt is None:
            start_opt = table_options.get("startTime") or table_options.get("start_time")
            if start_opt:
                start_dt = _parse_pi_time(str(start_opt), now=end_dt)
            else:
                lookback_minutes = int(table_options.get("lookback_minutes", 60))
                start_dt = end_dt - timedelta(minutes=lookback_minutes)

        start_str = _isoformat_z(start_dt)
        end_str = _isoformat_z(end_dt)
        intervals = int(table_options.get("intervals", 300) or 300)
        ingest_ts = _utcnow()

        def iterator() -> Iterator[dict]:
            for wid in tag_webids:
                try:
                    data = self._get_json(
                        f"/piwebapi/streams/{wid}/plot",
                        params={"startTime": start_str, "endTime": end_str, "intervals": str(intervals)},
                    )
                except requests.exceptions.HTTPError as e:
                    if getattr(e.response, 'status_code', None) == 404:
                        return
                    raise

                for item in (data.get("Items") or []):
                    ts = item.get("Timestamp")
                    if not ts:
                        continue
                    yield {
                        "tag_webid": wid,
                        "timestamp": _parse_ts(ts),
                        "value": _try_float(item.get("Value")),
                        "good": _as_bool(item.get("Good"), default=True),
                        "questionable": _as_bool(item.get("Questionable"), default=False),
                        "substituted": _as_bool(item.get("Substituted"), default=False),
                        "annotated": _as_bool(item.get("Annotated"), default=False),
                        "units": item.get("UnitsAbbreviation", ""),
                        "ingestion_timestamp": ingest_ts,
                    }

        return iterator(), {"offset": end_str}


    def _read_streamset_summary(self, start_offset: dict, table_options: Dict[str, str]) -> Tuple[Iterator[dict], dict]:
        """Multi-tag summary (best-effort).

        If streamset summary endpoints are not available on the PI Web API host, this returns an empty iterator.
        """
        tag_webids = self._resolve_tag_webids(table_options)
        now = _utcnow()

        end_opt = table_options.get("endTime") or table_options.get("end_time") or "*"
        end_dt = _parse_pi_time(end_opt, now=now)

        start_dt: Optional[datetime] = None
        if start_offset and isinstance(start_offset, dict):
            off = start_offset.get("offset")
            if isinstance(off, str) and off:
                try:
                    start_dt = _parse_ts(off)
                except Exception:
                    start_dt = None

        if start_dt is None:
            start_opt = table_options.get("startTime") or table_options.get("start_time")
            if start_opt:
                start_dt = _parse_pi_time(str(start_opt), now=end_dt)
            else:
                lookback_minutes = int(table_options.get("lookback_minutes", 60))
                start_dt = end_dt - timedelta(minutes=lookback_minutes)

        start_str = _isoformat_z(start_dt)
        end_str = _isoformat_z(end_dt)

        summary_type = (table_options.get("summaryType") or "Total").strip()
        calculation_basis = (table_options.get("calculationBasis") or "TimeWeighted").strip()
        summary_duration = (table_options.get("summaryDuration") or "1h").strip()
        selected_fields = table_options.get("selectedFields")

        tags_per_request = int(table_options.get("tags_per_request", 0) or 0)
        groups = _chunks(tag_webids, tags_per_request) if tags_per_request else [tag_webids]

        ingest_ts = _utcnow()

        def iterator() -> Iterator[dict]:
            for group in groups:
                if not group:
                    continue
                params: List[Tuple[str, str]] = [("webId", w) for w in group]
                params += [
                    ("startTime", start_str),
                    ("endTime", end_str),
                    ("summaryType", summary_type),
                    ("calculationBasis", calculation_basis),
                    ("summaryDuration", summary_duration),
                ]
                if selected_fields:
                    params.append(("selectedFields", str(selected_fields)))

                try:
                    data = self._get_json("/piwebapi/streamsets/summary", params=params)
                except requests.exceptions.HTTPError as e:
                    if getattr(e.response, 'status_code', None) == 404:
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
                            "timestamp": _parse_ts(ts),
                            "value": _try_float(v.get("Value")),
                            "good": _as_bool(v.get("Good"), default=True),
                            "questionable": _as_bool(v.get("Questionable"), default=False),
                            "substituted": _as_bool(v.get("Substituted"), default=False),
                            "annotated": _as_bool(v.get("Annotated"), default=False),
                            "units": v.get("UnitsAbbreviation", ""),
                            "ingestion_timestamp": ingest_ts,
                        }

        return iterator(), {"offset": end_str}


    def _read_assetservers_table(self) -> List[dict]:
        try:
            items = self._read_assetservers()
        except requests.exceptions.HTTPError as e:
            if getattr(e.response, 'status_code', None) == 404:
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
        try:
            assetservers = self._read_assetservers()
        except requests.exceptions.HTTPError as e:
            if getattr(e.response, 'status_code', None) == 404:
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
                if getattr(e.response, 'status_code', None) == 404:
                    continue
                raise
            for db in dbs or []:
                db_wid = db.get("WebId")
                if not db_wid:
                    continue
                out.append({
                    "webid": db_wid,
                    "name": db.get("Name", ""),
                    "path": db.get("Path", ""),
                    "assetserver_webid": srv_wid,
                })
        return out


    def _read_element_templates_table(self, table_options: Dict[str, str]) -> List[dict]:
        out: List[dict] = []
        db_wid_opt = (table_options.get("assetdatabase_webid") or "").strip()
        if db_wid_opt:
            db_wids = [db_wid_opt]
        else:
            db_wids = [d.get("webid") for d in self._read_assetdatabases_table() if d.get("webid")]

        for db_wid in db_wids:
            try:
                data = self._get_json(f"/piwebapi/assetdatabases/{db_wid}/elementtemplates")
            except requests.exceptions.HTTPError as e:
                if getattr(e.response, 'status_code', None) == 404:
                    continue
                raise
            for it in (data.get("Items") or []):
                wid = it.get("WebId")
                if not wid:
                    continue
                out.append({
                    "webid": wid,
                    "name": it.get("Name", ""),
                    "description": it.get("Description", ""),
                    "path": it.get("Path", ""),
                    "assetdatabase_webid": db_wid,
                })
        return out
    def _read_current_value(self, table_options: Dict[str, str]) -> List[dict]:
        tag_webids = self._resolve_tag_webids(table_options)
        tags_per_request = int(table_options.get("tags_per_request", 0) or 0)
        tag_webid_groups = _chunks(tag_webids, tags_per_request) if tags_per_request else [tag_webids]
        time_param = table_options.get("time")

        reqs: List[dict] = []
        for w in tag_webids:
            params: Dict[str, str] = {}
            if time_param:
                params["time"] = str(time_param)
            reqs.append({"Method": "GET", "Resource": f"/piwebapi/streams/{w}/value", "Parameters": params})

        ingest_ts = _utcnow()
        out: List[dict] = []

        for group in tag_webid_groups:
            if not group:
                continue
            group_reqs: List[dict] = []
            for w in group:
                params: Dict[str, str] = {}
                if time_param:
                    params["time"] = str(time_param)
                group_reqs.append({"Method": "GET", "Resource": f"/piwebapi/streams/{w}/value", "Parameters": params})
            responses = self._batch_execute(group_reqs)
            for idx, (_rid, resp) in enumerate(responses):
                if resp.get("Status") != 200:
                    continue
                webid = group[idx] if idx < len(group) else None
                if not webid:
                    continue
                v = resp.get("Content", {}) or {}
                ts = v.get("Timestamp")
                out.append({
                    "tag_webid": webid,
                    "timestamp": _parse_ts(ts) if ts else None,
                    "value": _try_float(v.get("Value")),
                    "good": _as_bool(v.get("Good"), default=True),
                    "questionable": _as_bool(v.get("Questionable"), default=False),
                    "substituted": _as_bool(v.get("Substituted"), default=False),
                    "annotated": _as_bool(v.get("Annotated"), default=False),
                    "units": v.get("UnitsAbbreviation", ""),
                    "ingestion_timestamp": ingest_ts,
                })

        return out

    def _read_summary(self, table_options: Dict[str, str]) -> List[dict]:
        # NOTE: The PI Web API supports multiple instances of summaryType.
        # We implement the API-correct approach (repeat summaryType in query params) and keep this table snapshot-like.
        tag_webids = self._resolve_tag_webids(table_options)
        tags_per_request = int(table_options.get("tags_per_request", 0) or 0)
        tag_webid_groups = _chunks(tag_webids, tags_per_request) if tags_per_request else [tag_webids]
        start_time = table_options.get("startTime")
        end_time = table_options.get("endTime")
        summary_types_csv = table_options.get("summaryType", "Total")
        summary_types = [s.strip() for s in str(summary_types_csv).split(",") if s.strip()]
        if not summary_types:
            summary_types = ["Total"]

        passthrough_keys = ("calculationBasis", "timeType", "summaryDuration", "sampleType", "sampleInterval", "timeZone", "filterExpression")
        passthrough = {k: str(table_options.get(k)) for k in passthrough_keys if table_options.get(k) is not None}

        ingest_ts = _utcnow()
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

            data = self._get_json(f"/piwebapi/streams/{w}/summary", params=params)
            for item in data.get("Items", []) or []:
                stype = item.get("Type") or ""
                v = item.get("Value", {}) or {}
                ts = v.get("Timestamp")
                out.append({
                    "tag_webid": w,
                    "summary_type": str(stype),
                    "timestamp": _parse_ts(ts) if ts else None,
                    "value": _try_float(v.get("Value")),
                    "good": _as_bool(v.get("Good"), default=True),
                    "questionable": _as_bool(v.get("Questionable"), default=False),
                    "substituted": _as_bool(v.get("Substituted"), default=False),
                    "annotated": _as_bool(v.get("Annotated"), default=False),
                    "units": v.get("UnitsAbbreviation", ""),
                    "ingestion_timestamp": ingest_ts,
                })

        return out

    def _read_assetservers(self) -> List[dict]:
        data = self._get_json("/piwebapi/assetservers")
        return data.get("Items", []) or []

    def _read_assetdatabases(self, assetserver_webid: str) -> List[dict]:
        data = self._get_json(f"/piwebapi/assetservers/{assetserver_webid}/assetdatabases")
        return data.get("Items", []) or []

    def _read_af_hierarchy(self) -> List[dict]:
        assetservers = self._read_assetservers()
        if not assetservers:
            return []

        out: List[dict] = []
        ingest_ts = _utcnow()

        def walk(elements: List[dict], parent_webid: str, depth: int):
            for e in elements:
                webid = e.get("WebId")
                if not webid:
                    continue
                out.append({
                    "element_webid": webid,
                    "name": e.get("Name", ""),
                    "template_name": e.get("TemplateName", ""),
                    "description": e.get("Description", ""),
                    "path": e.get("Path", ""),
                    "parent_webid": parent_webid or "",
                    "depth": depth,
                    "category_names": e.get("CategoryNames") or [],
                    "ingestion_timestamp": ingest_ts,
                })
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
                roots = self._get_json(
                    f"/piwebapi/assetdatabases/{db_webid}/elements",
                    params={"searchFullHierarchy": "true"},
                ).get("Items", []) or []
                walk(roots, parent_webid="", depth=0)

        return out

    def _read_event_frames(self, start_offset: dict, table_options: Dict[str, str]) -> Tuple[Iterator[dict], dict]:
        now = _utcnow()

        start = None
        if start_offset and isinstance(start_offset, dict):
            off = start_offset.get("offset")
            if isinstance(off, str) and off:
                try:
                    start = _parse_ts(off)
                except Exception:
                    start = None
        if start is None:
            lookback_days = int(table_options.get("lookback_days", 30))
            start = now - timedelta(days=lookback_days)

        start_str = _isoformat_z(start)
        end_str = _isoformat_z(now)

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
                    resp = self._get_json(f"/piwebapi/assetdatabases/{db_webid}/eventframes", params=params)
                    items = resp.get("Items", []) or []
                    all_events.extend(items)
                    if len(items) < page_size:
                        break
                    start_index += page_size

        ingest_ts = _utcnow()

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
                    "start_time": _parse_ts(ef.get("StartTime")) if ef.get("StartTime") else None,
                    "end_time": _parse_ts(ef.get("EndTime")) if ef.get("EndTime") else None,
                    "primary_referenced_element_webid": ef.get("PrimaryReferencedElementWebId"),
                    "description": ef.get("Description", ""),
                    "category_names": ef.get("CategoryNames") or [],
                    "attributes": attrs,
                    "ingestion_timestamp": ingest_ts,
                }

        return iterator(), {"offset": end_str}

    def _read_element_attributes(self, table_options: Dict[str, str]) -> List[dict]:
        element_csv = table_options.get("element_webids", "")
        element_webids = [e.strip() for e in str(element_csv).split(",") if e.strip()]
        if not element_webids:
            af = self._read_af_hierarchy()
            element_webids = [r.get("element_webid") for r in af[: int(table_options.get("default_elements", 10))] if r.get("element_webid")]

        name_filter = table_options.get("nameFilter")
        page_size = int(table_options.get("maxCount", 1000))
        start_index = int(table_options.get("startIndex", 0))

        ingest_ts = _utcnow()
        out: List[dict] = []
        for ew in element_webids:
            params: Dict[str, str] = {"maxCount": str(page_size), "startIndex": str(start_index)}
            if name_filter:
                params["nameFilter"] = str(name_filter)
            data = self._get_json(f"/piwebapi/elements/{ew}/attributes", params=params)
            for a in data.get("Items", []) or []:
                aw = a.get("WebId")
                if not aw:
                    continue
                out.append({
                    "element_webid": ew,
                    "attribute_webid": aw,
                    "name": a.get("Name", ""),
                    "description": a.get("Description", ""),
                    "path": a.get("Path", ""),
                    "type": a.get("Type", ""),
                    "default_units_name": a.get("DefaultUnitsName", ""),
                    "data_reference_plugin": a.get("DataReferencePlugIn", ""),
                    "is_configuration_item": _as_bool(a.get("IsConfigurationItem"), default=False),
                    "ingestion_timestamp": ingest_ts,
                })
        return out

    def _read_eventframe_attributes(self, table_options: Dict[str, str]) -> List[dict]:
        ef_csv = table_options.get("event_frame_webids", "")
        ef_webids = [e.strip() for e in str(ef_csv).split(",") if e.strip()]
        if not ef_webids:
            # sample event frames
            records, _ = self._read_event_frames({}, {"lookback_days": table_options.get("lookback_days", 30)})
            tmp = []
            for i, r in enumerate(records):
                tmp.append(r)
                if i >= int(table_options.get("default_event_frames", 10)) - 1:
                    break
            ef_webids = [r.get("event_frame_webid") for r in tmp if r.get("event_frame_webid")]

        name_filter = table_options.get("nameFilter")
        page_size = int(table_options.get("maxCount", 1000))
        start_index = int(table_options.get("startIndex", 0))

        ingest_ts = _utcnow()
        out: List[dict] = []
        for efw in ef_webids:
            params: Dict[str, str] = {"maxCount": str(page_size), "startIndex": str(start_index)}
            if name_filter:
                params["nameFilter"] = str(name_filter)
            data = self._get_json(f"/piwebapi/eventframes/{efw}/attributes", params=params)
            for a in data.get("Items", []) or []:
                aw = a.get("WebId")
                if not aw:
                    continue
                out.append({
                    "event_frame_webid": efw,
                    "attribute_webid": aw,
                    "name": a.get("Name", ""),
                    "description": a.get("Description", ""),
                    "path": a.get("Path", ""),
                    "type": a.get("Type", ""),
                    "default_units_name": a.get("DefaultUnitsName", ""),
                    "data_reference_plugin": a.get("DataReferencePlugIn", ""),
                    "is_configuration_item": _as_bool(a.get("IsConfigurationItem"), default=False),
                    "ingestion_timestamp": ingest_ts,
                })
        return out
