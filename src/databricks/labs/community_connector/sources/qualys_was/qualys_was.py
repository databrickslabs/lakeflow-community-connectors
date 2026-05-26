"""qualys_was connector."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Iterator
from xml.etree import ElementTree

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.qualys_common import (
    QualysClient,
    as_iso8601,
    coalesce,
    get_text,
    parse_xml,
)
from databricks.labs.community_connector.sources.qualys_was.qualys_was_schemas import (
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
)


class QualysWasLakeflowConnect(LakeflowConnect):
    """Lakeflow connector for Qualys WAS APIs."""

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        username = options.get("username")
        password = options.get("password")
        if not username or not password:
            raise ValueError("qualys_was requires username and password")

        self.client = QualysClient(
            base_url=options.get("base_url", "https://qualysapi.qualys.com"),
            auth_mode="basic",
            username=username,
            password=password,
            timeout_seconds=int(options.get("timeout_seconds", "30")),
            extra_headers={"content-type": "text/xml", "X-Requested-With": "Lakeflow-Qualys-WAS"},
        )
        self._init_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def list_tables(self) -> list[str]:
        return SUPPORTED_TABLES.copy()

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        del table_options
        if table_name not in TABLE_SCHEMAS:
            raise ValueError(f"Unsupported table: {table_name!r}")
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        del table_options
        if table_name not in TABLE_METADATA:
            raise ValueError(f"Unsupported table: {table_name!r}")
        return TABLE_METADATA[table_name]

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        del table_options
        start_offset = start_offset or {}
        if table_name == "web_apps":
            return self._read_web_apps(start_offset)
        if table_name == "web_findings":
            return self._read_web_findings(start_offset)
        if table_name == "was_scans":
            return self._read_was_scans(start_offset)
        if table_name == "was_scan_results":
            return self._read_was_scan_results(start_offset)
        raise ValueError(f"Unsupported table: {table_name!r}")

    def _read_web_apps(self, start_offset: dict) -> tuple[Iterator[dict], dict]:
        xml_text = self.client.request_text("POST", "/qps/rest/3.0/search/was/webapp")
        root = parse_xml(xml_text)
        rows = [self._map_web_app_node(node) for node in root.findall(".//WebApp")]
        return self._finalize_cdc(rows, start_offset, cursor_field="updated_date")

    def _read_web_findings(self, start_offset: dict) -> tuple[Iterator[dict], dict]:
        xml_text = self.client.request_text("POST", "/qps/rest/3.0/search/was/finding")
        root = parse_xml(xml_text)
        rows = [self._map_web_finding_node(node) for node in root.findall(".//Finding")]
        return self._finalize_cdc(rows, start_offset, cursor_field="last_found")

    def _read_was_scans(self, start_offset: dict) -> tuple[Iterator[dict], dict]:
        xml_text = self.client.request_text("POST", "/qps/rest/3.0/search/was/wasscan")
        root = parse_xml(xml_text)
        rows = [self._map_was_scan_node(node) for node in root.findall(".//WasScan")]
        return self._finalize_cdc(rows, start_offset, cursor_field="launched_date")

    def _read_was_scan_results(self, start_offset: dict) -> tuple[Iterator[dict], dict]:
        xml_text = self.client.request_text("POST", "/qps/rest/3.0/search/was/wasscan")
        root = parse_xml(xml_text)
        scans = [self._map_was_scan_node(node) for node in root.findall(".//WasScan")]

        rows: list[dict[str, Any]] = []
        for scan in scans:
            scan_id = scan.get("scan_id")
            if scan_id is None:
                continue
            detail_xml = self.client.request_text("GET", f"/qps/rest/3.0/get/was/wasscan/{scan_id}")
            detail_root = parse_xml(detail_xml)
            detail_node = detail_root.find(".//WasScan")
            if detail_node is not None:
                rows.append(self._map_was_scan_result_node(detail_node, fallback=scan))

        return self._finalize_cdc(rows, start_offset, cursor_field="result_date")

    def _map_web_app_node(self, node: ElementTree.Element) -> dict[str, Any]:
        payload = {
            "webapp_id": self._to_int(get_text(node, "id")) or -1,
            "name": get_text(node, "name"),
            "url": get_text(node, "url"),
            "owner_id": self._to_int(get_text(node, "owner/id")),
            "created_date": as_iso8601(get_text(node, "createdDate")),
            "updated_date": as_iso8601(get_text(node, "updatedDate")),
            "last_scan_date": as_iso8601(get_text(node, "lastScan/date")),
            "last_scan_status": get_text(node, "lastScan/status"),
        }
        payload["raw_payload"] = json.dumps(payload, separators=(",", ":"), default=str)
        return payload

    def _map_web_finding_node(self, node: ElementTree.Element) -> dict[str, Any]:
        payload = {
            "finding_id": self._to_int(get_text(node, "id")) or -1,
            "webapp_id": self._to_int(get_text(node, "webApp/id")),
            "qid": self._to_int(get_text(node, "qid")),
            "severity": get_text(node, "severity"),
            "status": get_text(node, "status"),
            "type": get_text(node, "type"),
            "first_found": as_iso8601(get_text(node, "firstFoundDate")),
            "last_found": as_iso8601(get_text(node, "lastFoundDate")),
            "qds": get_text(node, "qds"),
        }
        payload["raw_payload"] = json.dumps(payload, separators=(",", ":"), default=str)
        return payload

    def _map_was_scan_node(self, node: ElementTree.Element) -> dict[str, Any]:
        payload = {
            "scan_id": self._to_int(get_text(node, "id")) or -1,
            "name": get_text(node, "name"),
            "reference": get_text(node, "reference"),
            "webapp_id": self._to_int(get_text(node, "webApp/id")),
            "scan_type": get_text(node, "type"),
            "scan_mode": get_text(node, "mode"),
            "status": get_text(node, "status"),
            "launched_date": as_iso8601(coalesce(get_text(node, "launchedDate"), get_text(node, "submittedDate"))),
        }
        payload["raw_payload"] = json.dumps(payload, separators=(",", ":"), default=str)
        return payload

    def _map_was_scan_result_node(
        self,
        node: ElementTree.Element,
        *,
        fallback: dict[str, Any],
    ) -> dict[str, Any]:
        scan_id = self._to_int(get_text(node, "id")) or fallback["scan_id"]
        payload = {
            "scan_id": scan_id,
            "reference": coalesce(get_text(node, "reference"), fallback.get("reference")),
            "status": coalesce(get_text(node, "status"), fallback.get("status")),
            "result_date": as_iso8601(
                coalesce(get_text(node, "updatedDate"), get_text(node, "launchedDate"), fallback.get("launched_date"))
            ),
            "finding_count": self._to_int(coalesce(get_text(node, "findingCount"), get_text(node, "summary/findingCount"))),
        }
        payload["raw_payload"] = json.dumps(payload, separators=(",", ":"), default=str)
        return payload

    def _finalize_cdc(
        self,
        records: list[dict[str, Any]],
        start_offset: dict,
        *,
        cursor_field: str,
    ) -> tuple[Iterator[dict], dict]:
        if not records:
            return iter([]), start_offset or {}

        start_cursor = start_offset.get("cursor") if start_offset else None
        max_cursor = start_cursor
        for record in records:
            value = record.get(cursor_field)
            if value and (max_cursor is None or str(value) > str(max_cursor)):
                max_cursor = value

        if max_cursor is None:
            max_cursor = self._init_time

        if start_cursor is not None and str(max_cursor) <= str(start_cursor):
            return iter([]), start_offset

        return iter(records), {"cursor": max_cursor}

    @staticmethod
    def _to_int(value: Any) -> int | None:
        try:
            if value is None or value == "":
                return None
            return int(value)
        except (TypeError, ValueError):
            return None
