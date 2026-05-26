"""qualys_vmdr connector."""

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
from databricks.labs.community_connector.sources.qualys_vmdr.qualys_vmdr_schemas import (
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
)


class QualysVmdrLakeflowConnect(LakeflowConnect):
    """Lakeflow connector for Qualys VMDR APIs."""

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        username = options.get("username")
        password = options.get("password")
        if not username or not password:
            raise ValueError("qualys_vmdr requires username and password")

        self.client = QualysClient(
            base_url=options.get("base_url", "https://qualysapi.qualys.com"),
            auth_mode="basic",
            username=username,
            password=password,
            timeout_seconds=int(options.get("timeout_seconds", "30")),
            extra_headers={"X-Requested-With": "Lakeflow-Qualys-VMDR"},
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
        start_offset = start_offset or {}
        if table_name in {"assets", "hosts"}:
            return self._read_hosts(start_offset, table_name)
        if table_name in {"detections", "vulnerabilities", "findings"}:
            return self._read_detections(start_offset, table_name)
        if table_name in {"knowledgebase", "qids"}:
            return self._read_knowledgebase(start_offset, table_name)
        if table_name == "scans":
            return self._read_scans(start_offset)
        if table_name == "scan_results":
            return self._read_scan_results(start_offset)
        if table_name == "asset_groups":
            return self._read_asset_groups()
        raise ValueError(f"Unsupported table: {table_name!r}")

    def _read_hosts(self, start_offset: dict, table_name: str) -> tuple[Iterator[dict], dict]:
        xml_text = self.client.request_text(
            "GET",
            "/api/2.0/fo/asset/host/",
            params={"action": "list", "details": "All", "truncation_limit": "1000"},
        )
        root = parse_xml(xml_text)
        records = [self._map_host_node(host, table_name) for host in root.findall(".//HOST")]
        return self._finalize_cdc(records, start_offset, cursor_field="last_vuln_scan")

    def _read_detections(
        self, start_offset: dict, table_name: str
    ) -> tuple[Iterator[dict], dict]:
        xml_text = self.client.request_text(
            "GET",
            "/api/2.0/fo/asset/host/vm/detection/",
            params={"action": "list", "truncation_limit": "1000"},
        )
        root = parse_xml(xml_text)

        records: list[dict[str, Any]] = []
        for host in root.findall(".//HOST"):
            host_id = self._to_int(get_text(host, "ID"))
            host_ip = get_text(host, "IP")
            for detection in host.findall("./DETECTION_LIST/DETECTION"):
                records.append(self._map_detection_node(detection, host_id=host_id, host_ip=host_ip))

        return self._finalize_cdc(records, start_offset, cursor_field="last_found")

    def _read_knowledgebase(
        self, start_offset: dict, table_name: str
    ) -> tuple[Iterator[dict], dict]:
        del table_name
        xml_text = self.client.request_text(
            "POST",
            "/api/2.0/fo/knowledge_base/vuln/",
            data={"action": "list", "details": "Basic"},
        )
        root = parse_xml(xml_text)
        records = [self._map_knowledgebase_node(v) for v in root.findall(".//VULN")]
        return self._finalize_cdc(records, start_offset, cursor_field="last_service_modification_date")

    def _read_scans(self, start_offset: dict) -> tuple[Iterator[dict], dict]:
        xml_text = self.client.request_text(
            "GET",
            "/api/2.0/fo/scan/",
            params={"action": "list", "echo_request": "0"},
        )
        root = parse_xml(xml_text)
        records = [self._map_scan_node(scan) for scan in root.findall(".//SCAN")]
        return self._finalize_cdc(records, start_offset, cursor_field="launch_date")

    def _read_scan_results(self, start_offset: dict) -> tuple[Iterator[dict], dict]:
        xml_text = self.client.request_text(
            "GET",
            "/api/2.0/fo/scan/vm/summary/",
            params={"action": "list", "output_format": "xml"},
        )
        root = parse_xml(xml_text)
        records = [self._map_scan_summary_node(scan) for scan in root.findall(".//SCAN")]
        return self._finalize_cdc(records, start_offset, cursor_field="scan_date")

    def _read_asset_groups(self) -> tuple[Iterator[dict], dict]:
        xml_text = self.client.request_text(
            "GET",
            "/api/2.0/fo/asset/group/",
            params={"action": "list"},
        )
        root = parse_xml(xml_text)
        records = [self._map_asset_group_node(group) for group in root.findall(".//ASSET_GROUP")]
        return iter(records), {}

    def _map_host_node(self, host: ElementTree.Element, table_name: str) -> dict[str, Any]:
        del table_name
        payload = {
            "host_id": self._to_int(get_text(host, "ID")) or -1,
            "asset_id": get_text(host, "ASSET_ID"),
            "ip": get_text(host, "IP"),
            "dns": get_text(host, "DNS"),
            "netbios": get_text(host, "NETBIOS"),
            "os": get_text(host, "OS"),
            "tracking_method": get_text(host, "TRACKING_METHOD"),
            "last_vuln_scan": as_iso8601(get_text(host, "LAST_VULN_SCAN_DATETIME")),
            "last_compliance_scan": as_iso8601(get_text(host, "LAST_COMPLIANCE_SCAN_DATETIME")),
        }
        payload["raw_payload"] = json.dumps(payload, separators=(",", ":"), default=str)
        return payload

    def _map_detection_node(
        self,
        detection: ElementTree.Element,
        *,
        host_id: int | None,
        host_ip: str | None,
    ) -> dict[str, Any]:
        qid = self._to_int(get_text(detection, "QID"))
        status = get_text(detection, "STATUS")
        last_found = as_iso8601(get_text(detection, "LAST_FOUND_DATETIME"))
        detection_id = f"{host_id or 0}:{qid or 0}:{status or 'UNKNOWN'}"
        cve_values = [cve.text.strip() for cve in detection.findall("./CVE_ID_LIST/CVE_ID") if cve.text]

        payload = {
            "detection_id": detection_id,
            "host_id": host_id,
            "ip": host_ip,
            "qid": qid,
            "severity": get_text(detection, "SEVERITY"),
            "status": status,
            "first_found": as_iso8601(get_text(detection, "FIRST_FOUND_DATETIME")),
            "last_found": last_found,
            "last_fixed": as_iso8601(get_text(detection, "LAST_FIXED_DATETIME")),
            "times_found": self._to_int(get_text(detection, "TIMES_FOUND")),
            "qds": get_text(detection, "QDS"),
            "cve": cve_values,
            "result": get_text(detection, "RESULT"),
        }
        payload["raw_payload"] = json.dumps(payload, separators=(",", ":"), default=str)
        return payload

    def _map_knowledgebase_node(self, vuln: ElementTree.Element) -> dict[str, Any]:
        qid = self._to_int(get_text(vuln, "QID")) or -1
        payload = {
            "qid": qid,
            "title": get_text(vuln, "TITLE"),
            "severity_level": get_text(vuln, "SEVERITY_LEVEL"),
            "category": get_text(vuln, "CATEGORY"),
            "published_date": as_iso8601(get_text(vuln, "PUBLISHED_DATETIME")),
            "last_service_modification_date": as_iso8601(
                coalesce(
                    get_text(vuln, "LAST_SERVICE_MODIFICATION_DATETIME"),
                    get_text(vuln, "CODE_MODIFIED_DATETIME"),
                )
            ),
            "patchable": get_text(vuln, "PATCHABLE"),
            "cve": [cve.text.strip() for cve in vuln.findall("./CVE_ID_LIST/CVE_ID") if cve.text],
            "vendor_reference": [
                reference.text.strip()
                for reference in vuln.findall("./VENDOR_REFERENCE_LIST/VENDOR_REFERENCE/ID")
                if reference.text
            ],
        }
        payload["raw_payload"] = json.dumps(payload, separators=(",", ":"), default=str)
        return payload

    def _map_scan_node(self, scan: ElementTree.Element) -> dict[str, Any]:
        payload = {
            "scan_ref": get_text(scan, "REF") or "",
            "scan_id": self._to_int(get_text(scan, "ID")),
            "title": get_text(scan, "TITLE"),
            "type": get_text(scan, "TYPE"),
            "state": get_text(scan, "STATE"),
            "launch_date": as_iso8601(get_text(scan, "LAUNCH_DATETIME")),
            "duration": get_text(scan, "DURATION"),
            "processed": get_text(scan, "PROCESSED"),
            "target": get_text(scan, "TARGET"),
        }
        payload["raw_payload"] = json.dumps(payload, separators=(",", ":"), default=str)
        return payload

    def _map_scan_summary_node(self, scan: ElementTree.Element) -> dict[str, Any]:
        payload = {
            "scan_ref": coalesce(get_text(scan, "SCAN_REF"), get_text(scan, "REF"), ""),
            "scan_id": self._to_int(get_text(scan, "SCAN_ID")),
            "status": get_text(scan, "STATUS"),
            "scan_date": as_iso8601(coalesce(get_text(scan, "SCAN_DATE"), get_text(scan, "LAUNCH_DATETIME"))),
            "hosts_scanned": self._to_int(get_text(scan, "HOSTS_SCANNED")),
            "hosts_alive": self._to_int(get_text(scan, "HOSTS_ALIVE")),
            "total_vulnerabilities": self._to_int(get_text(scan, "TOTAL_VULNERABILITIES")),
        }
        payload["raw_payload"] = json.dumps(payload, separators=(",", ":"), default=str)
        return payload

    def _map_asset_group_node(self, group: ElementTree.Element) -> dict[str, Any]:
        payload = {
            "asset_group_id": self._to_int(get_text(group, "ID")) or -1,
            "title": get_text(group, "TITLE"),
            "owner_id": self._to_int(get_text(group, "OWNER_ID")),
            "owner": get_text(group, "OWNER"),
            "network_id": self._to_int(get_text(group, "NETWORK_ID")),
            "last_update": as_iso8601(get_text(group, "LAST_UPDATE")),
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
