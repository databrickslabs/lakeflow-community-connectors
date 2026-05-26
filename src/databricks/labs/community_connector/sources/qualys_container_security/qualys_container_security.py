"""qualys_container_security connector."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Iterator

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.qualys_common import (
    QualysClient,
    as_iso8601,
    coalesce,
    epoch_millis_to_iso,
)
from databricks.labs.community_connector.sources.qualys_container_security.qualys_container_security_schemas import (
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
)


class QualysContainerSecurityLakeflowConnect(LakeflowConnect):
    """Lakeflow connector for Qualys Container Security."""

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        token = options.get("token")
        if not token:
            raise ValueError("qualys_container_security requires bearer token")

        self.client = QualysClient(
            base_url=options.get("base_url", "https://qualysapi.qualys.com"),
            auth_mode="bearer",
            token=token,
            timeout_seconds=int(options.get("timeout_seconds", "30")),
            extra_headers={"accept": "application/json"},
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
        if table_name == "containers":
            return self._read_containers(start_offset)
        if table_name == "container_images":
            return self._read_images(start_offset)
        if table_name == "container_vulnerabilities":
            return self._read_container_vulnerabilities(start_offset)
        if table_name == "registries":
            return self._read_registries()
        if table_name == "sensors":
            return self._read_sensors()
        if table_name == "container_scan_results":
            return self._read_container_scan_results(start_offset)
        raise ValueError(f"Unsupported table: {table_name!r}")

    def _read_containers(self, start_offset: dict) -> tuple[Iterator[dict], dict]:
        body = self.client.request_json("GET", "/csapi/v1.3/containers", params={"pageNumber": 1, "pageSize": 1000})
        data = body.get("data", []) if isinstance(body, dict) else []
        records = [self._map_container(row) for row in data if isinstance(row, dict)]
        return self._finalize_cdc(records, start_offset, cursor_field="updated_at")

    def _read_images(self, start_offset: dict) -> tuple[Iterator[dict], dict]:
        body = self.client.request_json("GET", "/csapi/v1.3/images", params={"pageNumber": 1, "pageSize": 1000})
        data = body.get("data", []) if isinstance(body, dict) else []
        records = [self._map_image(row) for row in data if isinstance(row, dict)]
        return self._finalize_cdc(records, start_offset, cursor_field="updated_at")

    def _read_container_vulnerabilities(self, start_offset: dict) -> tuple[Iterator[dict], dict]:
        images_body = self.client.request_json("GET", "/csapi/v1.3/images", params={"pageNumber": 1, "pageSize": 1000})
        image_rows = images_body.get("data", []) if isinstance(images_body, dict) else []
        findings: list[dict[str, Any]] = []

        for image in image_rows:
            if not isinstance(image, dict):
                continue
            image_sha = coalesce(image.get("sha"), image.get("imageSha"))
            if not image_sha:
                continue

            vuln_body = self.client.request_json(
                "GET",
                f"/csapi/v1.3/images/{image_sha}/vuln",
                params={"type": "ALL", "applyException": "true"},
            )
            vulnerabilities = vuln_body.get("vulnerabilities", []) if isinstance(vuln_body, dict) else []
            for vuln in vulnerabilities:
                if isinstance(vuln, dict):
                    findings.append(self._map_container_vuln(vuln, image_sha=str(image_sha)))

        return self._finalize_cdc(findings, start_offset, cursor_field="last_found")

    def _read_registries(self) -> tuple[Iterator[dict], dict]:
        body = self.client.request_json("GET", "/csapi/v1.3/registry", params={"pageNumber": 1, "pageSize": 1000})
        data = body.get("data", []) if isinstance(body, dict) else []
        records = [self._map_registry(row) for row in data if isinstance(row, dict)]
        return iter(records), {}

    def _read_sensors(self) -> tuple[Iterator[dict], dict]:
        body = self.client.request_json("GET", "/csapi/v1.3/sensors", params={"pageNumber": 1, "pageSize": 1000})
        data = body.get("data", []) if isinstance(body, dict) else []
        records = [self._map_sensor(row) for row in data if isinstance(row, dict)]
        return iter(records), {}

    def _read_container_scan_results(self, start_offset: dict) -> tuple[Iterator[dict], dict]:
        body = self.client.request_json("GET", "/csapi/v1.3/images", params={"pageNumber": 1, "pageSize": 1000})
        data = body.get("data", []) if isinstance(body, dict) else []
        records = [self._map_container_scan_result(row) for row in data if isinstance(row, dict)]
        return self._finalize_cdc(records, start_offset, cursor_field="updated_at")

    def _map_container(self, row: dict[str, Any]) -> dict[str, Any]:
        payload = {
            "container_sha": str(coalesce(row.get("sha"), row.get("containerSha"), row.get("uuid"), "")),
            "container_id": coalesce(row.get("containerId"), row.get("container_id"), row.get("uuid")),
            "name": row.get("name"),
            "state": row.get("state"),
            "image_sha": coalesce(row.get("imageSha"), row.get("imageId")),
            "created_at": epoch_millis_to_iso(row.get("created")),
            "updated_at": epoch_millis_to_iso(row.get("updated")),
            "host_name": coalesce((row.get("host") or {}).get("hostname") if isinstance(row.get("host"), dict) else None, row.get("hostName")),
            "sensor_id": coalesce((row.get("sensor") or {}).get("sensorId") if isinstance(row.get("sensor"), dict) else None, row.get("sensorId")),
        }
        payload["raw_payload"] = json.dumps(row, separators=(",", ":"), default=str)
        return payload

    def _map_image(self, row: dict[str, Any]) -> dict[str, Any]:
        tags = row.get("repoTags") if isinstance(row.get("repoTags"), list) else []
        first_repo = tags[0] if tags else {}
        payload = {
            "image_sha": str(coalesce(row.get("sha"), row.get("imageSha"), row.get("uuid"), "")),
            "image_id": coalesce(row.get("imageId"), row.get("image_id"), row.get("uuid")),
            "repo": first_repo.get("repo") if isinstance(first_repo, dict) else row.get("repo"),
            "tag": first_repo.get("tag") if isinstance(first_repo, dict) else row.get("tag"),
            "created_at": epoch_millis_to_iso(row.get("created")),
            "updated_at": epoch_millis_to_iso(row.get("updated")),
            "in_use": bool(row.get("imagesInUse")) if row.get("imagesInUse") is not None else None,
            "registry": coalesce(row.get("registry"), row.get("registryName")),
        }
        payload["raw_payload"] = json.dumps(row, separators=(",", ":"), default=str)
        return payload

    def _map_container_vuln(self, vuln: dict[str, Any], *, image_sha: str) -> dict[str, Any]:
        qid = self._to_int(coalesce(vuln.get("qid"), vuln.get("vulnerability", {}).get("qid") if isinstance(vuln.get("vulnerability"), dict) else None))
        first_found = as_iso8601(coalesce(vuln.get("firstFound"), vuln.get("first_found"), vuln.get("firstDetected")))
        last_found = as_iso8601(coalesce(vuln.get("lastFound"), vuln.get("last_found"), vuln.get("lastDetected")))
        cve_values = vuln.get("cve") if isinstance(vuln.get("cve"), list) else []
        status = coalesce(vuln.get("status"), vuln.get("state"), "OPEN")

        payload = {
            "finding_id": f"{image_sha}:{qid or 0}:{status}",
            "image_sha": image_sha,
            "container_sha": coalesce(vuln.get("containerSha"), vuln.get("container_sha")),
            "qid": qid,
            "severity": coalesce(vuln.get("severity"), vuln.get("severityLevel")),
            "status": status,
            "first_found": first_found,
            "last_found": last_found,
            "cve": [str(c) for c in cve_values],
            "result": coalesce(vuln.get("result"), vuln.get("diagnosis")),
        }
        payload["raw_payload"] = json.dumps(vuln, separators=(",", ":"), default=str)
        return payload

    def _map_registry(self, row: dict[str, Any]) -> dict[str, Any]:
        payload = {
            "registry_id": str(coalesce(row.get("registryId"), row.get("id"), row.get("uuid"), "")),
            "registry_name": row.get("registryName"),
            "registry_type": row.get("registryType"),
            "registry_uri": row.get("registryUri"),
            "created_at": epoch_millis_to_iso(row.get("created")),
            "updated_at": epoch_millis_to_iso(row.get("updated")),
        }
        payload["raw_payload"] = json.dumps(row, separators=(",", ":"), default=str)
        return payload

    def _map_sensor(self, row: dict[str, Any]) -> dict[str, Any]:
        payload = {
            "sensor_id": str(coalesce(row.get("sensorId"), row.get("uuid"), "")),
            "hostname": row.get("hostname"),
            "platform": row.get("platform"),
            "sensor_version": row.get("sensorVersion"),
            "ipv4": row.get("ipv4"),
            "last_checked_in": epoch_millis_to_iso(row.get("lastCheckedIn")),
        }
        payload["raw_payload"] = json.dumps(row, separators=(",", ":"), default=str)
        return payload

    def _map_container_scan_result(self, row: dict[str, Any]) -> dict[str, Any]:
        image_sha = str(coalesce(row.get("sha"), row.get("imageSha"), row.get("uuid"), ""))
        vuln_summary = row.get("vulnSummary") if isinstance(row.get("vulnSummary"), dict) else {}
        payload = {
            "scan_result_id": image_sha,
            "image_sha": image_sha,
            "status": coalesce(row.get("status"), row.get("scanStatus"), "SCANNED"),
            "updated_at": epoch_millis_to_iso(row.get("updated")),
            "severity_count": json.dumps(vuln_summary, separators=(",", ":"), default=str),
        }
        payload["raw_payload"] = json.dumps(row, separators=(",", ":"), default=str)
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
