"""FHIR R4 Lakeflow Community Connector.

Implements LakeflowConnect to ingest FHIR R4 resources from any
SMART-on-FHIR-compliant server into Databricks Delta tables.

See fhir_api_doc.md for API details and design decisions.
"""

from datetime import datetime, timezone
from typing import Iterator

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.fhir.fhir_constants import (
    CURSOR_FIELD, DEFAULT_MAX_RECORDS, DEFAULT_PAGE_SIZE, DEFAULT_RESOURCES,
)
from databricks.labs.community_connector.sources.fhir.fhir_schemas import get_schema
from databricks.labs.community_connector.sources.fhir.fhir_utils import (
    FhirHttpClient, SmartAuthClient, extract_record, iter_bundle_pages,
)


class FhirLakeflowConnect(LakeflowConnect):
    """LakeflowConnect implementation for FHIR R4 servers."""

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)

        auth_client = SmartAuthClient(
            token_url=options.get("token_url", ""),
            client_id=options.get("client_id", ""),
            auth_type=options.get("auth_type", "none"),
            private_key_pem=options.get("private_key_pem", ""),
            client_secret=options.get("client_secret", ""),
            scope=options.get("scope", ""),
        )
        self._client = FhirHttpClient(base_url=options["base_url"], auth_client=auth_client)

        # Cap cursor at init time -- never chase data written after this trigger started.
        self._init_ts = datetime.now(timezone.utc).isoformat()

        resource_types_opt = options.get("resource_types", "").strip()
        self._resources = (
            [r.strip() for r in resource_types_opt.split(",") if r.strip()]
            if resource_types_opt
            else list(DEFAULT_RESOURCES)
        )

    def list_tables(self) -> list[str]:
        return list(self._resources)

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        self._validate_table(table_name)
        return get_schema(table_name)

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        self._validate_table(table_name)
        return {
            "primary_keys": ["id"],
            "cursor_field": CURSOR_FIELD,
            "ingestion_type": "cdc",
        }

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Fetch FHIR resources with bundle pagination and _lastUpdated incremental sync.

        Uses max_records_per_batch admission control (always required for CDC tables).
        Short-circuits when cursor >= _init_ts to prevent chasing live writes.
        """
        self._validate_table(table_name)

        since = start_offset.get("cursor") if start_offset else None
        if since and since >= self._init_ts:
            return iter([]), start_offset

        page_size = int(table_options.get("page_size", str(DEFAULT_PAGE_SIZE)))
        max_records = int(table_options.get("max_records_per_batch", str(DEFAULT_MAX_RECORDS)))

        params: dict[str, str] = {"_count": str(page_size)}
        if since:
            params["_lastUpdated"] = f"gt{since}"

        records = []
        for resource in iter_bundle_pages(self._client, table_name, params, max_records=max_records):
            records.append(extract_record(resource, table_name))

        if not records:
            return iter([]), start_offset or {}

        # Advance cursor to max lastUpdated across batch.
        # ISO8601 string comparison is correct for FHIR instants (always timezone-qualified).
        cursors = [r[CURSOR_FIELD] for r in records if r.get(CURSOR_FIELD)]
        if not cursors:
            return iter(records), start_offset or {}

        last_cursor = max(cursors)
        end_offset = {"cursor": last_cursor}

        if start_offset and start_offset == end_offset:
            return iter([]), start_offset

        return iter(records), end_offset

    def _validate_table(self, table_name: str) -> None:
        if table_name not in self._resources:
            raise ValueError(
                f"Resource type '{table_name}' is not configured. "
                f"Configured: {self._resources}"
            )
