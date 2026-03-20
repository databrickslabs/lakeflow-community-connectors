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
    CURSOR_FIELD, DEFAULT_MAX_RECORDS, DEFAULT_PAGE_SIZE, DEFAULT_RESOURCES, PAGE_DELAY,
)


def _parse_ts(ts: str) -> datetime:
    """Parse a FHIR instant (ISO 8601 with timezone) to a datetime."""
    return datetime.fromisoformat(ts)
from databricks.labs.community_connector.sources.fhir.fhir_schemas import get_schema
from databricks.labs.community_connector.sources.fhir.fhir_utils import (
    FhirHttpClient, SmartAuthClient, discover_token_url, extract_record, iter_bundle_pages,
)


class FhirLakeflowConnect(LakeflowConnect):
    """LakeflowConnect implementation for FHIR R4 servers."""

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)

        auth_type = options.get("auth_type", "none")
        token_url = options.get("token_url", "").strip()

        if not token_url and auth_type != "none":
            token_url = discover_token_url(options["base_url"])

        auth_client = SmartAuthClient(
            token_url=token_url,
            client_id=options.get("client_id", ""),
            auth_type=auth_type,
            private_key_pem=options.get("private_key_pem", ""),
            client_secret=options.get("client_secret", ""),
            scope=options.get("scope", ""),
            kid=options.get("kid", ""),
            private_key_algorithm=options.get("private_key_algorithm", "RS384"),
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
        profile = table_options.get("profile", "uk_core")
        return get_schema(table_name, profile=profile)

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
        if since and _parse_ts(since) >= _parse_ts(self._init_ts):
            return iter([]), start_offset

        page_size = int(table_options.get("page_size", str(DEFAULT_PAGE_SIZE)))
        max_records = int(table_options.get("max_records_per_batch", str(DEFAULT_MAX_RECORDS)))
        page_delay = float(table_options.get("page_delay", str(PAGE_DELAY)))
        profile = table_options.get("profile", "uk_core")

        params: dict[str, str] = {"_count": str(page_size)}
        if since:
            params["_lastUpdated"] = f"gt{since}"

        records = []
        for resource in iter_bundle_pages(self._client, table_name, params, max_records=max_records, page_delay=page_delay):
            records.append(extract_record(resource, table_name, profile=profile))

        if not records:
            return iter([]), start_offset or {}

        # Detect if server ignored the _lastUpdated filter.
        # Only raise if there are records with a datable lastUpdated AND none of them are
        # newer than since — that combination is evidence the filter was ignored.
        # Records with null lastUpdated are excluded from this check (cardinality 0..1 in spec).
        if since:
            has_datable = [r for r in records if r.get(CURSOR_FIELD)]
            since_dt = _parse_ts(since)
            newer = [r for r in has_datable if _parse_ts(r[CURSOR_FIELD]) > since_dt]
            if has_datable and not newer:
                raise RuntimeError(
                    f"FHIR server appears to have ignored the '_lastUpdated=gt{since}' filter — "
                    f"all {len(has_datable)} records with a lastUpdated for '{table_name}' have "
                    f"lastUpdated <= '{since}'. "
                    f"This server may not support the _lastUpdated search parameter. "
                    f"See README.md for server requirements."
                )

        # Advance cursor to max lastUpdated across batch.
        cursors = [r[CURSOR_FIELD] for r in records if r.get(CURSOR_FIELD)]
        if not cursors:
            return iter(records), start_offset or {}

        last_cursor = max(cursors, key=_parse_ts)
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
