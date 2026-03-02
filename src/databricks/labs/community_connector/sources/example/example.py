import time
from datetime import datetime, timezone
from typing import Iterator

from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.libs.simulated_source.api import get_api

_RETRIABLE_STATUS_CODES = {429, 500, 503}
_MAX_RETRIES = 5
_INITIAL_BACKOFF = 1.0

_SPARK_TYPE_MAP = {
    "string": StringType(),
    "integer": LongType(),
    "double": DoubleType(),
    "timestamp": TimestampType(),
    "date": DateType(),
}

_INGESTION_TYPE_OVERRIDES = {
    "metrics": "cdc",
    "events": "append_only",
    "orders": "cdc_with_deletes",
}

_METRICS_SCHEMA = StructType(
    [
        StructField("metric_id", StringType(), nullable=False),
        StructField("name", StringType(), nullable=True),
        StructField(
            "value",
            StructType(
                [
                    StructField("count", LongType(), nullable=True),
                    StructField("label", StringType(), nullable=True),
                    StructField("measure", DoubleType(), nullable=True),
                ]
            ),
            nullable=True,
        ),
        StructField("host", StringType(), nullable=True),
        StructField("updated_at", TimestampType(), nullable=False),
    ]
)

_METRICS_METADATA = {
    "primary_keys": ["metric_id"],
    "cursor_field": "updated_at",
}


def _build_spark_type(field_descriptor: dict) -> StructField:
    field_type = field_descriptor["type"]
    if field_type == "struct":
        children = [_build_spark_type(f) for f in field_descriptor.get("fields", [])]
        spark_type = StructType(children)
    else:
        spark_type = _SPARK_TYPE_MAP[field_type]
    return StructField(
        field_descriptor["name"],
        spark_type,
        nullable=field_descriptor.get("nullable", True),
    )


class ExampleLakeflowConnect(LakeflowConnect):
    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        username = options.get("username", "default_user")
        password = options.get("password", "default_pass")
        self._api = get_api(username, password)
        self._init_ts = datetime.now(timezone.utc).isoformat()

    def _request_with_retry(self, method: str, path: str, **kwargs):
        backoff = _INITIAL_BACKOFF
        for attempt in range(_MAX_RETRIES):
            if method == "GET":
                resp = self._api.get(path, **kwargs)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            if resp.status_code not in _RETRIABLE_STATUS_CODES:
                return resp

            if attempt < _MAX_RETRIES - 1:
                time.sleep(backoff)
                backoff *= 2

        return resp

    def list_tables(self) -> list[str]:
        resp = self._request_with_retry("GET", "/tables")
        tables = resp.json()["tables"]
        tables.append("metrics")
        return tables

    def get_table_schema(
        self, table_name: str, table_options: dict[str, str]
    ) -> StructType:
        self._validate_table(table_name)
        if table_name == "metrics":
            return _METRICS_SCHEMA

        resp = self._request_with_retry("GET", f"/tables/{table_name}/schema")
        if resp.status_code != 200:
            raise RuntimeError(
                f"Failed to get schema for '{table_name}': {resp.json()}"
            )
        fields = resp.json()["schema"]
        return StructType([_build_spark_type(f) for f in fields])

    def read_table_metadata(
        self, table_name: str, table_options: dict[str, str]
    ) -> dict:
        self._validate_table(table_name)
        if table_name == "metrics":
            metadata = dict(_METRICS_METADATA)
        else:
            resp = self._request_with_retry(
                "GET", f"/tables/{table_name}/metadata"
            )
            if resp.status_code != 200:
                raise RuntimeError(
                    f"Failed to get metadata for '{table_name}': {resp.json()}"
                )
            metadata = dict(resp.json()["metadata"])

        if table_name in _INGESTION_TYPE_OVERRIDES:
            metadata["ingestion_type"] = _INGESTION_TYPE_OVERRIDES[table_name]
        elif metadata.get("cursor_field"):
            metadata["ingestion_type"] = "cdc"
        else:
            metadata["ingestion_type"] = "snapshot"

        return metadata

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        self._validate_table(table_name)
        metadata = self.read_table_metadata(table_name, table_options)
        ingestion_type = metadata["ingestion_type"]

        if ingestion_type == "snapshot":
            return self._read_snapshot(table_name, table_options)

        cursor_field = metadata.get("cursor_field")
        return self._read_incremental(
            table_name, start_offset, table_options, cursor_field
        )

    def read_table_deletes(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        self._validate_table(table_name)
        if table_name != "orders":
            raise ValueError(
                f"Table '{table_name}' does not support deleted records"
            )

        since = start_offset.get("cursor") if start_offset else None
        max_records = int(table_options.get("max_records_per_batch", "200"))

        records = []
        page = 1
        while len(records) < max_records:
            params = {"page": str(page)}
            if since:
                params["since"] = since

            resp = self._request_with_retry(
                "GET", "/tables/orders/deleted_records", params=params
            )
            if resp.status_code != 200:
                raise RuntimeError(
                    f"Failed to read deleted records for 'orders': {resp.json()}"
                )

            body = resp.json()
            batch = body["records"]
            if not batch:
                break

            records.extend(batch)

            if body["next_page"] is None:
                break
            page = body["next_page"]

        if not records:
            return iter([]), start_offset or {}

        max_cursor = max(r["updated_at"] for r in records)
        if max_cursor > self._init_ts:
            max_cursor = self._init_ts

        end_offset = {"cursor": max_cursor}
        if start_offset and start_offset == end_offset:
            return iter([]), start_offset

        return iter(records), end_offset

    def _validate_table(self, table_name: str) -> None:
        supported = self.list_tables()
        if table_name not in supported:
            raise ValueError(
                f"Table '{table_name}' is not supported. "
                f"Supported tables: {supported}"
            )

    def _read_snapshot(
        self, table_name: str, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        records = []
        page = 1
        params = {}

        if table_name == "products" and "category" in table_options:
            params["category"] = table_options["category"]

        while True:
            params["page"] = str(page)
            resp = self._request_with_retry(
                "GET", f"/tables/{table_name}/records", params=params
            )
            if resp.status_code != 200:
                raise RuntimeError(
                    f"Failed to read records from '{table_name}': {resp.json()}"
                )

            body = resp.json()
            records.extend(body["records"])

            if body["next_page"] is None:
                break
            page = body["next_page"]

        return iter(records), None

    def _read_incremental(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
        cursor_field: str,
    ) -> tuple[Iterator[dict], dict]:
        since = start_offset.get("cursor") if start_offset else None
        max_records = int(table_options.get("max_records_per_batch", "200"))

        params = {}
        if since:
            params["since"] = since
        if table_name == "metrics":
            params["until"] = self._init_ts

        if table_name == "orders":
            for opt_key in ("user_id", "status"):
                if opt_key in table_options:
                    params[opt_key] = table_options[opt_key]

        if table_name == "events" and "limit" in table_options:
            params["limit"] = table_options["limit"]

        records = []
        page = 1
        while len(records) < max_records:
            params["page"] = str(page)
            resp = self._request_with_retry(
                "GET", f"/tables/{table_name}/records", params=params
            )
            if resp.status_code != 200:
                raise RuntimeError(
                    f"Failed to read records from '{table_name}': {resp.json()}"
                )

            body = resp.json()
            batch = body["records"]
            if not batch:
                break

            records.extend(batch)

            if body["next_page"] is None:
                break
            page = body["next_page"]

        if max_records and len(records) > max_records:
            records = records[:max_records]

        if not records:
            return iter([]), start_offset or {}

        max_cursor = max(r[cursor_field] for r in records)
        if max_cursor > self._init_ts:
            max_cursor = self._init_ts

        end_offset = {"cursor": max_cursor}
        if start_offset and start_offset == end_offset:
            return iter([]), start_offset

        return iter(records), end_offset
