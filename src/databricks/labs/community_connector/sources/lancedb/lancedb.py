"""Lakeflow community connector for LanceDB Cloud.

LanceDB Cloud exposes a vector-first REST API.  Tables are user-defined and
fully dynamic, so this connector discovers them at runtime:

| Step                | Endpoint                                   |
|---------------------|--------------------------------------------|
| ``list_tables``     | ``GET /v1/table/`` (page_token paginated)  |
| ``get_table_schema``| ``POST /v1/table/{name}/describe/``        |
| ``read_table``      | ``POST /v1/table/{name}/query/`` (Arrow IPC)|

Authentication is an ``x-api-key`` header; the project/database name and
region are encoded into the host (``https://{project}.{region}.api.lancedb.com``).

Read model
----------
Reads are **snapshot-only**: every table is read as a full-table scan and
re-read in full on each run.  LanceDB's query endpoint is top-K and
vector-first, so the scan is performed with an all-zero *dummy* vector sized to
the table's embedding dimension (auto-detected from ``describe``),
``bypass_vector_index: true``, and offset/``k`` pagination.  An optional user
``filter_expression`` is applied server-side.  Responses arrive as Apache Arrow
IPC; the parser tries the streaming then the file sub-format, and also accepts
plain JSON so the connector is exercisable against the in-process source
simulator.

Ingestion type is always ``snapshot`` with no primary keys and no cursor:
LanceDB's REST API exposes no primary keys, no cursor/watermark, and no
change/delete feed, so incremental (cdc) reads are not supported.

A note on partitioning: LanceDB offers only offset-based pagination (no
``since``/``until`` range query and no row-count endpoint to pre-split an
offset range), and publishes no rate limits.  There is therefore no reliable
axis on which to distribute parallel reads, so this connector implements the
single-driver ``LakeflowConnect`` interface rather than
``SupportsPartitionedStream``.
"""

import importlib
import io
import json
import time
from typing import Iterator
from urllib.parse import quote

import requests
from pyspark.sql.types import StructField, StructType
from requests.exceptions import RequestException

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.lancedb.lancedb_schemas import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_TIMEOUT,
    INITIAL_BACKOFF,
    LIST_TABLES_LIMIT,
    MAX_BATCH_SIZE,
    MAX_RETRIES,
    RETRIABLE_STATUS_CODES,
    TABLE_SCHEMAS,
    TABLES,
    arrow_type_to_spark_type,
    vector_dimension,
)


class LancedbLakeflowConnect(LakeflowConnect):
    """LakeflowConnect implementation for LanceDB Cloud."""

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        self._api_key = options.get("api_key")
        project = options.get("project_name") or options.get("database")
        region = options.get("region")
        if not self._api_key or not project or not region:
            raise ValueError(
                "LanceDB connector requires 'api_key', 'project_name', and 'region' "
                "in options"
            )
        self._project = self._sanitize_identifier(project)
        self._region = self._sanitize_identifier(region)
        self._base_url = f"https://{self._project}.{self._region}.api.lancedb.com"

        # Cache per-table embedding dimension so a full scan does not re-describe
        # on every page.
        self._vector_dims: dict[str, int | None] = {}

    # ----- HTTP -----------------------------------------------------------

    def _request_with_retry(
        self,
        method: str,
        path: str,
        json_data: dict | None = None,
        params: dict | None = None,
    ) -> requests.Response:
        """Issue an API request, retrying transient errors with backoff.

        A fresh ``requests`` call is used per request (rather than a cached
        ``Session``) so the connector instance carries no non-picklable state
        when shipped to Spark executors.
        """
        url = f"{self._base_url}{path}"
        headers = {
            "x-api-key": self._api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        backoff = INITIAL_BACKOFF
        resp = None
        last_exc: RequestException | None = None
        for attempt in range(MAX_RETRIES):
            try:
                resp = requests.request(
                    method,
                    url,
                    headers=headers,
                    json=json_data,
                    params=params,
                    timeout=DEFAULT_TIMEOUT,
                )
            except RequestException as exc:
                last_exc = exc
            else:
                if resp.status_code not in RETRIABLE_STATUS_CODES:
                    return resp
            if attempt < MAX_RETRIES - 1:
                sleep_for = backoff
                if resp is not None:
                    retry_after = resp.headers.get("Retry-After", "").strip()
                    if retry_after:
                        try:
                            sleep_for = max(sleep_for, float(retry_after))
                        except ValueError:
                            pass
                time.sleep(sleep_for)
                backoff *= 2
        if resp is None and last_exc is not None:
            raise RuntimeError(
                f"LanceDB request failed after {MAX_RETRIES} attempts for {path}: "
                f"{last_exc}"
            ) from last_exc
        return resp

    # ----- interface ------------------------------------------------------

    def list_tables(self) -> list[str]:
        """Discover tables via ``GET /v1/table/`` (page_token paginated).

        Falls back to the built-in example tables when discovery yields nothing
        (e.g. offline simulate-mode tests), mirroring the HubSpot connector's
        empty-discovery fallback.
        """
        names: list[str] = []
        params = {"limit": LIST_TABLES_LIMIT}
        while True:
            resp = self._request_with_retry("GET", "/v1/table/", params=params)
            if resp.status_code != 200:
                break
            body = resp.json()
            names.extend(self._extract_table_names(body))
            page_token = body.get("page_token") if isinstance(body, dict) else None
            if not page_token:
                break
            params = {"limit": LIST_TABLES_LIMIT, "page_token": page_token}
        return names or list(TABLES)

    def get_table_schema(
        self, table_name: str, table_options: dict[str, str]
    ) -> StructType:
        """Derive the Spark schema from ``POST /v1/table/{name}/describe/``.

        Falls back to the built-in example schema when describe returns no
        fields.  When a ``columns`` table option is set, the schema is projected
        to those columns so it matches the records ``read_table`` returns.
        """
        safe = self._sanitize_identifier(table_name)
        resp = self._request_with_retry(
            "POST", f"/v1/table/{quote(safe)}/describe/", json_data={}
        )
        schema: StructType | None = None
        if resp.status_code == 200:
            schema = self._schema_from_describe(resp.json())
        if schema is None or not schema.fields:
            schema = TABLE_SCHEMAS.get(table_name)
        if schema is None:
            raise RuntimeError(
                f"Could not determine schema for table '{table_name}'"
            )

        columns = self._columns_option(table_options)
        if columns:
            wanted = set(columns)
            schema = StructType([f for f in schema.fields if f.name in wanted])
        return schema

    @staticmethod
    def _schema_from_describe(body) -> StructType | None:
        """Build a Spark schema from a ``describe`` response, or ``None``.

        Tolerates the structured and flattened Arrow ``type`` shapes and returns
        ``None`` when the response carries no field list (so the caller can fall
        back to a built-in schema).
        """
        schema = body.get("schema") if isinstance(body, dict) else None
        if not isinstance(schema, dict):
            return None
        fields = schema.get("fields")
        if not isinstance(fields, list):
            return None
        spark_fields = [
            StructField(
                field.get("name", ""),
                arrow_type_to_spark_type(field.get("type", {})),
                field.get("nullable", True),
            )
            for field in fields
            if isinstance(field, dict)
        ]
        return StructType(spark_fields)

    def read_table_metadata(
        self, table_name: str, table_options: dict[str, str]
    ) -> dict:
        """Report snapshot ingestion metadata.

        Reads are snapshot-only: ``ingestion_type`` is always ``snapshot`` and
        there is no cursor.  LanceDB's REST API exposes no primary keys, no
        cursor/watermark, and no change/delete feed, so incremental (cdc) reads
        are not supported.

        A single primary-key column is derived from the table's own schema
        (preferring an ``id`` column, else the first field) purely to key the
        Delta merge on the destination — LanceDB itself declares none.

        Rejects malformed table names up front (the same identifier check the
        schema / query paths apply) so an unknown / injection-shaped name raises
        rather than silently returning metadata.
        """
        self._sanitize_identifier(table_name)
        schema = self.get_table_schema(table_name, table_options)
        return {
            "primary_keys": self._infer_primary_keys(schema),
            "cursor_field": None,
            "ingestion_type": "snapshot",
        }

    @staticmethod
    def _infer_primary_keys(schema: StructType) -> list[str]:
        """Pick a single scalar key column, preferring ``id``.

        Never returns a vector/array, binary, or nested (struct/map) column —
        those make no sense as a Delta merge key.  Falls back to the first
        field only when the table has no scalar column at all.
        """
        fields = schema.fields
        if not fields:
            return []
        non_key_types = ("array", "binary", "struct", "map")
        scalar = [f.name for f in fields if f.dataType.typeName() not in non_key_types]
        for name in scalar:
            if name.lower() == "id":
                return [name]
        if scalar:
            return [scalar[0]]
        return [fields[0].name]

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Full-table snapshot scan; returns an empty offset (non-incremental).

        All rows are read in a single call and an empty ``end_offset`` is
        returned, so the stream terminates after one batch.
        """
        records = list(self._scan(table_name, table_options))
        return iter(records), {}

    # ----- query / scan ---------------------------------------------------

    def _scan(
        self, table_name: str, table_options: dict[str, str]
    ) -> Iterator[dict]:
        """Offset-paginate a full-table scan via ``POST .../query/``.

        Uses an all-zero dummy vector (sized to the embedding dimension) with
        ``bypass_vector_index`` so the query behaves as a plain scan rather than
        a similarity search.  An optional user ``filter_expression`` is applied
        server-side.  Stops when a short page is returned.
        """
        safe = self._sanitize_identifier(table_name)
        endpoint = f"/v1/table/{quote(safe)}/query/"
        dim = self._get_vector_dimension(table_name)
        dummy_vector = [0.0] * dim if dim else [0.0]
        columns = self._columns_option(table_options)
        batch_size = self._batch_size_option(table_options)
        filter_expr = table_options.get("filter_expression")

        offset = 0
        while True:
            payload: dict = {
                "vector": {"single_vector": dummy_vector},
                "k": batch_size,
                "bypass_vector_index": True,
                "offset": offset,
            }
            if filter_expr:
                payload["filter"] = filter_expr
                payload["prefilter"] = True
            if columns:
                payload["columns"] = {"column_names": columns}

            resp = self._request_with_retry("POST", endpoint, json_data=payload)
            if resp.status_code != 200:
                raise RuntimeError(
                    f"LanceDB query error for '{table_name}': "
                    f"{resp.status_code} {resp.text[:500]}"
                )

            rows = self._project_columns(self._parse_query_response(resp), columns)
            yield from rows

            if len(rows) < batch_size:
                break
            offset += batch_size

    def _get_vector_dimension(self, table_name: str) -> int | None:
        """Detect the embedding dimension from ``describe`` (cached per table)."""
        if table_name in self._vector_dims:
            return self._vector_dims[table_name]

        dim: int | None = None
        safe = self._sanitize_identifier(table_name)
        resp = self._request_with_retry(
            "POST", f"/v1/table/{quote(safe)}/describe/", json_data={}
        )
        if resp.status_code == 200:
            body = resp.json()
            schema = body.get("schema", {}) if isinstance(body, dict) else {}
            for field in schema.get("fields", []) if isinstance(schema, dict) else []:
                if not isinstance(field, dict):
                    continue
                found = vector_dimension(field.get("type"))
                if found:
                    dim = found
                    break
        self._vector_dims[table_name] = dim
        return dim

    @staticmethod
    def _parse_query_response(resp: requests.Response) -> list[dict]:
        """Decode a query response — Apache Arrow IPC (real API) or JSON (sim)."""
        content = resp.content or b""
        content_type = resp.headers.get("Content-Type", "").lower()

        if "arrow" in content_type or content[:5] == b"ARROW":
            return LancedbLakeflowConnect._parse_arrow(content)

        try:
            data = resp.json()
        except ValueError:
            # No usable Content-Type but the body may still be Arrow.
            if content:
                return LancedbLakeflowConnect._parse_arrow(content)
            return []

        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for key in ("data", "records", "rows", "results"):
                value = data.get(key)
                if isinstance(value, list):
                    return value
        return []

    @staticmethod
    def _parse_arrow(content: bytes) -> list[dict]:
        """Decode an Apache Arrow IPC payload, trying stream then file format."""
        if not content:
            return []
        pa = importlib.import_module("pyarrow")

        try:
            table = pa.ipc.open_stream(io.BytesIO(content)).read_all()
        except (pa.ArrowInvalid, OSError, ValueError):
            table = pa.ipc.open_file(io.BytesIO(content)).read_all()
        return table.to_pylist()

    @staticmethod
    def _project_columns(rows: list[dict], columns: list[str] | None) -> list[dict]:
        """Re-filter rows to the requested columns.

        Some LanceDB API versions ignore server-side ``columns`` projection and
        return all columns; this makes the client honour the request regardless
        (a no-op when the server already projected).
        """
        if not columns:
            return rows
        wanted = set(columns)
        if rows and all(key in wanted for key in rows[0].keys()):
            return rows
        return [{k: v for k, v in row.items() if k in wanted} for row in rows]

    # ----- options helpers ------------------------------------------------

    def _batch_size_option(self, table_options: dict[str, str]) -> int:
        raw = table_options.get("batch_size", str(DEFAULT_BATCH_SIZE))
        try:
            value = int(raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"LanceDB option 'batch_size' must be an integer; got {raw!r}"
            ) from exc
        return max(1, min(value, MAX_BATCH_SIZE))

    @staticmethod
    def _columns_option(table_options: dict[str, str]) -> list[str] | None:
        """Parse the ``columns`` projection option (JSON array, or a single name).

        Validates each name (alphanumeric, underscore, dot) to prevent
        injection into the query ``columns`` projection.
        """
        raw = table_options.get("columns")
        if raw is None:
            return None
        if isinstance(raw, list):
            value = raw
        else:
            try:
                value = json.loads(raw)
            except (TypeError, json.JSONDecodeError):
                value = [raw]
            if not isinstance(value, list):
                value = [value]

        names: list[str] = []
        for name in value:
            if not isinstance(name, str):
                raise ValueError(
                    "LanceDB option 'columns' must be a list of column names"
                )
            if not name.replace("_", "").replace(".", "").isalnum():
                raise ValueError(
                    f"Invalid column name in 'columns': {name!r}. Only alphanumeric, "
                    "underscore, and dot are allowed."
                )
            names.append(name)
        return names or None

    @staticmethod
    def _extract_table_names(body) -> list[str]:
        """Extract table names from a List Tables response (bare or wrapped)."""
        tables = body.get("tables", []) if isinstance(body, dict) else body
        if not isinstance(tables, list):
            return []
        names = []
        for entry in tables:
            if isinstance(entry, dict):
                name = entry.get("name")
                if isinstance(name, str):
                    names.append(name)
            elif isinstance(entry, str):
                names.append(entry)
        return names

    @staticmethod
    def _sanitize_identifier(identifier: str) -> str:
        """Reject identifiers with characters unsafe for URL/host interpolation."""
        if not identifier or not identifier.replace("-", "").replace("_", "").isalnum():
            raise ValueError(
                f"Invalid identifier: {identifier!r}. Only alphanumeric, hyphen, and "
                "underscore are allowed."
            )
        return identifier
