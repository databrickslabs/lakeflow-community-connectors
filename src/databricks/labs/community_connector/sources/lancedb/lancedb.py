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
Every read is a full-table scan built on LanceDB's top-K, vector-first query
endpoint: the scan uses an all-zero *dummy* vector sized to the table's
embedding dimension (auto-detected from ``describe``), ``bypass_vector_index:
true``, and offset/``k`` pagination.  An optional user ``filter_expression`` is
applied server-side.  Responses arrive as Apache Arrow IPC; the parser tries the
streaming then the file sub-format, and also accepts plain JSON so the connector
is exercisable against the in-process source simulator.

Ingestion mode (opt-in via the ``ingestion_type`` table option)
---------------------------------------------------------------
* ``snapshot`` (default) — full scan; empty offset ``{}``; the whole table is
  re-read on every run.
* ``cdc`` — one incremental microbatch per call.  Requires a ``cursor_field``
  table option naming an orderable column (e.g. a timestamp or monotonic id).
  Each call queries ``{cursor_field} > {since}`` (AND-combined with any user
  ``filter_expression``), tracks the max cursor value seen, and returns
  ``end_offset = {"cursor": <max>}``.  When no newer rows exist (max cursor ==
  incoming cursor) it returns an empty iterator and the unchanged offset, so
  ``Trigger.AvailableNow`` terminates.  ``max_records_per_batch`` client-side
  truncation is safe because every row carries the ``_rowid`` primary key.
  ``cdc_with_deletes`` is **not** supported — LanceDB exposes no delete/change
  feed.
* ``append`` — insert-only, keyed on the monotonic ``_rowid``.  The framework
  runs append as a streaming source (whose contract requires the end offset to
  advance on every non-empty batch), so each microbatch emits only rows with
  ``_rowid`` greater than the last checkpoint and returns
  ``end_offset = {"rowid": <max>}``; it terminates (empty batch, unchanged
  offset) once no higher ids remain.  No primary key (framework appends).
  Suitable for immutable / log tables.

Primary key
-----------
LanceDB declares no natural unique key, so every ``snapshot`` / ``cdc`` query
sets ``with_row_id: true`` and the connector uses LanceDB's guaranteed-unique
``_rowid`` system column (int64 → ``LongType``) as the default merge key — or a
user-supplied ``primary_keys`` table option when provided.  ``_rowid`` is always
present in both the schema and the returned rows — even when a ``columns``
projection is set, it is kept in addition to the projected columns and survives
client-side projection.  ``append`` tables have no primary key.

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
from pyspark.sql.types import LongType, StructField, StructType
from requests.exceptions import RequestException

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.lancedb.lancedb_schemas import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_MAX_RECORDS,
    DEFAULT_TIMEOUT,
    INITIAL_BACKOFF,
    LIST_TABLES_LIMIT,
    MAX_BATCH_SIZE,
    MAX_RETRIES,
    RETRIABLE_STATUS_CODES,
    ROW_ID_COLUMN,
    TABLE_SCHEMAS,
    TABLES,
    VALID_INGESTION_TYPES,
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

        ``_rowid`` (``LongType``) — LanceDB's guaranteed-unique row id, used as
        the snapshot merge key — is always included: it is appended when absent
        and is retained *in addition to* the projected columns when a ``columns``
        projection filters the schema, so the merge key is never dropped.
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
        return self._with_row_id_field(schema)

    @staticmethod
    def _with_row_id_field(schema: StructType) -> StructType:
        """Return ``schema`` guaranteed to include the ``_rowid`` merge key.

        Appends a ``LongType`` ``_rowid`` field when absent (e.g. after a
        ``columns`` projection, or when ``describe`` did not report it).
        """
        if any(field.name == ROW_ID_COLUMN for field in schema.fields):
            return schema
        return StructType(
            list(schema.fields)
            + [StructField(ROW_ID_COLUMN, LongType(), nullable=False)]
        )

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
        """Report ingestion metadata for the mode selected by ``ingestion_type``.

        The optional ``ingestion_type`` table option selects the mode
        (default ``snapshot``):

        * ``snapshot`` — ``primary_keys`` (``_rowid`` unless overridden by a
          ``primary_keys`` option), ``cursor_field: None``.
        * ``cdc`` — same ``primary_keys``, plus a ``cursor_field`` (a required
          table option naming an orderable column).  Raises ``ValueError`` when
          ``cursor_field`` is missing.
        * ``append`` — ``primary_keys: []``, ``cursor_field: None`` (the
          framework appends; no key, no cursor).

        The primary key defaults to LanceDB's guaranteed-unique ``_rowid`` system
        column (requested via ``with_row_id`` on every query and always present
        in the schema); a ``primary_keys`` table option overrides it.

        Rejects malformed table names up front (the same identifier check the
        schema / query paths apply) so an unknown / injection-shaped name raises
        rather than silently returning metadata.
        """
        self._sanitize_identifier(table_name)
        ingestion_type = self._ingestion_type_option(table_options)

        if ingestion_type == "append":
            return {
                "primary_keys": [],
                "cursor_field": None,
                "ingestion_type": "append",
            }

        primary_keys = self._primary_keys_option(table_options) or [ROW_ID_COLUMN]

        if ingestion_type == "cdc":
            cursor_field = self._cursor_field_option(table_options)
            if not cursor_field:
                raise ValueError(
                    "LanceDB 'ingestion_type=cdc' requires a 'cursor_field' table "
                    "option naming an orderable column (e.g. a timestamp or "
                    "monotonic id)."
                )
            return {
                "primary_keys": primary_keys,
                "cursor_field": cursor_field,
                "ingestion_type": "cdc",
            }

        return {
            "primary_keys": primary_keys,
            "cursor_field": None,
            "ingestion_type": "snapshot",
        }

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read one batch, routed by the ``ingestion_type`` table option.

        * ``snapshot`` — full-table scan returning an empty offset ``{}``; the
          whole table is re-read on every run.
        * ``append`` — insert-only microbatch keyed on ``_rowid`` (see
          :meth:`_read_append`); the offset advances as required by the
          streaming source contract.
        * ``cdc`` — one incremental microbatch (see :meth:`_read_cdc`).
        """
        ingestion_type = self._ingestion_type_option(table_options)
        if ingestion_type == "cdc":
            return self._read_cdc(table_name, start_offset, table_options)
        if ingestion_type == "append":
            return self._read_append(table_name, start_offset, table_options)

        records = list(self._scan(table_name, table_options))
        return iter(records), {}

    def _read_append(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Insert-only append keyed on the monotonic ``_rowid``.

        The framework runs ``append`` as a streaming source, whose contract
        requires the end offset to advance whenever a non-empty batch is
        returned (``SIMPLE_STREAM_READER_OFFSET_DID_NOT_ADVANCE`` otherwise).
        LanceDB's ``_rowid`` increases for newly-inserted rows, so it doubles as
        that offset: each microbatch scans the table, keeps only rows with
        ``_rowid`` greater than the last checkpoint, and records the new maximum.
        Returns an empty batch with the offset unchanged once no higher ids
        remain, so the stream terminates.
        """
        since = start_offset.get("rowid") if start_offset else None
        records = [
            row
            for row in self._scan(table_name, table_options)
            if since is None or row.get(ROW_ID_COLUMN, -1) > since
        ]
        if not records:
            return iter([]), start_offset or {}
        max_rowid = max(row.get(ROW_ID_COLUMN, 0) for row in records)
        return iter(records), {"rowid": max_rowid}

    def _read_cdc(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Incremental (cdc) microbatch.

        Queries ``{cursor_field} > {since}`` AND-combined with any user
        ``filter_expression``, accumulates up to ``max_records_per_batch`` rows,
        orders them by the cursor, and advances the offset to the max cursor
        value seen.  Client-side truncation is safe because every row carries the
        ``_rowid`` (or user-supplied) primary key, so a re-fetched overlap is
        de-duplicated by the framework's merge/upsert.

        Terminates (returns an empty iterator + the unchanged offset) when no
        rows newer than the incoming cursor are found, satisfying
        ``Trigger.AvailableNow``'s ``end_offset == start_offset`` contract.
        """
        cursor_field = self._cursor_field_option(table_options)
        if not cursor_field:
            raise ValueError(
                "LanceDB 'ingestion_type=cdc' requires a 'cursor_field' table option."
            )
        since = start_offset.get("cursor") if start_offset else None
        combined = self._combined_filter(
            cursor_field, since, table_options.get("filter_expression")
        )
        max_records = self._max_records_option(table_options)

        records: list[dict] = []
        for row in self._scan(table_name, table_options, filter_expr=combined):
            records.append(row)
            if len(records) >= max_records:
                break

        # Order by cursor ascending so truncation keeps the earliest rows and
        # the offset advances monotonically.  The simulator ignores the query
        # body's ordering, so realise it client-side; against the live API this
        # is a cheap no-op on an already-ordered page.
        records = self._sort_by_cursor(records, cursor_field)[:max_records]

        if not records:
            return iter([]), start_offset or {}

        max_cursor = self._max_cursor(records, cursor_field, since)
        end_offset = {"cursor": max_cursor}
        # No rows newer than the incoming cursor → converged.
        if since is not None and max_cursor == since:
            return iter([]), start_offset
        if start_offset and start_offset == end_offset:
            return iter([]), start_offset
        return iter(records), end_offset

    # ----- query / scan ---------------------------------------------------

    def _scan(
        self,
        table_name: str,
        table_options: dict[str, str],
        filter_expr: str | None = None,
    ) -> Iterator[dict]:
        """Offset-paginate a full-table scan via ``POST .../query/``.

        Uses an all-zero dummy vector (sized to the embedding dimension) with
        ``bypass_vector_index`` so the query behaves as a plain scan rather than
        a similarity search.  Always sets ``with_row_id`` so LanceDB returns the
        guaranteed-unique ``_rowid`` column used as the default merge key.  Stops
        when a short page is returned.

        ``filter_expr`` overrides the server-side filter.  When ``None`` (the
        ``snapshot`` / ``append`` paths) the user ``filter_expression`` option is
        used as-is; the ``cdc`` path passes a pre-combined cursor filter.
        """
        safe = self._sanitize_identifier(table_name)
        endpoint = f"/v1/table/{quote(safe)}/query/"
        dim = self._get_vector_dimension(table_name)
        dummy_vector = [0.0] * dim if dim else [0.0]
        columns = self._columns_option(table_options)
        batch_size = self._batch_size_option(table_options)
        if filter_expr is None:
            filter_expr = table_options.get("filter_expression")

        offset = 0
        while True:
            payload: dict = {
                "vector": {"single_vector": dummy_vector},
                "k": batch_size,
                "bypass_vector_index": True,
                "with_row_id": True,
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
        """Re-filter rows to the requested columns, always keeping ``_rowid``.

        Some LanceDB API versions ignore server-side ``columns`` projection and
        return all columns; this makes the client honour the request regardless
        (a no-op when the server already projected).  ``_rowid`` (the snapshot
        merge key, requested via ``with_row_id``) is retained in addition to the
        projected columns so projection never drops it.
        """
        if not columns:
            return rows
        wanted = set(columns) | {ROW_ID_COLUMN}
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

    def _columns_option(self, table_options: dict[str, str]) -> list[str] | None:
        """Parse the ``columns`` projection option (JSON array, or a single name)."""
        return self._parse_name_list(table_options.get("columns"), "columns")

    def _primary_keys_option(self, table_options: dict[str, str]) -> list[str] | None:
        """Parse the optional ``primary_keys`` override (JSON array, or a name).

        When set, it replaces the default ``_rowid`` merge key for
        ``snapshot`` / ``cdc`` tables.
        """
        return self._parse_name_list(
            table_options.get("primary_keys"), "primary_keys"
        )

    @staticmethod
    def _cursor_field_option(table_options: dict[str, str]) -> str | None:
        """Return the validated ``cursor_field`` option, or ``None`` if unset."""
        raw = table_options.get("cursor_field")
        if raw is None or raw == "":
            return None
        return LancedbLakeflowConnect._validate_name(raw, "cursor_field")

    @staticmethod
    def _ingestion_type_option(table_options: dict[str, str]) -> str:
        """Return the ``ingestion_type`` option (default ``snapshot``).

        Raises ``ValueError`` for values outside ``snapshot`` / ``cdc`` /
        ``append``.
        """
        raw = (table_options.get("ingestion_type") or "snapshot").strip().lower()
        if raw not in VALID_INGESTION_TYPES:
            raise ValueError(
                f"Invalid 'ingestion_type': {raw!r}. Must be one of "
                f"{list(VALID_INGESTION_TYPES)}."
            )
        return raw

    def _max_records_option(self, table_options: dict[str, str]) -> int:
        raw = table_options.get("max_records_per_batch", str(DEFAULT_MAX_RECORDS))
        try:
            value = int(raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "LanceDB option 'max_records_per_batch' must be an integer; "
                f"got {raw!r}"
            ) from exc
        return max(1, value)

    @staticmethod
    def _parse_name_list(raw, option_name: str) -> list[str] | None:
        """Parse a name-list option (JSON array, list, or a single name).

        Validates each name (alphanumeric, underscore, dot) to prevent injection
        into the query.  Returns ``None`` when unset or empty.
        """
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

        names = [
            LancedbLakeflowConnect._validate_name(name, option_name)
            for name in value
        ]
        return names or None

    @staticmethod
    def _validate_name(name, option_name: str) -> str:
        """Validate a single column/field name for safe query interpolation."""
        if not isinstance(name, str):
            raise ValueError(
                f"LanceDB option {option_name!r} must contain column name strings"
            )
        if not name.replace("_", "").replace(".", "").isalnum():
            raise ValueError(
                f"Invalid name in {option_name!r}: {name!r}. Only alphanumeric, "
                "underscore, and dot are allowed."
            )
        return name

    # ----- cursor helpers -------------------------------------------------

    @staticmethod
    def _combined_filter(
        cursor_field: str, since, user_filter: str | None
    ) -> str | None:
        """AND-combine the ``cursor_field > since`` clause with a user filter.

        Returns ``None`` when neither applies (the first cdc call with no user
        filter), so the initial microbatch scans from the beginning.
        """
        clauses: list[str] = []
        if since is not None:
            formatted = LancedbLakeflowConnect._format_cursor_value(since)
            clauses.append(f"{cursor_field} > {formatted}")
        if user_filter:
            clauses.append(f"({user_filter})")
        return " AND ".join(clauses) if clauses else None

    @staticmethod
    def _format_cursor_value(value) -> str:
        """Format a cursor value for a Lance SQL filter.

        Numeric cursors are emitted bare; everything else (timestamps, ids) is
        single-quoted with embedded quotes escaped.
        """
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (int, float)):
            return str(value)
        escaped = str(value).replace("'", "''")
        return f"'{escaped}'"

    @staticmethod
    def _max_cursor(records: list[dict], cursor_field: str, current):
        """Return the max cursor value across ``records``, seeded with ``current``."""
        max_val = current
        for record in records:
            val = record.get(cursor_field)
            if val is None:
                continue
            if max_val is None or val > max_val:
                max_val = val
        return max_val

    @staticmethod
    def _sort_by_cursor(records: list[dict], cursor_field: str) -> list[dict]:
        """Sort records ascending by cursor, placing null cursors last.

        Nulls are partitioned out first so they never enter the ``<`` comparison
        (comparing ``None < None`` would raise).
        """
        with_cursor = [r for r in records if r.get(cursor_field) is not None]
        without_cursor = [r for r in records if r.get(cursor_field) is None]
        with_cursor.sort(key=lambda r: r.get(cursor_field))
        return with_cursor + without_cursor

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
