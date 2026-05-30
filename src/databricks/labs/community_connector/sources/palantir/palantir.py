"""
Palantir Foundry Connector for Lakeflow Community Connectors.

This connector enables ingestion of data from Palantir Foundry ontologies
into Databricks using the LakeflowConnect interface.
"""

import logging
import random
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional, Tuple

import requests
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    BooleanType,
    ArrayType,
    DecimalType,
    DataType,
)

from databricks.labs.community_connector.interface.lakeflow_connect import LakeflowConnect

logger = logging.getLogger(__name__)


class PalantirLakeflowConnect(LakeflowConnect):
    """
    Palantir Foundry connector implementing the LakeflowConnect interface.

    Supports:
    - Dynamic schema discovery from Palantir object type definitions
    - Both snapshot and incremental (CDC) ingestion modes
    - Cursor-based pagination using nextPageToken
    - User-configurable cursor fields for incremental sync
    """

    def __init__(self, options: Dict[str, str]) -> None:
        """
        Initialize Palantir Foundry connector with authentication.

        Args:
            options: Connection parameters including:
                - token: Bearer token for API authentication (required)
                - hostname: Palantir Foundry hostname (required). Bare form
                  (yourcompany.palantirfoundry.com) is preferred; full URLs
                  with https:// are also accepted — the connector normalises both.
                - ontology_api_name: Target ontology API identifier (required)

        Raises:
            ValueError: If required parameters are missing
        """
        super().__init__(options)
        # Extract and validate required parameters
        self.token = options.get("token")
        if not self.token:
            raise ValueError("Missing required parameter 'token'")

        self.hostname = (options.get("hostname") or "").strip()
        if not self.hostname:
            raise ValueError("Missing required parameter 'hostname'")

        self.ontology_api_name = options.get("ontology_api_name")
        if not self.ontology_api_name:
            raise ValueError("Missing required parameter 'ontology_api_name'")

        # Normalize hostname into a base URL. Accepts either a bare
        # hostname (the documented form) or a full URL with scheme —
        # mirrors the pattern in sources/osipi/osipi_http.py:64-86 so
        # the user's "I'll just paste my browser URL" mental model
        # doesn't fail silently with double-scheme strings like
        # ``https://https://yourcompany.palantirfoundry.com``.
        # Preserves a user-supplied scheme; otherwise prepends https://.
        if self.hostname.startswith(("http://", "https://")):
            self.base_url = self.hostname.rstrip("/")
        else:
            self.base_url = f"https://{self.hostname}".rstrip("/")

        # Configure authenticated session
        self._session = requests.Session()
        self._session.headers.update({
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        })

        # Admission control: cap cursor at init time so a single trigger
        # run only drains data that existed when the connector started.
        # Store both the datetime (used in the cap loop — pre-parsed so
        # we don't re-parse the string once per microbatch) and the ISO
        # string (kept for backward-compatible introspection / logs).
        self._init_dt = datetime.now(timezone.utc)
        self._init_time = self._init_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        self._default_max_records_per_batch = 100_000

        # Initialize caches for performance
        self._schema_cache: Dict[str, StructType] = {}
        self._metadata_cache: Dict[str, dict] = {}
        self._object_types_cache: Optional[Dict[str, dict]] = None

    def close(self) -> None:
        """Release the underlying ``requests.Session`` connection pool.

        Spark's data-source lifecycle can construct many connector
        instances per query; each holds an open keep-alive pool that
        leaks sockets on long-running pipelines. Callers should
        invoke ``close()`` when done. Idempotent.
        """
        session = getattr(self, "_session", None)
        if session is not None:
            session.close()
            self._session = None

    def _ensure_object_types_cached(self) -> None:
        """
        Fetch all object type definitions from the Palantir API and cache
        them. Called lazily on first access. Subsequent calls use the cache.

        Uses the list endpoint (GET /objectTypes) which returns full
        definitions including properties and primaryKey for each type.
        """
        if self._object_types_cache is not None:
            return

        url = f"{self.base_url}/api/v2/ontologies/{self.ontology_api_name}/objectTypes"

        try:
            response = self._session.get(url, timeout=30)
            response.raise_for_status()
        except requests.RequestException as e:
            raise RuntimeError(
                f"Failed to list object types from Palantir ontology "
                f"'{self.ontology_api_name}': {e}"
            ) from e

        data = response.json()
        self._object_types_cache = {
            obj["apiName"]: obj for obj in data.get("data", [])
        }

    def list_tables(self) -> List[str]:
        """
        List all object types (tables) available in the configured ontology.

        Returns:
            List of object type API names (e.g., ["FlightsFinal", "ExampleRouteAlert"])
        """
        self._ensure_object_types_cached()
        return list(self._object_types_cache.keys())

    def _get_object_type(self, table_name: str) -> dict:
        """Get the cached object type definition for a table."""
        self._ensure_object_types_cached()
        if table_name not in self._object_types_cache:
            raise ValueError(
                f"Table '{table_name}' is not supported. "
                f"Available tables: {self.list_tables()}"
            )
        return self._object_types_cache[table_name]

    def get_table_schema(
        self, table_name: str, table_options: Dict[str, str]
    ) -> StructType:
        """
        Dynamically discover and return the Spark schema for a Palantir object type.

        Args:
            table_name: Object type API name (e.g., "FlightsFinal")
            table_options: Additional options (not used for schema discovery)

        Returns:
            Spark StructType schema

        Raises:
            ValueError: If table_name is not supported
        """
        if table_name in self._schema_cache:
            return self._schema_cache[table_name]

        obj_type = self._get_object_type(table_name)
        schema = self._build_schema_from_object_type(obj_type)

        self._schema_cache[table_name] = schema
        return schema

    def _build_schema_from_object_type(self, obj_type: dict) -> StructType:
        """
        Build Spark StructType schema from Palantir object type definition.

        Args:
            obj_type: Object type definition from Palantir API

        Returns:
            Spark StructType schema
        """
        fields: List[StructField] = []

        # Collect primary key field names to avoid duplicates
        primary_key = obj_type.get("primaryKey")
        if isinstance(primary_key, str):
            pk_fields = {primary_key}
        elif isinstance(primary_key, list):
            pk_fields = set(primary_key)
        else:
            pk_fields = set()

        # Add properties from object type definition
        properties = obj_type.get("properties", {})
        for prop_name, prop_def in properties.items():
            data_type_def = prop_def.get("dataType", {})
            spark_type = self._map_palantir_type_to_spark(data_type_def)
            # Primary key fields are non-nullable
            nullable = prop_name not in pk_fields
            fields.append(StructField(prop_name, spark_type, nullable))

        # If primary key is not in properties, add it explicitly
        for pk in pk_fields:
            if pk not in properties:
                fields.insert(0, StructField(pk, StringType(), False))

        return StructType(fields)

    def _map_palantir_type_to_spark(self, palantir_type: dict) -> DataType:
        """
        Map Palantir data type to Spark DataType recursively.

        Args:
            palantir_type: Palantir data type definition

        Returns:
            Corresponding Spark DataType
        """
        type_name = palantir_type.get("type", "string")

        # Basic type mapping
        type_mapping = {
            "string": StringType(),
            "integer": LongType(),
            "long": LongType(),
            "double": DoubleType(),
            "float": DoubleType(),
            "boolean": BooleanType(),
            "timestamp": StringType(),  # Keep as ISO 8601 string
            "date": StringType(),  # Keep as ISO date string
            "datetime": StringType(),
            "attachment": StringType(),  # JSON representation
        }

        # Decimal carries explicit precision/scale on the property
        # definition. Use Spark's DecimalType for exact arithmetic
        # rather than coercing to DoubleType (which silently loses
        # precision past 15-16 digits — unsafe for finance /
        # measurement data). Spark caps precision at 38; clamp
        # defensively in case Palantir reports a wider type, and fall
        # back to (38, 18) — the conventional "wide" default — when
        # Palantir omits the fields.
        if type_name == "decimal":
            precision = palantir_type.get("precision") or 38
            scale = palantir_type.get("scale") or 18
            precision = min(int(precision), 38)
            scale = min(int(scale), precision)
            return DecimalType(precision, scale)

        # Handle special/complex types
        if type_name == "geopoint":
            # Geopoint becomes struct with coordinates
            return StructType([
                StructField("latitude", DoubleType(), True),
                StructField("longitude", DoubleType(), True),
            ])

        if type_name == "array":
            # Recursive array type
            item_type = palantir_type.get("itemType", {"type": "string"})
            element_type = self._map_palantir_type_to_spark(item_type)
            return ArrayType(element_type, True)

        if type_name == "struct":
            # Recursive struct type
            sub_properties = palantir_type.get("properties", {})
            sub_fields = []
            for sub_prop_name, sub_prop_def in sub_properties.items():
                sub_data_type = sub_prop_def.get("dataType", {})
                sub_spark_type = self._map_palantir_type_to_spark(sub_data_type)
                sub_fields.append(StructField(sub_prop_name, sub_spark_type, True))
            return StructType(sub_fields)

        # Default to mapped type or string
        return type_mapping.get(type_name, StringType())

    def read_table_metadata(
        self, table_name: str, table_options: Dict[str, str]
    ) -> dict:
        """
        Return metadata for the specified table.

        Args:
            table_name: Object type API name
            table_options: Options including optional 'cursor_field' for incremental sync

        Returns:
            Metadata dict with primary_keys, cursor_field (optional), and ingestion_type

        Raises:
            ValueError: If table_name is not supported
        """
        cursor_field = table_options.get("cursor_field")
        cache_key = f"{table_name}_{cursor_field or 'snapshot'}"

        if cache_key in self._metadata_cache:
            return self._metadata_cache[cache_key]

        obj_type = self._get_object_type(table_name)
        primary_key = obj_type.get("primaryKey")

        # Determine primary key field(s)
        if isinstance(primary_key, str):
            pk_fields = [primary_key]
        elif isinstance(primary_key, list):
            pk_fields = primary_key
        else:
            pk_fields = ["__primaryKey"]

        # Build metadata based on whether cursor field is specified
        if cursor_field:
            # Incremental (CDC) mode
            metadata = {
                "primary_keys": pk_fields,
                "cursor_field": cursor_field,
                "ingestion_type": "cdc",
            }
        else:
            # Snapshot mode (full refresh)
            metadata = {
                "primary_keys": pk_fields,
                "ingestion_type": "snapshot",
            }

        # Cache the result
        self._metadata_cache[cache_key] = metadata
        return metadata

    def read_table(
        self,
        table_name: str,
        start_offset: dict,
        table_options: Dict[str, str],
    ) -> Tuple[Iterator[dict], dict]:
        """
        Read data from the specified Palantir object type.

        Args:
            table_name: Object type API name
            start_offset: Starting offset for pagination (None for first call)
            table_options: Options including 'cursor_field' and 'page_size'

        Returns:
            Tuple of (iterator of records, end offset dict)

        Raises:
            ValueError: If table_name is not supported
            Exception: If API request fails
        """
        # Get metadata to determine ingestion mode
        metadata = self.read_table_metadata(table_name, table_options)
        cursor_field = metadata.get("cursor_field")

        if cursor_field is not None:
            return self._read_incremental(
                table_name, start_offset, table_options, cursor_field
            )
        else:
            return self._read_snapshot(table_name, start_offset, table_options)

    def _generate_all_pages(
        self, table_name: str, page_size: int,
        where_clause: dict = None,
        order_by_field: str = None,
    ) -> Iterator[dict]:
        """
        Generator that yields records page by page, keeping only one page
        in memory at a time. This avoids OOM when reading large datasets.

        Args:
            order_by_field: Optional field to sort records by (ascending).
                Required for Strategy B incremental reads so the
                connector can advance the offset to the last-emitted
                record's cursor without skipping unread records.
        """
        object_set = self._build_object_set(table_name, where_clause)
        page_token = None
        while True:
            records, next_page_token = self._fetch_page(
                object_set, page_token, page_size, order_by_field
            )
            yield from records

            if not next_page_token:
                break
            page_token = next_page_token

    def _read_snapshot(
        self, table_name: str, start_offset: dict, table_options: Dict[str, str]
    ) -> Tuple[Iterator[dict], dict]:
        """
        Read all data in snapshot mode using a streaming generator.

        Yields records page by page to avoid loading the entire dataset
        into memory. Only one page (~10K records) is held at a time.
        Streams every record from the source on a single
        framework-driven pass (``apply_changes_from_snapshot``) —
        ``max_records_per_batch`` is intentionally not honoured here
        because snapshot mode has no resume mechanism (offset is
        always ``{}``); a cap would silently drop records past it.
        That option remains effective in incremental mode where the
        ``last_emitted_cursor`` offset enables resume.

        Args:
            table_name: Object type API name
            start_offset: Starting offset (unused for snapshot full refresh)
            table_options: Options including optional 'page_size'

        Returns:
            Tuple of (generator of all records, end offset dict)
        """
        page_size = int(table_options.get("page_size", "1000"))

        # ``max_records_per_batch`` applies to incremental mode only,
        # where the next call can resume via ``where: gt
        # last_emitted_cursor``. Snapshot mode runs in a single
        # framework-driven pass (``apply_changes_from_snapshot``) with
        # no mid-snapshot checkpoint, so capping here would silently
        # drop records past the cap — a data-loss bug.
        return self._generate_all_pages(
            table_name, page_size,
        ), {}

    @staticmethod
    def _to_utc_datetime(value: Any) -> Any:
        """Parse an ISO 8601 string into a tz-aware UTC ``datetime``.

        Handles the formats Palantir is known to return — the ``Z``
        suffix, explicit offsets like ``+00:00`` / ``-05:00``,
        date-only strings, and optional fractional seconds — by
        normalising to UTC. Naive strings (no tz) are assumed UTC.
        Returns ``None`` for anything that does not parse, so the
        cap logic can skip non-timestamp cursors silently.

        Used instead of lexicographic comparison because string
        ordering breaks across format/tz differences (e.g.
        ``"...-05:00"`` < ``"...Z"`` lexically even when the actual
        moments compare the other way).
        """
        if not isinstance(value, str):
            return None
        normalised = (
            value[:-1] + "+00:00" if value.endswith("Z") else value
        )
        try:
            parsed = datetime.fromisoformat(normalised)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    @staticmethod
    def _parse_retry_after(header_value: Optional[str]) -> Optional[float]:
        """Parse the ``Retry-After`` HTTP header into seconds.

        Only handles the integer-seconds form (RFC 7231) — the
        HTTP-date form is rare in API responses and would add date
        parsing for negligible benefit. Returns ``None`` for a
        missing, empty, or unparseable value so the caller falls
        back to exponential backoff.
        """
        if not header_value:
            return None
        try:
            seconds = float(header_value.strip())
        except (TypeError, ValueError):
            return None
        if seconds < 0:
            return None
        return seconds

    def _cursor_strictly_greater(
        self, new_value: Any, prev_value: Any
    ) -> bool:
        """Return ``True`` iff ``new_value > prev_value``.

        Used by the early-exit short-circuit in ``_read_incremental``.
        Prefers a datetime-aware compare (``_to_utc_datetime`` handles
        every ISO 8601 form Palantir is known to return) so values
        with different tz suffixes don't compare lexicographically.
        Falls back to direct ``>`` for non-timestamp cursor types
        (numeric IDs, UUIDs, etc.). On ``TypeError`` (e.g. comparing
        an ``int`` to a ``str``) returns ``True`` so we default to
        ``do the read`` — the safe choice for a quirky cursor type,
        because skipping incorrectly would silently drop records.
        """
        new_dt = self._to_utc_datetime(new_value)
        prev_dt = self._to_utc_datetime(prev_value)
        if new_dt is not None and prev_dt is not None:
            return new_dt > prev_dt
        try:
            return new_value > prev_value
        except TypeError:
            return True

    def _get_max_cursor_value(
        self, table_name: str, cursor_field: str
    ) -> Any:
        """Get the maximum cursor field value via the search endpoint.

        Earlier versions of this connector tried the ``aggregate``
        endpoint first as a "lightweight max-cursor lookup" and fell
        back to ``search``. In practice Palantir returns 500 for
        aggregate against the ontology object types we use, so the
        fallback path was the only one that ever produced a result.
        We removed the aggregate code path to skip the doomed HTTP
        round-trip on every CDC tick. ``search`` (``orderBy desc,
        limit 1``) works universally and the payload-size difference
        vs aggregate is negligible (one record vs one metric).
        """
        return self._get_max_cursor_via_search(table_name, cursor_field)

    def _get_max_cursor_via_search(
        self, table_name: str, cursor_field: str
    ) -> Any:
        """Get max cursor value using ``orderBy desc, limit 1``.

        Returns ``None`` if the call fails (logged at ``warning``) or
        the response has no records. Caller treats ``None`` as ``no
        new data`` and the offset-unchanged termination check kicks in.
        """
        url = (
            f"{self.base_url}/api/v2/ontologies/{self.ontology_api_name}"
            f"/objects/{table_name}/search"
        )
        body = {
            "orderBy": {
                "fields": [{"field": cursor_field, "direction": "desc"}]
            },
            "pageSize": 1,
        }
        try:
            response = self._session.post(url, json=body, timeout=30)
            response.raise_for_status()
        except requests.RequestException as e:
            logger.warning(
                "search failed for %s.%s: %s; "
                "cursor will not advance this tick",
                table_name,
                cursor_field,
                e,
            )
            return None

        data = response.json()
        records = data.get("data", [])
        if records:
            return records[0].get(cursor_field)
        return None

    def _read_incremental(
        self,
        table_name: str,
        start_offset: dict,
        table_options: Dict[str, str],
        cursor_field: str,
    ) -> Tuple[Iterator[dict], dict]:
        """
        Read data in incremental (CDC) mode — Strategy B (server-side
        limit with last-emitted-cursor offset).

        Flow:
        0. Early-exit short-circuit: on subsequent runs (when an
           offset is supplied), peek at the dataset's current max
           cursor via a single ``search orderBy desc, limit 1`` call.
           If it hasn't advanced past ``prev_max_cursor``, return an
           empty iterator with the same offset and skip the
           ``loadObjects`` round-trip entirely. Falls through to the
           full read path on lookup failure (``None`` from helper).
        1. Build a server-side ``where: cursor > prev_max_cursor`` filter
           so the API only returns records newer than the checkpoint.
        2. Request records sorted by ``cursor_field`` ascending. Without
           this the "first N records" returned by the API are in
           arbitrary order and the offset can't safely advance to a
           sub-range of the dataset.
        3. Eagerly drain up to ``max_records_per_batch`` records into
           memory, dropping anything with a null cursor and anything
           past ``_init_time`` (the latter is deferred to the next
           trigger run so this batch terminates).
        4. Set the offset to the cursor of the LAST emitted record —
           not the dataset max. If the admission cap was hit, the next
           microbatch picks up via ``where: gt last_emitted`` and
           drains the remainder. No records are skipped, regardless of
           how the dataset compares to the cap.
        5. If no records were emitted, return an empty iterator with
           the same offset so Spark Streaming terminates the microbatch.

        Works with both SCD_TYPE_1 and SCD_TYPE_2 — the SCD type is
        handled by the framework's apply_changes, not by this method.

        Args:
            table_name: Object type API name.
            start_offset: Offset with optional 'max_cursor_value'.
            table_options: Options including optional 'page_size'
                and 'max_records_per_batch'.
            cursor_field: Field name to use for cursor tracking.

        Returns:
            Tuple of (records iterator, end offset dict).
        """
        page_size = int(table_options.get("page_size", "1000"))
        max_records = int(
            table_options.get(
                "max_records_per_batch", self._default_max_records_per_batch
            )
        )
        prev_max_cursor = start_offset.get("max_cursor_value") if start_offset else None

        # Early-exit short-circuit: peek at the current dataset max
        # cursor via a single ``search orderBy desc, limit 1`` call
        # and bail out if nothing has advanced past our checkpoint.
        # Skips the (paginated) ``loadObjects`` round-trip on no-op
        # polls — the common case for incremental triggers. Mirrors
        # the example connector's ``since >= self._init_ts`` skip.
        #
        # Skipped on first run (``prev_max_cursor is None``) since
        # we must do a full load. On lookup failure (search returned
        # 4xx/5xx and the helper returned ``None``) we fall through
        # to the ``where: gt`` path so the read still works for
        # tables where search is unsupported or transiently failing.
        if prev_max_cursor is not None:
            new_max_cursor = self._get_max_cursor_value(
                table_name, cursor_field
            )
            if (
                new_max_cursor is not None
                and not self._cursor_strictly_greater(
                    new_max_cursor, prev_max_cursor
                )
            ):
                return iter([]), start_offset

        # Build server-side where clause for incremental runs.
        # On first run (prev_max_cursor is None): no filter → full load.
        # On subsequent runs: gt filter → only new/updated records.
        where_clause = None
        if prev_max_cursor is not None:
            where_clause = {
                "type": "gt",
                "field": cursor_field,
                "value": prev_max_cursor,
            }

        # Eagerly materialise records up to max_records, in cursor
        # ASC order. Tracking the last-emitted cursor is what makes
        # the cap correct — the next microbatch picks up from there.
        # Memory bound is max_records × record_size; the user controls
        # the cap via ``max_records_per_batch``.
        init_dt = self._init_dt
        records: List[dict] = []
        last_emitted_cursor: Any = None
        for record in self._generate_all_pages(
            table_name,
            page_size,
            where_clause=where_clause,
            order_by_field=cursor_field,
        ):
            if max_records and len(records) >= max_records:
                break
            cursor_value = record.get(cursor_field)
            if cursor_value is None:
                continue
            # _init_dt cap: defer records past wall-clock start to
            # the next trigger run so this microbatch terminates.
            # Records are sorted ASC by cursor, so the first
            # past-init record marks the end of this slice.
            cursor_dt = self._to_utc_datetime(cursor_value)
            if (
                cursor_dt is not None
                and init_dt is not None
                and cursor_dt > init_dt
            ):
                break
            records.append(record)
            last_emitted_cursor = cursor_value

        # No records emitted → terminate the microbatch with the
        # same offset so Spark sees "no progress" and stops.
        if not records:
            return iter([]), start_offset if start_offset else {
                "max_cursor_value": prev_max_cursor
            }

        return iter(records), {"max_cursor_value": last_emitted_cursor}

    def _build_object_set(
        self, table_name: str, where_clause: dict = None
    ) -> dict:
        """
        Build an objectSet definition for the loadObjects endpoint.

        Args:
            table_name: Object type API name
            where_clause: Optional filter (e.g.,
                {"type": "gt", "field": "cursor", "value": "2024-01-01"})

        Returns:
            objectSet dict for use in loadObjects request body
        """
        base = {"type": "base", "objectType": table_name}
        if where_clause:
            return {
                "type": "filter",
                "objectSet": base,
                "where": where_clause,
            }
        return base

    def _fetch_page(
        self,
        object_set: dict,
        page_token: str = None,
        page_size: int = 1000,
        order_by_field: str = None,
    ) -> Tuple[List[dict], str]:
        """
        Fetch a single page of data from Palantir API using the
        loadObjectSet endpoint with snapshot=true for consistent
        pagination across large datasets.

        Uses exponential backoff on transient errors (429, 503).

        Args:
            object_set: objectSet definition (from _build_object_set)
            page_token: Optional pagination token from previous call
            page_size: Number of records to fetch (default 1000, max 10000)
            order_by_field: Optional field name to sort by ascending.
                Used for Strategy B incremental reads so the offset
                can be set to the cursor of the last emitted record.

        Returns:
            Tuple of (list of records, next page token or None)

        Raises:
            Exception: If API request fails after retries
        """
        url = (
            f"{self.base_url}/api/v2/ontologies/{self.ontology_api_name}"
            f"/objectSets/loadObjects?snapshot=true"
        )

        body = {
            "objectSet": object_set,
            "pageSize": min(page_size, 10000),
        }
        if page_token:
            body["pageToken"] = page_token
        if order_by_field:
            body["orderBy"] = {
                "fields": [{"field": order_by_field, "direction": "asc"}]
            }

        # Retry policy: only transient failures retry. Everything else
        # (4xx/5xx that isn't 429 or 503) propagates immediately —
        # otherwise a real auth/config error like 401 (expired token)
        # or 404 (wrong ontology name) would burn 1+2+4+8 = 15s of
        # sleep and 5 wasted load-balancer hits before the user sees
        # the actual error.
        max_retries = 5
        for attempt in range(max_retries):
            try:
                response = self._session.post(url, json=body, timeout=120)
            except (requests.ConnectionError, requests.Timeout) as e:
                if attempt < max_retries - 1:
                    # Jittered backoff — uniform random multiplier in
                    # [0.5, 1.5) keeps the mean at 2**attempt but
                    # decorrelates concurrent Spark tasks so a 429
                    # storm doesn't have every task retry in lockstep
                    # against Palantir's per-user rate cap.
                    time.sleep((2 ** attempt) * (0.5 + random.random()))
                    continue
                raise RuntimeError(
                    f"Failed to fetch data after {max_retries} retries "
                    f"(network error): {e}"
                ) from e

            if response.status_code in (429, 503):
                if attempt < max_retries - 1:
                    # Honour ``Retry-After`` when the server sets it;
                    # otherwise fall back to exponential backoff. In
                    # both cases the jitter multiplier is applied so
                    # concurrent tasks don't unblock in lockstep.
                    retry_after = self._parse_retry_after(
                        response.headers.get("Retry-After")
                    )
                    base_wait = (
                        retry_after if retry_after is not None
                        else (2 ** attempt)
                    )
                    time.sleep(base_wait * (0.5 + random.random()))
                    continue
                raise RuntimeError(
                    f"Failed to fetch data after {max_retries} retries: "
                    f"last status {response.status_code}"
                )

            # Non-transient 4xx/5xx — raise immediately, no retry.
            response.raise_for_status()

            data = response.json()
            records = data.get("data", [])
            next_page_token = data.get("nextPageToken")
            return records, next_page_token
