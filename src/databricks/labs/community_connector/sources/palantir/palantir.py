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
        # hostname (the documented form) or a full URL with the https://
        # scheme — mirrors the pattern in sources/osipi/osipi_http.py so
        # the user's "I'll just paste my browser URL" mental model
        # doesn't fail silently with double-scheme strings like
        # ``https://https://yourcompany.palantirfoundry.com``.
        #
        # Reject http:// outright: a plaintext base URL would send the
        # bearer token unencrypted, and Foundry is HTTPS-only (the README
        # and connector spec document https:// exclusively).
        if self.hostname.startswith("http://"):
            raise ValueError(
                "hostname must use https:// — an http:// base URL would "
                "transmit the bearer token unencrypted. Provide a bare "
                "host (e.g. 'yourcompany.palantirfoundry.com') or an "
                "https:// URL."
            )
        if self.hostname.startswith("https://"):
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
        # Max attempts for the bounded transient-retry loop, shared by
        # the schema-discovery GET and the data-path POST.
        self._max_retries = 5

        # Initialize caches for performance
        self._schema_cache: Dict[str, StructType] = {}
        self._metadata_cache: Dict[str, dict] = {}
        self._object_types_cache: Optional[Dict[str, dict]] = None

    def close(self) -> None:
        """Release the underlying ``requests.Session`` connection pool.

        Spark's data-source lifecycle can construct many connector
        instances per query; each holds an open keep-alive pool that
        leaks sockets on long-running pipelines. Idempotent. NOTE: the
        framework data-source lifecycle does not currently call
        ``close()`` (it is not on the ``LakeflowConnect`` base), so this
        is provided for explicit/manual use and future lifecycle hooks.
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

        # This GET is the first request of every trigger (the cache is
        # per-connector-instance and SDP builds a fresh instance per
        # trigger), so a transient 429/503 here would otherwise hard-fail
        # the whole microbatch. Apply the same bounded retry as the data
        # path (_fetch_page) — retry 429/503 + network errors, fail fast
        # on other 4xx/5xx. (F7)
        response = self._request_with_retry(
            "get",
            url,
            timeout=30,
            error_label=(
                f"to list object types from Palantir ontology "
                f"'{self.ontology_api_name}'"
            ),
        )

        # Non-transient 4xx/5xx — surface the status + Palantir error
        # body (errorCode/errorName) so misconfiguration is actionable.
        if response.status_code >= 400:
            raise RuntimeError(
                f"Failed to list object types from Palantir ontology "
                f"'{self.ontology_api_name}': status "
                f"{response.status_code}: {response.text}"
            )

        data = response.json()
        self._object_types_cache = {
            obj["apiName"]: obj
            for obj in data.get("data", [])
            if obj.get("apiName")
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

        # An object type with no properties and no declared primary key
        # would yield an empty StructType, which Spark later rejects with
        # an opaque error. Fail early with an actionable message instead.
        if not fields:
            api_name = obj_type.get("apiName", "<unknown>")
            raise ValueError(
                f"Object type '{api_name}' has no readable columns: it "
                f"declares no properties and no primaryKey, so a schema "
                f"cannot be built. Verify the object type definition and "
                f"that your token has read access to its properties."
            )

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

        # Known scalar types map directly. An unrecognised type (e.g. a
        # newer Palantir type like ``marking`` / ``mediaReference`` /
        # ``timeseries`` / ``cipherText``) falls back to StringType —
        # warn so a silently stringified structured column is at least
        # visible in logs rather than failing silently. (A missing type
        # defaults to ``"string"`` above and is not warned on.)
        if type_name not in type_mapping:
            logger.warning(
                "Unmapped Palantir data type %r — falling back to "
                "StringType; this column will be ingested as a string.",
                type_name,
            )
            return StringType()
        return type_mapping[type_name]

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
        where_clause: Optional[dict] = None,
        order_by_field: Optional[str] = None,
        secondary_order_field: Optional[str] = None,
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
                object_set, page_token, page_size,
                order_by_field, secondary_order_field,
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

    def _sleep_before_retry(
        self, attempt: int, response: Optional[requests.Response] = None
    ) -> None:
        """Sleep before retrying a transient failure.

        Honours ``Retry-After`` when the server set it (429/503),
        otherwise exponential backoff (``2 ** attempt``). A uniform
        jitter multiplier in ``[0.5, 1.5)`` keeps the mean at the base
        wait but decorrelates concurrent Spark tasks so a 429 storm
        doesn't have every task retry in lockstep against Palantir's
        per-user rate cap. Shared by ``_fetch_page`` and the
        schema-discovery GET so their retry policy stays identical.
        """
        base_wait = 2 ** attempt
        if response is not None:
            retry_after = self._parse_retry_after(
                response.headers.get("Retry-After")
            )
            if retry_after is not None:
                base_wait = retry_after
        wait = base_wait * (0.5 + random.random())
        # One breadcrumb per retry (transient errors only, so low-noise)
        # so a stalling 429/503 storm is visible in pipeline logs for
        # triage — the data path is otherwise silent (OPS-1).
        logger.warning(
            "Transient Palantir error; backing off %.1fs before retry "
            "(attempt %d).", wait, attempt + 1,
        )
        time.sleep(wait)

    def _request_with_retry(
        self, method: str, url: str, *, timeout: int,
        json_body: Optional[dict] = None, error_label: str,
    ) -> requests.Response:
        """Issue an HTTP request under the connector's bounded
        transient-retry policy and return the ``requests.Response``.

        Retries 429/503 and network errors (``ConnectionError``/
        ``Timeout``) with jittered backoff up to ``self._max_retries``,
        then returns the response for the caller to handle non-transient
        statuses and parse the body. Shared by the schema-discovery GET
        and the data-path POST so their retry behaviour stays identical.
        ``method`` is the ``requests.Session`` attribute name
        ("get"/"post") so test mocks on those attributes still intercept.
        """
        send = getattr(self._session, method)
        kwargs: dict = {"timeout": timeout}
        if json_body is not None:
            kwargs["json"] = json_body
        for attempt in range(self._max_retries):
            try:
                response = send(url, **kwargs)
            except (requests.ConnectionError, requests.Timeout) as e:
                if attempt < self._max_retries - 1:
                    self._sleep_before_retry(attempt)
                    continue
                raise RuntimeError(
                    f"Failed {error_label} after {self._max_retries} "
                    f"retries (network error): {e}"
                ) from e
            if response.status_code in (429, 503):
                if attempt < self._max_retries - 1:
                    self._sleep_before_retry(attempt, response)
                    continue
                raise RuntimeError(
                    f"Failed {error_label} after {self._max_retries} "
                    f"retries: last status {response.status_code}"
                )
            return response
        # Unreachable: the final iteration always returns or raises.
        raise RuntimeError(  # pragma: no cover
            f"Failed {error_label}: retry loop exited unexpectedly"
        )

    def _cursor_strictly_greater(
        self, new_value: Any, prev_value: Any,
        *, default_on_type_error: bool = True,
    ) -> bool:
        """Return ``True`` iff ``new_value > prev_value``.

        Used by the early-exit short-circuit in ``_read_incremental``.
        Prefers a datetime-aware compare (``_to_utc_datetime`` handles
        every ISO 8601 form Palantir is known to return) so values
        with different tz suffixes don't compare lexicographically.
        Falls back to direct ``>`` for non-timestamp cursor types
        (numeric IDs, UUIDs, etc.). On ``TypeError`` (e.g. comparing an
        ``int`` to a ``str``) returns ``default_on_type_error`` — each
        caller passes whichever value makes "do the read" the outcome
        on an unorderable comparison, so an ambiguous peek never causes
        records to be skipped. (A caller that reads when this is ``True``
        keeps the default ``True``; the early-exit caller, which *skips*
        when this is ``True``, passes ``False``.)
        """
        new_dt = self._to_utc_datetime(new_value)
        prev_dt = self._to_utc_datetime(prev_value)
        if new_dt is not None and prev_dt is not None:
            return new_dt > prev_dt
        try:
            return new_value > prev_value
        except TypeError:
            return default_on_type_error

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

    def _resolve_tiebreak_field(
        self, table_name: str, table_options: Dict[str, str]
    ) -> Optional[str]:
        """Resolve a unique, server-sortable property used to break
        cursor ties in incremental reads.

        A non-unique ``cursor_field`` (e.g. a second-granularity
        timestamp shared by many rows) needs a unique tiebreaker so a
        strict ``gt`` cursor does not silently skip rows that share the
        boundary value. The tiebreaker MUST be a declared property:
        Foundry's objectSet API rejects system fields
        (``__primaryKey`` / ``__rid``) in ``orderBy``/``where`` with
        ``PropertiesNotFound``.

        Resolution order: an explicit ``tiebreaker_field`` option, then
        the ontology's declared primary-key property when it is a single
        real property. Returns ``None`` when no usable property exists
        (e.g. a multi-column primary key, or the only key is the system
        ``__primaryKey``) — the incremental caller then refuses the read
        with an actionable error rather than fall back to a lossy
        single-cursor ``gt`` (which would drop rows sharing a cursor
        value); the user must supply ``tiebreaker_field`` or use snapshot.
        """
        explicit = table_options.get("tiebreaker_field")
        if explicit:
            return explicit
        obj_type = self._get_object_type(table_name)
        primary_key = obj_type.get("primaryKey")
        if isinstance(primary_key, str) and primary_key:
            return primary_key
        if (
            isinstance(primary_key, list)
            and len(primary_key) == 1
            and primary_key[0]
        ):
            return primary_key[0]
        return None

    def _build_incremental_where(
        self, cursor_field: str, prev_max_cursor: Any,
        tiebreak_field: Optional[str], prev_tiebreak: Any,
    ) -> Optional[dict]:
        """Build the server-side ``where`` filter for an incremental read.

        - First run (``prev_max_cursor is None``): no filter → full load.
        - Resume with a tiebreaker checkpoint: a composite filter that
          resumes strictly after the last emitted ``(cursor, tiebreaker)``
          tuple — ``cursor > prev`` OR (``cursor == prev`` AND
          ``tiebreaker > prev_tiebreak``) — so rows sharing the boundary
          cursor value are neither skipped nor duplicated.
        - Resume without a persisted tiebreaker (e.g. an offset written by
          an older connector version): the legacy strict ``gt`` cursor.
        """
        if prev_max_cursor is None:
            return None
        if tiebreak_field is not None and prev_tiebreak is not None:
            return {
                "type": "or",
                "value": [
                    {
                        "type": "gt",
                        "field": cursor_field,
                        "value": prev_max_cursor,
                    },
                    {
                        "type": "and",
                        "value": [
                            {
                                "type": "eq",
                                "field": cursor_field,
                                "value": prev_max_cursor,
                            },
                            {
                                "type": "gt",
                                "field": tiebreak_field,
                                "value": prev_tiebreak,
                            },
                        ],
                    },
                ],
            }
        return {
            "type": "gt",
            "field": cursor_field,
            "value": prev_max_cursor,
        }

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
        prev_tiebreak = (
            start_offset.get("max_tiebreak_value") if start_offset else None
        )

        # Resolve a unique, server-sortable tiebreaker property. A
        # non-unique cursor (e.g. a timestamp shared by many rows) needs
        # one so the composite (cursor, tiebreaker) ordering is total and
        # the offset can resume past the last emitted row without
        # skipping rows that share the boundary cursor value.
        tiebreak_field = self._resolve_tiebreak_field(table_name, table_options)

        # Fail safe: if no unique, server-sortable tiebreaker resolves
        # (the object type declares no single-column primary key and no
        # ``tiebreaker_field`` option was given), a strict ``gt`` cursor
        # would silently DROP rows that share a cursor value across a
        # ``max_records_per_batch`` boundary. Refuse the incremental read
        # with an actionable error rather than lose data — the caller can
        # supply ``tiebreaker_field`` or run the table in snapshot mode.
        if tiebreak_field is None:
            raise ValueError(
                f"Incremental (CDC) read of '{table_name}' needs a unique, "
                f"server-sortable tiebreaker so rows sharing a cursor value "
                f"are not silently dropped, but none could be resolved: the "
                f"object type declares no single-column primary key and no "
                f"'tiebreaker_field' table option was provided. Fix: set "
                f"'tiebreaker_field' to a unique sortable property, or run "
                f"this table in snapshot mode (omit 'cursor_field')."
            )

        # Early-exit short-circuit: peek at the current dataset max
        # cursor via a single ``search orderBy desc, limit 1`` call and
        # bail out if nothing newer than our checkpoint exists. Skips
        # the (paginated) ``loadObjects`` round-trip on no-op polls.
        #
        # Skipped on first run (``prev_max_cursor is None``). On lookup
        # failure (helper returns ``None``) we fall through to the read.
        # With a tiebreaker we only skip when the dataset max cursor is
        # strictly BELOW the checkpoint: when it equals the checkpoint,
        # un-read rows may still exist at that cursor value with a higher
        # tiebreaker, so we must fall through to the composite read.
        if prev_max_cursor is not None:
            new_max_cursor = self._get_max_cursor_value(
                table_name, cursor_field
            )
            # Skip the (paginated) read only when the dataset's max cursor
            # is strictly BELOW our checkpoint — definitively nothing new.
            # An *equal* max may hide un-read rows sharing that cursor
            # value (the composite ``where`` resolves them), so fall
            # through and read. ``default_on_type_error=False`` makes an
            # unorderable/type-mismatched peek fall through to the read
            # too, rather than skip and risk dropping data (F3).
            # (``tiebreak_field`` is always resolved here — the ``None``
            # case is refused above — so there is no separate no-tiebreak
            # branch.)
            if new_max_cursor is not None and self._cursor_strictly_greater(
                prev_max_cursor, new_max_cursor, default_on_type_error=False
            ):
                return iter([]), start_offset

        # Build the server-side where clause (composite (cursor,
        # tiebreaker) resume, or legacy gt for a tiebreaker-less offset).
        where_clause = self._build_incremental_where(
            cursor_field, prev_max_cursor, tiebreak_field, prev_tiebreak
        )

        # Eagerly materialise records up to max_records, in
        # (cursor, tiebreaker) ASC order. The composite cursor makes the
        # ordering total, so a fixed ``max_records`` cap is safe: the
        # next microbatch resumes precisely after the last emitted tuple
        # via the composite ``where`` above — no tie-group is split.
        # Memory bound is max_records × record_size; the user controls
        # the cap via ``max_records_per_batch``.
        init_dt = self._init_dt
        records: List[dict] = []
        last_emitted_cursor: Any = None
        last_emitted_tiebreak: Any = None
        for record in self._generate_all_pages(
            table_name,
            page_size,
            where_clause=where_clause,
            order_by_field=cursor_field,
            secondary_order_field=tiebreak_field,
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
            # The tiebreaker must be non-null to keep the (cursor,
            # tiebreaker) ordering total. A null value would persist
            # ``max_tiebreak_value: None``, which the next run cannot
            # distinguish from "no tiebreaker" and would silently fall
            # back to a lossy strict-``gt`` cursor (F1). ``tiebreak_field``
            # is always resolved here (the ``None`` case is refused
            # above), so fail safe on a null value rather than lose data.
            tiebreak_value = record.get(tiebreak_field)
            if tiebreak_value is None:
                raise ValueError(
                    f"Tiebreaker field '{tiebreak_field}' is null on a record "
                    f"of '{table_name}'. A tiebreaker must be unique and "
                    f"non-null to guarantee no rows are dropped at a cursor "
                    f"boundary. Set 'tiebreaker_field' to a non-nullable "
                    f"unique property, or run this table in snapshot mode."
                )
            records.append(record)
            last_emitted_cursor = cursor_value
            last_emitted_tiebreak = tiebreak_value

        # No records emitted → terminate the microbatch with the
        # same offset so Spark sees "no progress" and stops.
        if not records:
            return iter([]), start_offset if start_offset else {
                "max_cursor_value": prev_max_cursor
            }

        end_offset: dict = {"max_cursor_value": last_emitted_cursor}
        if tiebreak_field is not None:
            end_offset["max_tiebreak_value"] = last_emitted_tiebreak
        return iter(records), end_offset

    def _build_object_set(
        self, table_name: str, where_clause: Optional[dict] = None
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
        page_token: Optional[str] = None,
        page_size: int = 1000,
        order_by_field: Optional[str] = None,
        secondary_order_field: Optional[str] = None,
    ) -> Tuple[List[dict], Optional[str]]:
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
            order_fields = [{"field": order_by_field, "direction": "asc"}]
            # Secondary sort key (a unique property) makes the overall
            # ordering total when the primary cursor has ties, so the
            # composite offset can resume past the last emitted row
            # without skipping records that share the boundary value.
            if secondary_order_field:
                order_fields.append(
                    {"field": secondary_order_field, "direction": "asc"}
                )
            body["orderBy"] = {"fields": order_fields}

        # Transient failures (429/503/network) retry with bounded
        # jittered backoff; everything else returns here for the
        # non-transient check below.
        response = self._request_with_retry(
            "post", url, timeout=120, json_body=body, error_label="loadObjects"
        )

        # Non-transient 4xx/5xx — raise immediately, no retry. Wrap in a
        # RuntimeError carrying Palantir's structured error body
        # (errorCode/errorName/parameters) and the request URL, so a
        # 400 (bad filter) / 401 (expired token) / 403 (missing scope) /
        # 404 (wrong ontology or object type) surfaces the actionable
        # detail instead of a bare status line. No secret leak: the
        # bearer token is sent in a header, never in the URL or body.
        if response.status_code >= 400:
            raise RuntimeError(
                f"Palantir loadObjects request failed with status "
                f"{response.status_code} for {url}: {response.text}"
            )

        data = response.json()
        records = data.get("data", [])
        next_page_token = data.get("nextPageToken")
        return records, next_page_token
