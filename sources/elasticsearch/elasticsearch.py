from typing import Iterator, Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    BooleanType,
    BinaryType,
    ArrayType,
    TimestampType,
)


class _AuthMethod:
    """Authentication method for producing auth headers for Elasticsearch API requests."""

    def headers(self) -> dict[str, str]:
        raise NotImplementedError

    @classmethod
    def keys(cls) -> list[str]:
        raise NotImplementedError

    @staticmethod
    def kind() -> str:
        raise NotImplementedError


class _ApiKeyAuth(_AuthMethod):
    def __init__(self, api_key: str):
        self._api_key = api_key

    def headers(self) -> dict[str, str]:
        return {"Authorization": f"ApiKey {self._api_key}"}

    @staticmethod
    def keys() -> list[str]:
        return ["api_key"]

    @staticmethod
    def kind() -> str:
        return "API_KEY"


class _BearerAuth(_AuthMethod):
    def __init__(self, token: str):
        self._token = token

    def headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._token}"}

    @classmethod
    def keys(cls) -> list[str]:
        return ["token"]

    @staticmethod
    def kind() -> str:
        return "BEARER"


class _ElasticsearchClient:
    def __init__(
        self,
        endpoint: str,
        auth: _AuthMethod,
        verify_ssl: bool = True,
        timeout: float | tuple[float, float] = (5.0, 30.0),
        max_retries: int = 3,
        backoff_factor: float = 0.5,
    ):
        self._endpoint = endpoint.rstrip("/")
        self._auth = auth
        self._verify_ssl = verify_ssl
        self._timeout = timeout

        # Configure a session with basic retry/backoff for transient failures.
        retry = Retry(
            total=max_retries,
            connect=max_retries,
            read=max_retries,
            status=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=frozenset(["GET", "POST"]),
            respect_retry_after_header=True,
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session = requests.Session()
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        self._session = session

    def _build_headers(self, extra: dict[str, str] | None = None) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        headers.update(self._auth.headers())
        if extra:
            headers.update(extra)
        return headers

    def get(self, path: str, params: dict | None = None) -> dict:
        resp = self._session.get(
            f"{self._endpoint}{path}",
            headers=self._build_headers(),
            params=params,
            verify=self._verify_ssl,
            timeout=self._timeout,
        )
        resp.raise_for_status()
        return resp.json()

    def post(self, path: str, json: dict, params: dict | None = None) -> dict:
        resp = self._session.post(
            f"{self._endpoint}{path}",
            headers=self._build_headers(),
            json=json,
            params=params,
            verify=self._verify_ssl,
            timeout=self._timeout,
        )
        resp.raise_for_status()
        return resp.json()


class LakeflowConnect:

    _supported_auth_strategies = [
        _ApiKeyAuth,
        _BearerAuth,
    ]

    def __init__(self, options: dict[str, str]) -> None:
        """
        Initialize the Elasticsearch connector.

        Required options:
          - endpoint: base URL (e.g., https://elasticsearch-host:9200)
          - authentication: one of [API_KEY, BEARER]
            - api_key when authentication=API_KEY
            - token when authentication=BEARER
        Optional:
          - verify_ssl: "true"/"false" to control TLS verification (default: true)
        """
        endpoint = options.get("endpoint")
        authentication = options.get("authentication", "").upper()

        if not endpoint or authentication == "":
            raise ValueError("Missing required options: endpoint, authentication")

        auth_method = next((method for method in self._supported_auth_strategies if method.kind() == authentication), None)

        if auth_method is None:
            raise ValueError(f"Unsupported authentication method: {authentication}")

        auth_opts: dict[str, str] = {}

        for k in auth_method.keys():
            if k not in options or options.get(k, None) is None:
                raise ValueError(f"Missing required authentication option: {k}")
            auth_opts[k] = options.get(k)

        verify_ssl = str(options.get("verify_ssl", "true")).lower() != "false"

        self._client = _ElasticsearchClient(
            endpoint=endpoint,
            auth=auth_method(**auth_opts),
            verify_ssl=verify_ssl
        )

        self._default_cursor_fields = ["timestamp", "updated_at", "_seq_no"] # evaluated in order!

        # Cache for discovered and available indices to avoid repeated API calls
        self._indices_cache: list[str] = []

        # Cache for index properties to avoid repeated API calls
        self._index_properties_cache: dict[str, dict] = {}

        # Cache for table metadata to avoid recomputation
        self._metadata_cache: dict[str, dict[str, Any]] = {}

        # Cache for discovered schemas to avoid recomputation
        self._schema_cache: dict[str, StructType] = {}

    def _discover_indices(self) -> list[str]:
        """Fetch indices and aliases visible to the provided credentials."""
        candidates: set[str] = set()

        # _cat/indices returns a list of dicts with "index"
        cat = self._client.get(path="/_cat/indices", params={"format": "json", "expand_wildcards": "all"})
        if isinstance(cat, list):
            for entry in cat:
                idx = entry.get("index")
                if idx:
                    candidates.add(idx)

        # aliases: include alias names too
        aliases_resp = self._client.get(path="/_aliases")
        if isinstance(aliases_resp, dict):
            for idx, data in aliases_resp.items():
                candidates.add(idx)
                aliases = data.get("aliases") or {}
                for alias in aliases.keys():
                    candidates.add(alias)

        accessible: set[str] = set()

        # Probe each candidate with a lightweight search to ensure accessibility
        for idx in candidates:
            try:
                self._client.post(path=f"/{idx}/_search", json={"size": 0}, params={"size": 0})
                accessible.add(idx)
            except Exception:
                continue

        return sorted(accessible)

    def _fetch_index_properties(self, index: str) -> dict:
        """
        Fetch and cache index properties for Elasticsearch 8.x+ (typeless mappings).
        Expects `mappings.properties` to be present; raises if the structure is not found.
        """
        if index in self._index_properties_cache:
            return self._index_properties_cache[index]

        # Fetch mapping from API
        mapping = self._client.get(f"/{index}/_mapping")

        # mapping can be keyed by concrete index even when requested via alias
        if not isinstance(mapping, dict):
            raise ValueError(f"Unexpected mapping response for index/alias {index}")

        if index in mapping:
            item = mapping[index]

        elif len(mapping) == 1:
            # alias resolving to a single concrete index
            _, item = next(iter(mapping.items()))

        else:
            raise ValueError(f"Expected mapping for {index}, got keys: {list(mapping.keys())}")

        properties = item.get("mappings", {}).get("properties")
        if not isinstance(properties, dict):
            raise ValueError(
                "Expected typeless mapping with 'mappings.properties' (Elasticsearch 8.x+). "
                f"Received keys: {list(item.get('mappings', {}).keys())}"
            )

        # Cache the result
        self._index_properties_cache[index] = properties

        return properties

    @staticmethod
    def _is_field_available(properties: dict, field_path: str | None) -> bool:
        """
        Determine whether a (possibly dotted) field path exists in the mapping. Supports
        nested objects (properties) and multi-fields (fields).
        """
        parts = (field_path or "").split(".")
        current = properties

        for part in parts:
            next_level = None
            if part in current:
                next_level = current[part]
            elif "properties" in current and isinstance(current["properties"], dict) and part in current["properties"]:
                next_level = current["properties"][part]
            elif "fields" in current and isinstance(current["fields"], dict) and part in current["fields"]:
                next_level = current["fields"][part]
            else:
                return False
            current = next_level

        return True

    def _metadata_for_index(self, index: str, options: dict[str, str]) -> dict[str, Any]:
        """Compute metadata (PKs, ingestion type, cursor) for a given index."""

        properties = self._fetch_index_properties(index=index)

        # Determine cursor field
        if "cursor_field" in options:
            cursor_field = options["cursor_field"]
            if not self._is_field_available(properties=properties, field_path=cursor_field):
                raise ValueError(f"Configured cursor_field '{cursor_field}' not found in index '{index}'")

        else:
            # fallback to default cursor fields, None if no default field is available
            cursor_field = next(
                (
                    field for field in self._default_cursor_fields
                    if self._is_field_available(properties=properties, field_path=field)
                ),
                None
            )

        # Determine ingestion type with optional override (requires a cursor)
        ingestion_override = str(options.get("ingestion_type", "")).lower()
        has_cursor = cursor_field is not None
        ingestion_type = "cdc" if has_cursor else "snapshot"
        if ingestion_override in {"append", "cdc"}:
            ingestion_type = ingestion_override if has_cursor else "snapshot"

        metadata: dict[str, Any] = {
            "primary_keys": ["_id"],
            "ingestion_type": ingestion_type,
        }
        if cursor_field:
            metadata["cursor_field"] = cursor_field
        return metadata

    def _map_elasticsearch_field_to_spark(self, name: str, field: dict) -> StructField:
        """
        Convert an Elasticsearch field definition to a StructField.
        Unknown or dynamic types are mapped to StringType.
        """
        field_type = field.get("type")

        # object → struct; nested → array<struct>
        if "properties" in field and field_type != "nested":
            return StructField(
                name=name,
                dataType=self._properties_to_struct(properties=field.get("properties", {})),
                nullable=True,
            )

        if field_type == "nested":
            return StructField(
                name=name,
                dataType=ArrayType(
                    self._properties_to_struct(properties=field.get("properties", {})),
                    containsNull=True
                ),
                nullable=True
            )

        type_map = {
            "keyword": StringType(),
            "text": StringType(),
            "date": TimestampType(),
            "boolean": BooleanType(),
            "binary": BinaryType(),
            "long": LongType(),
            "integer": LongType(),
            "short": LongType(),
            "byte": LongType(),
            "unsigned_long": LongType(),
            "double": DoubleType(),
            "float": DoubleType(),
            "half_float": DoubleType(),
            "scaled_float": DoubleType(),
            "geo_shape": StringType(),
            "geo_point": StructType(
                [
                    StructField("lat", DoubleType(), True),
                    StructField("lon", DoubleType(), True),
                ]
            ),
        }

        spark_type = type_map.get(field_type, StringType())
        return StructField(name=name, dataType=spark_type, nullable=True)

    def _properties_to_struct(self, properties: dict) -> StructType:
        fields = []
        for name, field in properties.items():
            fields.append(self._map_elasticsearch_field_to_spark(name=name, field=field))
        return StructType(fields)

    def _schema_for_index(self, index: str) -> StructType:
        """Get table schema for a given index."""
        properties = self._fetch_index_properties(index=index)
        return self._properties_to_struct(properties=properties)

    def _open_point_in_time(self, index: str, keep_alive: str) -> str:
        resp = self._client.post(path=f"/{index}/_pit", json={}, params={"keep_alive": keep_alive})
        pit_id = resp.get("id")
        if not pit_id:
            raise ValueError(f"Failed to open point-in-time for index {index}")
        return pit_id

    def _build_search_request(
        self,
        index: str,
        cursor_field: str | None,
        start_offset: dict | None,
        size: int,
        keep_alive: str,
    ) -> tuple[str, dict]:
        offset = start_offset or {}
        pit_id = offset.get("pit_id") or self._open_point_in_time(index=index, keep_alive=keep_alive)
        search_after = offset.get("search_after")

        query: dict[str, Any] = {"match_all": {}}
        # Backward compatibility when only a cursor value is available
        if cursor_field and not search_after and offset.get("cursor") is not None:
            query = {"range": {cursor_field: {"gt": offset["cursor"]}}}

        if cursor_field:
            # Use _shard_doc as a stable tiebreaker
            sort_order = [{cursor_field: "asc"}, {"_shard_doc": "asc"}]
        else:
            # For snapshot reads without a cursor, use a deterministic shard order
            sort_order = [{"_shard_doc": "asc"}]

        body: dict[str, Any] = {
            "pit": {"id": pit_id, "keep_alive": keep_alive},
            "size": size,
            "sort": sort_order,
            "query": query,
            "track_total_hits": False,
        }

        if search_after:
            body["search_after"] = search_after

        return pit_id, body

    def _read_index(self, index: str, start_offset: dict | None, options: dict[str, str]) -> tuple[Iterator[dict], dict]:
        options = options or {}
        start_offset = start_offset or {}

        metadata = self.read_table_metadata(table_name=index, table_options=options)

        # Page size for pagination (default 1000)
        size = int(options.get("page_size", 1000))
        keep_alive = str(options.get("pit_keep_alive", "1m"))
        cursor_field = options.get("cursor_field") or metadata.get("cursor_field")

        pit_id, search_body = self._build_search_request(
            index=index,
            cursor_field=cursor_field,
            start_offset=start_offset,
            size=size,
            keep_alive=keep_alive,
        )

        response = self._client.post(path="/_search", json=search_body)
        hits = response.get("hits", {}).get("hits", [])

        def _iter_records() -> Iterator[dict]:
            for hit in hits:
                record = dict(hit.get("_source", {}))
                record["_id"] = hit.get("_id")
                yield record

        if not hits:
            next_offset = start_offset or {}
            return _iter_records(), next_offset

        last = hits[-1]
        last_sort = last.get("sort", [])

        next_offset: dict[str, Any] = {
            "pit_id": pit_id,
            "search_after": last_sort,
        }

        if cursor_field and last_sort:
            next_offset["cursor"] = last_sort[0]
            next_offset["cursor_field"] = cursor_field

        return _iter_records(), next_offset

    def list_tables(self) -> list[str]:
        """List available indices/aliases."""
        if not self._indices_cache:
            indices = self._discover_indices()
            # Cache the result
            self._indices_cache = indices
        return self._indices_cache

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        if table_name not in self.list_tables():
            raise ValueError(f"Unsupported index: {table_name}. Index is not available or not accessible.")

        # Check cache first
        if table_name in self._schema_cache:
            return self._schema_cache[table_name]

        # Get schema for index and add _id column for downstream consumers
        schema = self._schema_for_index(index=table_name)
        fields = schema.fields + [StructField("_id", StringType(), False)]
        augmented = StructType(fields)

        # Cache the result
        self._schema_cache[table_name] = augmented

        return augmented

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict[str, Any]:
        table_options = table_options or {}
        if table_name not in self.list_tables():
            raise ValueError(f"Unsupported index: {table_name}. Index is not available or not accessible.")

        # Check cache first
        if table_name in self._metadata_cache:
            return self._metadata_cache[table_name]

        # Get metadata for index
        metadata = self._metadata_for_index(index=table_name, options=table_options)

        # Cache the result
        self._metadata_cache[table_name] = metadata

        return metadata

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:

        if table_name not in self.list_tables():
            raise ValueError(f"Unsupported index: {table_name}. Index is not available or not accessible.")

        return self._read_index(index=table_name, start_offset=start_offset, options=table_options)
