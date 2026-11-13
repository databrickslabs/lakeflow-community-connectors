import json
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Dict, Iterator, List, Mapping

import requests
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DataType,
    DecimalType,
    StringType,
    StructField,
    StructType,
)

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.hubspot.hubspot_associations import HubspotAssociations
from databricks.labs.community_connector.sources.hubspot.hubspot_non_crm import NON_CRM_TABLES, HubspotNonCrm
from databricks.labs.community_connector.sources.hubspot.hubspot_property_history import (
    HISTORY_STANDARD_OBJECTS,
    MAX_RETRIES,
    REQUEST_TIMEOUT_S,
    HubspotPropertyHistory,
    filter_property_names,
    history_table_name,
    is_history_table,
)


class HubspotLakeflowConnect(LakeflowConnect):
    _STANDARD_OBJECT_TYPES = {
        "contacts": "0-1", "companies": "0-2", "deals": "0-3", "tickets": "0-5",
        "calls": "0-48", "emails": "0-49", "meetings": "0-47", "tasks": "0-27",
        "notes": "0-46",
    }

    def __init__(self, options: dict) -> None:
        # Validate before the framework's virtual metadata decoder can parse or
        # log raw JSON. Retain an instance-scoped snapshot when adapters extract
        # entries (including .get(table, {}) for a missing table).
        self._table_configs = self._parse_table_configs(options)
        self.access_token = options["access_token"]
        self.base_url = "https://api.hubapi.com"
        self.auth_header = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }
        # Cache for discovered schemas to avoid repeated API calls
        self._schema_cache = {}
        self._property_types = {}
        # Cache for table metadata
        self._metadata_cache = {}
        self._custom_objects = None
        self._custom_object_types = {}
        self._custom_object_configs = {}

        # Freeze the upper cursor bound at init time so read_table returns a
        # stable cursor across microbatches in a single Trigger.AvailableNow
        # trigger.  Without this, the connector would chase continuously
        # arriving updatedAt timestamps indefinitely.  HubSpot timestamps are
        # ISO 8601 with milliseconds (e.g. "2026-04-23T12:34:56.789Z").
        self._init_ts = (
            datetime.now(timezone.utc)
            .strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
            + "Z"
        )

        # Centralized object metadata configuration
        # Meetings reject archived listing; complete active snapshots reconcile removals.
        self._object_config = {
            "contacts": {
                "primary_keys": ["id"],
                "cursor_field": "updatedAt",
                "cursor_property_field": "lastmodifieddate",
                "associations": ["companies"],
                "supports_deletes": True,
            },
            "companies": {
                "primary_keys": ["id"],
                "cursor_field": "updatedAt",
                "cursor_property_field": "hs_lastmodifieddate",
                "associations": ["contacts"],
                "supports_deletes": True,
            },
            "deals": {
                "primary_keys": ["id"],
                "cursor_field": "updatedAt",
                "cursor_property_field": "hs_lastmodifieddate",
                "associations": ["contacts", "companies", "tickets"],
                "supports_deletes": True,
            },
            "tickets": {
                "primary_keys": ["id"],
                "cursor_field": "updatedAt",
                "cursor_property_field": "hs_lastmodifieddate",
                "associations": ["contacts", "companies", "deals"],
                "supports_deletes": True,
            },
            "calls": {
                "primary_keys": ["id"],
                "cursor_field": "updatedAt",
                "cursor_property_field": "hs_lastmodifieddate",
                "associations": ["contacts", "companies", "deals", "tickets"],
                "supports_deletes": True,
            },
            "emails": {
                "primary_keys": ["id"],
                "cursor_field": "updatedAt",
                "cursor_property_field": "hs_lastmodifieddate",
                "associations": ["contacts", "companies", "deals", "tickets"],
                "supports_deletes": True,
            },
            "meetings": {
                "primary_keys": ["id"],
                "cursor_field": "updatedAt",
                "cursor_property_field": "hs_lastmodifieddate",
                "associations": ["contacts", "companies", "deals", "tickets"],
                "supports_deletes": False,
            },
            "tasks": {
                "primary_keys": ["id"],
                "cursor_field": "updatedAt",
                "cursor_property_field": "hs_lastmodifieddate",
                "associations": ["contacts", "companies", "deals", "tickets"],
                "supports_deletes": True,
            },
            "notes": {
                "primary_keys": ["id"],
                "cursor_field": "updatedAt",
                "cursor_property_field": "hs_lastmodifieddate",
                "associations": ["contacts", "companies", "deals", "tickets"],
                "supports_deletes": True,
            },
        }

        # Default config for custom objects
        self._default_object_config = {
            "primary_keys": ["id"],
            "cursor_field": "updatedAt",
            "cursor_property_field": "hs_lastmodifieddate",
            "associations": [],
            # Fallback only, for a table whose config was never resolved. Discovered
            # custom objects opt into deletes explicitly in _discover_custom_objects.
            "supports_deletes": False,
        }

        # {object}_property_history tables (see hubspot_property_history.py)
        self._history = HubspotPropertyHistory(
            base_url=self.base_url,
            auth_header=self.auth_header,
            init_ts=self._init_ts,
            get_property_names=lambda obj: [
                p["name"] for p in self._get_object_properties(obj) if p.get("name")
            ],
            get_cursor_property=lambda obj: self._get_object_config(obj)[
                "cursor_property_field"
            ],
            get_api_object_type=self._get_api_object_type,
        )
        self._associations = HubspotAssociations(
            self.base_url, self._history._post, get=self._get_association_page
        )
        self._non_crm = HubspotNonCrm(
            self.base_url, self.auth_header, self._custom_api_types, self._history._post
        )

    def _get_association_page(self, kind, url, params, min_interval):
        """Bounded GET transport without response bodies or record IDs in errors."""
        for attempt in range(MAX_RETRIES + 1):
            self._history._throttle(kind, min_interval)
            try:
                response = requests.get(
                    url, headers=self.auth_header, params=params, timeout=REQUEST_TIMEOUT_S
                )
            except requests.RequestException:
                raise RuntimeError("HubSpot association GET request failed") from None
            if response.status_code == 200:
                try:
                    return response.json()
                except ValueError:
                    raise RuntimeError("Malformed HubSpot association GET JSON") from None
            if response.status_code == 429 and attempt < MAX_RETRIES:
                self._history._sleep(self._history._retry_delay(response, attempt))
                continue
            raise RuntimeError(f"HubSpot association GET failed (status={response.status_code})")
        raise RuntimeError("HubSpot association GET exhausted retries")

    def read_table_between_offsets(self, table_name, start_offset, end_offset, table_options):
        table_options = self._resolve_table_options(table_name, table_options)
        if is_history_table(table_name):
            return self._history.read_between_offsets(
                table_name, start_offset, end_offset, table_options
            )
        return self.read_table(table_name, start_offset, table_options)[0]

    def _custom_api_types(self) -> List[str]:
        self._discover_custom_objects(require_access=True)
        return list(self._custom_object_types.values())

    def list_tables(self) -> list[str]:
        """
        List available tables including standard CRM objects and custom objects.
        """
        # Standard HubSpot CRM objects
        standard_tables = [
            "contacts",
            "companies",
            "deals",
            "tickets",
            "calls",
            "emails",
            "meetings",
            "tasks",
            "notes",
        ]

        # Add dynamic discovery of custom objects
        custom_objects = self._discover_custom_objects()
        standard_tables.extend(custom_objects)

        # Property history tables for the core CRM objects and custom objects
        history_tables = [
            history_table_name(obj) for obj in HISTORY_STANDARD_OBJECTS + custom_objects
        ]
        return standard_tables + history_tables + self._non_crm.list_tables()

    def _discover_custom_objects(self, require_access: bool = False) -> List[str]:
        """
        Discover custom objects from HubSpot CRM schemas API
        """
        if self._custom_objects is not None:
            return list(self._custom_objects)
        url = f"{self.base_url}/crm-object-schemas/2026-09/schemas"
        resp = requests.get(url, headers=self.auth_header, timeout=60)
        # Schema access is optional for accounts ingesting only standard CRM
        # objects. Do not cache a denied response as successful empty discovery.
        if resp.status_code == 403 and not require_access:
            return []
        if resp.status_code != 200:
            raise RuntimeError(f"HubSpot schema discovery failed: HTTP {resp.status_code}")
        schemas = resp.json().get("results")
        if not isinstance(schemas, list):
            raise RuntimeError("Malformed HubSpot schema discovery results")
        names_by_type = {v: k for k, v in self._STANDARD_OBJECT_TYPES.items()}
        custom_types = {}
        for schema in schemas:
            if not isinstance(schema, dict):
                raise RuntimeError("Malformed HubSpot custom schema")
            object_type, name = schema.get("objectTypeId"), schema.get("name")
            if object_type in self._STANDARD_OBJECT_TYPES.values():
                continue
            if not isinstance(object_type, str) or not object_type.startswith("2-") or not isinstance(name, str) or not name:
                raise RuntimeError("HubSpot custom schema requires name and objectTypeId")
            alias = name.lower()
            if alias in self._object_config or alias in NON_CRM_TABLES or alias in custom_types or object_type in names_by_type or is_history_table(alias):
                raise RuntimeError("HubSpot custom schema has a conflicting identity")
            custom_types[alias] = object_type
            names_by_type[object_type] = alias

        configs = {name: {**config, "associations": list(config["associations"])} for name, config in self._object_config.items()}
        # Custom objects do support archived listing (verified live: the
        # `archived=true` object listing returns records with `archived` and
        # `archivedAt`, plus requested properties). Without a delete flow an
        # archived custom record never leaves the destination and keeps looking
        # active, so opt discovered custom objects into deletes.
        configs.update({
            name: {**self._default_object_config, "associations": [], "supports_deletes": True}
            for name in custom_types
        })
        reserved = {"id", "createdat", "updatedat", "archived", "properties"}
        # Resolve after collecting every type so custom/custom relationships do
        # not depend on schema order. Definitions are directional, not inferred.
        discovered = {name: set() for name in configs}
        for schema in schemas:
            definitions = schema.get("associations", [])
            if not isinstance(definitions, list):
                raise RuntimeError("Malformed HubSpot association definitions")
            for definition in definitions:
                if not isinstance(definition, dict):
                    raise RuntimeError("Malformed HubSpot association definition")
                source = names_by_type.get(definition.get("fromObjectTypeId"))
                target = names_by_type.get(definition.get("toObjectTypeId"))
                if source is None or target is None:
                    # HubSpot auto-creates association definitions between a new
                    # custom object and internal object types the connector does
                    # not model (for example 0-18 communication, 0-116 postal
                    # mail, 0-51 conversation session, 0-4 engagement). There is
                    # no table to attach an association column to, so skip the
                    # edge instead of failing discovery for every table.
                    unresolved = [
                        value for value, resolved in (
                            (definition.get("fromObjectTypeId"), source),
                            (definition.get("toObjectTypeId"), target),
                        ) if resolved is None
                    ]
                    if all(
                        isinstance(value, str) and value.startswith("0-")
                        for value in unresolved
                    ):
                        continue
                    # An unresolved custom type means a schema was dropped or the
                    # account is mid-provisioning; do not silently lose the edge.
                    raise RuntimeError("Unsupported HubSpot association object type")
                if target.lower() in reserved:
                    raise RuntimeError("HubSpot association column conflicts with a base field")
                discovered[source].add(target)
        for name, targets in discovered.items():
            configs[name]["associations"].extend(sorted(targets - set(configs[name]["associations"])))
        # Commit discovery atomically: failures must not leave a partial graph.
        self._custom_object_types = custom_types
        self._custom_object_configs = {name: configs[name] for name in custom_types}
        self._object_config = {name: configs[name] for name in self._object_config}
        self._custom_objects = list(custom_types)
        return list(self._custom_objects)

    def _get_api_object_type(self, table_name: str) -> str:
        """Keep public table names stable, but address custom types by HubSpot ID."""
        if table_name not in self._object_config:
            self._discover_custom_objects()
        return self._custom_object_types.get(table_name, table_name)

    def _get_object_config(self, table_name: str) -> Dict:
        """Get configuration for a specific object type"""
        return self._object_config.get(table_name, self._custom_object_configs.get(table_name, self._default_object_config))

    # Keep in sync with connector_spec.yaml; envelopes cannot smuggle connection
    # credentials or framework routing keys through the external option allowlist.
    _TABLE_OPTION_KEYS = frozenset({
        "max_records_per_batch", "history_properties", "history_since",
        "history_lookback_minutes", "include_properties", "exclude_properties",
        "association_sync_mode", "pipeline_object_types",
    })

    @classmethod
    def _parse_table_configs(cls, table_options: Mapping[str, str] | None) -> dict | None:
        """Validate every envelope entry without logging user-supplied values."""
        options = dict(table_options or {})
        # Spark's CaseInsensitiveDict iterates lowercased keys. Its dict copy
        # loses case-insensitive lookup, so recognize the envelope explicitly.
        envelope_keys = [key for key in options if isinstance(key, str)
                         and key.lower() == "tableconfigs"]
        if not envelope_keys:
            return None
        if len(envelope_keys) != 1:
            raise ValueError("HubSpot tableConfigs contains conflicting option keys")

        def unique_object(pairs):
            result = {}
            for key, value in pairs:
                if key in result:
                    raise ValueError("HubSpot tableConfigs contains duplicate keys")
                result[key] = value
            return result

        raw = options[envelope_keys[0]]
        if not isinstance(raw, str):
            raise ValueError("HubSpot tableConfigs must be a JSON object string")
        try:
            configs = json.loads(raw, object_pairs_hook=unique_object)
        except (ValueError, TypeError, RecursionError):
            raise ValueError("HubSpot tableConfigs is invalid JSON or has duplicate keys") from None
        if not isinstance(configs, dict):
            raise ValueError("HubSpot tableConfigs must contain an object")
        for name, config in configs.items():
            if not isinstance(name, str) or not name or name.strip() != name:
                raise ValueError("HubSpot tableConfigs has an invalid table name")
            if not isinstance(config, dict) or any(
                key not in cls._TABLE_OPTION_KEYS or not isinstance(value, str)
                for key, value in config.items()
            ):
                raise ValueError("HubSpot tableConfigs has invalid table options")
        return configs

    def _resolve_table_options(
        self, table_name: str, table_options: Mapping[str, str] | None
    ) -> dict[str, str]:
        """Use the constructor snapshot or supplied complete map, never a merge.

        A constructor envelope belongs to this source instance for its lifetime.
        Conflicting supplied maps require a new source instance. Legacy instances
        without an envelope can resolve different maps on successive SPI calls.
        """
        configs = self._parse_table_configs(table_options)
        if self._table_configs is not None:
            if configs is not None and configs != self._table_configs:
                raise ValueError("HubSpot tableConfigs conflicts with the constructor map")
            configs = self._table_configs
        if configs is None:
            return dict(table_options or {})
        if table_name not in configs:
            raise ValueError("HubSpot tableConfigs is missing the requested table")
        return dict(configs[table_name])

    @staticmethod
    def _association_sync_mode(table_options: Dict[str, str] | None) -> str:
        mode = (table_options or {}).get("association_sync_mode", "incremental")
        if mode not in ("incremental", "full_refresh"):
            raise ValueError("association_sync_mode must be incremental or full_refresh")
        return mode

    def get_table_schema(
        self, table_name: str, table_options: Dict[str, str]
    ) -> StructType:
        """
        Fetch the schema of a table.

        Args:
            table_name: The name of the table to fetch the schema for.

        Returns:
            A StructType object representing the schema of the table.
        """
        table_options = self._resolve_table_options(table_name, table_options)
        if table_name in NON_CRM_TABLES:
            return self._non_crm.get_table_schema(table_name)
        supported_tables = self.list_tables()
        if table_name not in supported_tables:
            raise ValueError(
                f"Unsupported table: {table_name}. "
                f"Supported tables are: {supported_tables}"
            )

        if is_history_table(table_name):
            return self._history.get_table_schema()

        # Cache key includes the property filters — the schema is a function of
        # (table_name, table_options), so cached entries must not leak across
        # different include_properties / exclude_properties values.
        cache_key = self._options_cache_key(table_name, table_options)
        if cache_key in self._schema_cache:
            return self._schema_cache[cache_key]

        # Discover schema via API
        schema = self._discover_table_schema(table_name, table_options)

        # Cache the result
        self._schema_cache[cache_key] = schema

        return schema

    @staticmethod
    def _parse_max_records(table_options: Dict[str, str] | None) -> int | None:
        """Read max_records_per_batch from table options. None means no cap
        (opt-in admission control)."""
        if not table_options:
            return None
        raw = table_options.get("max_records_per_batch")
        if raw is None:
            return None
        try:
            value = int(raw)
        except (TypeError, ValueError):
            return None
        return value if value > 0 else None

    def read_table_metadata(
        self, table_name: str, table_options: Dict[str, str]
    ) -> dict:
        """
        Fetch the metadata of a table.

        Args:
            table_name: The name of the table to fetch the metadata for.

        Returns:
            A dictionary containing the metadata of the table. It includes the following keys:
                - primary_keys: The name of the primary key columns of the table.
                - cursor_field: The name of the field to use as a cursor for incremental loading.
                - ingestion_type: The type of ingestion to use for the table. It should be one of:
                    - "snapshot": For snapshot loading.
                    - "cdc": capture incremental changes
                    - "append": incremental append
        """
        table_options = self._resolve_table_options(table_name, table_options)
        if table_name in NON_CRM_TABLES:
            return self._non_crm.read_table_metadata(table_name)
        supported_tables = self.list_tables()
        if table_name not in supported_tables:
            raise ValueError(
                f"Unsupported table: {table_name}. "
                f"Supported tables are: {supported_tables}"
            )

        if is_history_table(table_name):
            return self._history.get_table_metadata()

        cache_key = self._options_cache_key(table_name, table_options)
        if cache_key in self._metadata_cache:
            return self._metadata_cache[cache_key]

        # Get metadata from object configuration
        metadata = self._get_table_metadata(table_name, table_options)

        # Cache the result
        self._metadata_cache[cache_key] = metadata

        return metadata

    @classmethod
    def _uses_snapshot(cls, table_name: str, table_options: Dict[str, str] | None) -> bool:
        # HubSpot rejects archived-object paging for MEETING_EVENT (0-47).
        # Complete active snapshots preserve current-state removal reconciliation.
        return table_name == "meetings" or cls._association_sync_mode(table_options) == "full_refresh"

    @classmethod
    def _options_cache_key(cls, table_name: str, table_options: Dict[str, str] | None) -> tuple:
        """Cache key covering the options that change schema/metadata output."""
        opts = table_options or {}
        return (
            table_name,
            opts.get("include_properties"),
            opts.get("exclude_properties"),
            cls._association_sync_mode(opts),
        )

    def _discover_table_schema(self, table_name: str, table_options: Dict[str, str]) -> StructType:
        """
        Discover table schema by calling HubSpot Properties API.

        Args:
            table_name: Name of the table/object to discover schema for
            table_options: Table options (include_properties / exclude_properties)

        Returns:
            StructType representing the table schema
        """
        # All CRM objects follow the same schema pattern
        return self._discover_crm_object_schema(table_name, table_options)

    def _get_table_metadata(
        self, table_name: str, table_options: Dict[str, str] | None = None
    ) -> dict:
        """
        Get metadata for a table based on object configuration.
        """
        table_options = table_options or {}
        config = self._get_object_config(table_name)

        # Get property names and cursor property field for API calls.
        # include_properties / exclude_properties bound the requested set;
        # the cursor property is always retained so incremental reads work.
        properties = self._get_object_properties(table_name)
        property_names = filter_property_names(
            [prop["name"] for prop in properties],
            table_options,
            cursor_property=config["cursor_property_field"],
        )

        # Use cdc_with_deletes only for tables that support archived queries
        supports_deletes = config.get("supports_deletes", False)
        ingestion_type = "cdc_with_deletes" if supports_deletes else "cdc"
        if self._uses_snapshot(table_name, table_options):
            ingestion_type = "snapshot"

        return {
            "primary_keys": config["primary_keys"],
            "cursor_field": config["cursor_field"],
            "cursor_property_field": config["cursor_property_field"],
            "property_names": property_names,
            "associations": config.get("associations", []),
            "ingestion_type": ingestion_type,
        }

    def _discover_crm_object_schema(
        self, table_name: str, table_options: Dict[str, str] | None = None
    ) -> StructType:
        """
        Discover CRM object schema using HubSpot Properties API.
        Works for contacts, companies, deals, tickets, and custom objects.
        """
        # Get object configuration and properties (filtered by table options)
        config = self._get_object_config(table_name)
        properties = self._get_object_properties(table_name)
        property_names = filter_property_names(
            [prop.get("name", "") for prop in properties],
            table_options,
            cursor_property=config["cursor_property_field"],
        )
        property_by_name = {prop.get("name", ""): prop for prop in properties}

        # Build base schema fields (these are always present for CRM objects)
        base_fields = [
            StructField("id", StringType(), True),
            StructField("createdAt", StringType(), True),
            StructField("updatedAt", StringType(), True),
            StructField("archived", BooleanType(), True),
        ]

        # Add association fields based on configuration
        for association in config["associations"]:
            base_fields.append(StructField(association, ArrayType(StringType()), True))

        # Build nested properties schema based on API response
        properties_fields = []

        for prop_name in property_names:
            prop = property_by_name.get(prop_name, {})
            prop_type = prop.get("type", "string")

            # Map HubSpot property types to Spark types
            spark_type = self._map_hubspot_type_to_spark(prop_type)
            properties_fields.append(StructField(prop_name, spark_type, True))

        # Create nested properties StructType
        properties_struct = StructType(properties_fields) if properties_fields else StructType([])

        # Add properties as a nested field
        base_fields.append(StructField("properties", properties_struct, True))

        schema = StructType(base_fields)

        return schema

    def _get_associations_for_object(self, table_name: str) -> List[str]:
        """Get associations to include for the given object type"""
        config = self._get_object_config(table_name)
        return config["associations"]

    def _get_object_properties(self, object_type: str) -> List[Dict]:
        """
        Fetch object properties from HubSpot Properties API
        """
        api_type = self._get_api_object_type(object_type)
        url = f"{self.base_url}/crm/properties/2026-09/{api_type}"

        try:
            resp = requests.get(url, headers=self.auth_header, timeout=60)
            if resp.status_code != 200:
                raise RuntimeError(
                    f"HubSpot Properties API error for {object_type}: "
                    f"{resp.status_code} {resp.text}"
                )

            properties = resp.json()["results"]
            self._property_types[object_type] = {
                prop["name"]: str(prop.get("type", "string")).lower() for prop in properties
            }
            return properties
        except Exception as e:
            # Re-raise (don't return an error dict): callers iterate this as a
            # list of property dicts, so a dict here turns a real API error into
            # an unrelated TypeError far downstream.
            raise RuntimeError(
                f"Failed to get object properties for {object_type}: {e}"
            ) from e

    def _map_hubspot_type_to_spark(self, hubspot_type: str) -> DataType:
        """
        Map HubSpot property types to Spark DataTypes
        HubSpot number properties can contain fractions; preserve exact decimals.
        """
        type_mapping = {
            "string": StringType(),
            "enumeration": StringType(),
            "bool": BooleanType(),  # Keep boolean as boolean for better data handling
            "number": DecimalType(38, 18),
            "date": StringType(),  # Store as string to preserve ISO format
            "date-time": StringType(),  # Store as string to preserve ISO format
            "datetime": StringType(),
            "json": StringType(),
            "phone_number": StringType(),
            "object_coordinates": StringType(),
        }

        return type_mapping.get(hubspot_type.lower(), StringType())

    def read_table(
        self, table_name: str, start_offset: dict, table_options: Dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read data from HubSpot API.

        Args:
            table_name: Name of the table to read
            start_offset: Dictionary containing cursor information for incremental reads
            table_options: Additional options for reading

        Returns:
            Tuple of (records, new_offset)
        """
        table_options = self._resolve_table_options(table_name, table_options)
        if table_name in NON_CRM_TABLES:
            return iter(self._non_crm.read_table(table_name, table_options)), {}
        supported_tables = self.list_tables()
        if table_name not in supported_tables:
            raise ValueError(
                f"Unsupported table: {table_name}. "
                f"Supported tables are: {supported_tables}"
            )

        if is_history_table(table_name):
            return self._history.read_table(table_name, start_offset, table_options)

        # Determine if this is an incremental read
        is_incremental = (
            start_offset is not None and start_offset.get("updatedAt") is not None
        )
        if self._uses_snapshot(table_name, table_options):
            is_incremental = False
            start_offset = {}

        return self._read_data(
            table_name,
            start_offset,
            incremental=is_incremental,
            table_options=table_options,
        )

    @staticmethod
    def _archive_time(value):
        if not isinstance(value, str):
            raise RuntimeError("Malformed HubSpot archive timestamp")
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise RuntimeError("Malformed HubSpot archive timestamp") from exc
        if parsed.tzinfo is None:
            raise RuntimeError("Malformed HubSpot archive timestamp timezone")
        return parsed.astimezone(timezone.utc)

    def read_table_deletes(self, table_name, start_offset, table_options):
        """Reconcile all archived pages in a frozen window before checkpointing.

        Object listing is not ordered by archivedAt. Admission cannot truncate
        this scan without skipping older archive times on later pages.
        """
        table_options = self._resolve_table_options(table_name, table_options)
        start = start_offset or {}
        if table_name in NON_CRM_TABLES or is_history_table(table_name):
            return iter([]), start
        if table_name not in self.list_tables():
            raise ValueError(f"Unsupported table: {table_name}")
        if not self._get_object_config(table_name).get("supports_deletes", False):
            return iter([]), start
        upper = self._archive_time(self._init_ts)
        checkpoint = self._archive_time(start["updatedAt"]) if "updatedAt" in start else None
        if checkpoint is not None and checkpoint >= upper:
            return iter([]), start
        properties = self.read_table_metadata(table_name, table_options).get("property_names", [])
        rows, seen_ids, seen_cursors = [], {}, set()
        after = None
        while True:
            records, next_after = self._fetch_full_refresh_batch(
                table_name, properties, associations=[], after=after, archived=True
            )
            if not records and next_after is not None:
                raise RuntimeError("Empty HubSpot archive page has pagination")
            for record in records:
                if not isinstance(record, dict) or record.get("archived") is not True:
                    raise RuntimeError("Malformed HubSpot archive record")
                raw_id = record.get("id")
                if isinstance(raw_id, bool) or not isinstance(raw_id, (str, int)) or not str(raw_id).strip():
                    raise RuntimeError("Malformed HubSpot archive identity")
                identity = str(raw_id)
                archived = self._archive_time(record.get("archivedAt"))
                if identity in seen_ids:
                    if seen_ids[identity] != record:
                        raise RuntimeError("Conflicting duplicate HubSpot archive identity")
                    continue
                seen_ids[identity] = record
                if archived > upper or (checkpoint is not None and archived <= checkpoint):
                    continue
                row = self._transform_single_record({**record, "id": identity}, table_name, properties)
                row["updatedAt"] = archived.isoformat(timespec="milliseconds").replace("+00:00", "Z")
                rows.append(row)
            if next_after is None:
                break
            if str(next_after) in seen_cursors:
                raise RuntimeError("HubSpot archive pagination cursor repeated")
            seen_cursors.add(str(next_after))
            after = next_after
            time.sleep(0.1)
        return iter(rows), {"updatedAt": self._init_ts}

    def _read_data(
        self, table_name: str, start_offset: dict = None, incremental: bool = False,
        table_options: Dict[str, str] = None
    ):
        """Read active (non-archived) data from HubSpot API"""

        # Short-circuit once the cursor has caught up to the init-time cap,
        # so Trigger.AvailableNow can terminate.
        if (
            incremental
            and start_offset
            and start_offset.get("updatedAt", "") >= self._init_ts
        ):
            return [], start_offset

        # Get discovered properties and object configuration
        metadata = self.read_table_metadata(table_name, table_options)
        property_names = metadata.get("property_names", [])
        cursor_property_field = metadata.get("cursor_property_field")
        associations = metadata.get("associations", [])

        # Only cap on the incremental path — full-refresh snapshots need
        # to drain everything in a single call to keep the snapshot atomic.
        max_records = self._parse_max_records(table_options) if incremental else None

        all_records = []
        snapshot_ids, snapshot_cursors = set(), set()
        after = None
        checkpoint = start_offset.get("updatedAt") if start_offset else None
        latest_updated = checkpoint

        while True:
            if incremental:
                # Use search API for incremental reads
                records, after, _ = self._fetch_incremental_batch(
                    table_name,
                    property_names,
                    cursor_property_field,
                    start_offset,
                    after,
                )
                # Strict > boundary: the search filter is GTE-inclusive, so the
                # record that set the watermark is returned again. Re-emitting it
                # without advancing the offset violates the
                # SimpleDataSourceStreamReader contract (a non-empty batch MUST
                # advance the end offset past the start) and the managed pipeline
                # aborts with SIMPLE_STREAM_READER_OFFSET_DID_NOT_ADVANCE.
                # Equal timestamps are common in bulk imports. Drain each
                # group before checkpointing past it.
                if checkpoint:
                    records = [
                        r for r in records
                        if (r.get("updatedAt") or "") > checkpoint
                    ]
            else:
                # Use objects API for full refresh
                records, after = self._fetch_full_refresh_batch(
                    table_name, property_names, associations, after, archived=False
                )

            if not incremental:
                if not records and after is not None:
                    raise RuntimeError("Empty HubSpot snapshot page has pagination")
                if after is not None:
                    cursor = str(after)
                    if cursor in snapshot_cursors:
                        raise RuntimeError("HubSpot snapshot pagination cursor repeated")
                    snapshot_cursors.add(cursor)
                for record in records:
                    if not isinstance(record, dict):
                        raise RuntimeError("Malformed HubSpot snapshot record")
                    identity = record.get("id")
                    if (isinstance(identity, bool) or not isinstance(identity, (str, int))
                            or not str(identity).strip() or record.get("archived", False) is not False):
                        raise RuntimeError("Malformed HubSpot active snapshot identity")
                    identity = str(identity)
                    if identity in snapshot_ids:
                        raise RuntimeError("Duplicate HubSpot snapshot identity")
                    snapshot_ids.add(identity)

            if not records:
                if incremental and after:
                    time.sleep(0.1)
                    continue
                break

            # Admission control is a soft cap: only stop when the next page
            # starts after the timestamp already drained. Checking after
            # consuming this page would skip its unprocessed equal-time peers.
            if (
                incremental
                and max_records is not None
                and len(all_records) >= max_records
                and latest_updated
                and (records[0].get("updatedAt") or "") > latest_updated
            ):
                break

            # Neither embedded list responses nor Search establish complete
            # relationships. Hydrate every selected parent before publishing it.
            self._hydrate_associations(table_name, records, associations)
            # Transform records
            transformed_records = self._transform_records(records, table_name, property_names)
            all_records.extend(transformed_records)

            # Update latest timestamp
            for record in transformed_records:
                updated_at = record.get("updatedAt")
                if updated_at and (not latest_updated or updated_at > latest_updated):
                    latest_updated = updated_at

            if after is None:
                break

            # Rate limiting
            time.sleep(0.1)

        # Cap the returned cursor at the init-time bound so the next call
        # eventually short-circuits.
        if incremental and latest_updated and latest_updated > self._init_ts:
            latest_updated = self._init_ts

        offset = {"updatedAt": latest_updated} if latest_updated else {}
        return all_records, offset

    def _hydrate_associations(self, table_name: str, records: List[Dict], targets: List[str]) -> None:
        if not targets or not records:
            return
        from_type = self._get_api_object_type(table_name)
        parent_ids = [str(record["id"]) for record in records]
        for target in targets:
            links = self._associations.read(from_type, self._get_api_object_type(target), parent_ids)
            for record in records:
                record["associations"] = record.get("associations") or {}
                record["associations"][target] = {
                    "results": [{"id": value} for value in links[str(record["id"])]]
                }

    def _fetch_full_refresh_batch(
        self,
        table_name: str,
        property_names: List[str],
        associations: List[str],
        after: str = None,
        archived: bool = False,
    ):
        """Fetch a batch of records using full refresh API"""
        archived_param = "true" if archived else "false"
        api_type = self._get_api_object_type(table_name)
        url = f"{self.base_url}/crm/objects/2026-09/{api_type}?limit=100&archived={archived_param}"

        if after is not None:
            url += f"&after={after}"
        if property_names:
            url += f"&properties={','.join(property_names)}"
        # Relationships are read separately with complete per-parent pagination.

        resp = requests.get(url, headers=self.auth_header, timeout=60)
        if resp.status_code != 200:
            raise Exception(
                f"HubSpot API error for {table_name}: {resp.status_code} {resp.text}"
            )

        data = resp.json()
        if not isinstance(data, dict) or not isinstance(data.get("results"), list):
            raise RuntimeError("Malformed HubSpot object results")
        if (any(key in data for key in ("errors", "numErrors", "category"))
                or ("status" in data and data["status"] not in ("success", "SUCCESS"))):
            raise RuntimeError("HubSpot object response contains an error envelope")
        records = data["results"]
        paging = data.get("paging", {})
        if not isinstance(paging, dict):
            raise RuntimeError("Malformed HubSpot object pagination")
        next_after = None
        if "next" in paging:
            nxt = paging["next"]
            if not isinstance(nxt, dict) or "after" not in nxt:
                raise RuntimeError("Malformed HubSpot object pagination cursor")
            next_after = nxt["after"]
            if isinstance(next_after, bool) or not isinstance(next_after, (str, int)) or not str(next_after):
                raise RuntimeError("Malformed HubSpot object pagination cursor")

        return records, next_after

    def _fetch_incremental_batch(
        self,
        table_name: str,
        property_names: List[str],
        cursor_property_field: str,
        start_offset: dict,
        after: str = None,
    ):
        """Fetch a batch of records using incremental search API"""
        last_updated = start_offset.get("updatedAt", "1970-01-01T00:00:00.000Z")

        # Convert to milliseconds for HubSpot
        try:
            last_updated_ms = int(
                datetime.fromisoformat(last_updated.replace("Z", "+00:00")).timestamp()
                * 1000
            )
        except ValueError:
            last_updated_ms = 0

        search_body = {
            "filterGroups": [
                {
                    "filters": [
                        {
                            "propertyName": cursor_property_field,
                            "operator": "GTE",
                            "value": str(last_updated_ms),
                        }
                    ]
                }
            ],
            "sorts": [
                {"propertyName": cursor_property_field, "direction": "ASCENDING"}
            ],
            "limit": 100,
            "properties": property_names or [],
        }

        if after is not None:
            search_body["after"] = after

        api_type = self._get_api_object_type(table_name)
        url = f"{self.base_url}/crm/objects/2026-09/{api_type}/search"
        resp = requests.post(url, headers=self.auth_header, json=search_body, timeout=60)

        if resp.status_code != 200:
            raise Exception(
                f"HubSpot API error for {table_name}: {resp.status_code} {resp.text}"
            )

        data = resp.json()
        records = data.get("results", [])
        next_after = data.get("paging", {}).get("next", {}).get("after")

        # Get latest update time from this batch
        latest_in_batch = last_updated
        for record in records:
            updated_at = record.get("updatedAt")
            if updated_at and updated_at > latest_in_batch:
                latest_in_batch = updated_at

        return records, next_after, latest_in_batch

    def _transform_records(self, records: List[Dict], table_name: str, property_names=None) -> List[Dict]:
        """Transform HubSpot records by flattening properties and associations"""
        return [self._transform_single_record(record, table_name, property_names) for record in records]

    def _transform_single_record(self, record: Dict, table_name: str, property_names=None) -> Dict:
        """Transform a single HubSpot record"""
        transformed_record = {}

        # Copy base fields
        for field in ["id", "createdAt", "updatedAt", "archived"]:
            if field in record:
                transformed_record[field] = record[field]

        # Copy properties with empty string to None conversion
        # HubSpot API returns "" for null values in many fields
        if "properties" in record:
            transformed_record["properties"] = self._sanitize_properties(record["properties"], table_name, property_names)

        # Handle associations
        transformed_record.update(self._extract_associations(record, table_name))

        return transformed_record

    @staticmethod
    def _decimal_property(value):
        """Reject values which Spark decimal(38,18) would round or overflow."""
        try:
            number = Decimal(str(value))
        except InvalidOperation as exc:
            raise ValueError("Invalid HubSpot numeric property") from exc
        if not number.is_finite():
            raise ValueError("Nonfinite HubSpot numeric property")
        if number == 0:
            return Decimal(0)
        sign, digits, exponent = number.as_tuple()
        digits = list(digits)
        while digits[-1] == 0:
            digits.pop()
            exponent += 1
        if max(-exponent, 0) > 18 or max(len(digits) + exponent, 0) > 20:
            raise ValueError("HubSpot numeric property exceeds decimal(38,18)")
        return Decimal((sign, tuple(digits), exponent))

    def _sanitize_properties(self, properties: Dict, table_name: str = None, property_names=None) -> Dict:
        """Normalize HubSpot nulls and preserve representable numeric values."""
        if not properties:
            return properties
        types = self._property_types.get(table_name, {})
        output = {}
        for name, value in properties.items():
            if value == "" or value is None:
                output[name] = None
            elif types.get(name) == "number" and (property_names is None or name in property_names):
                output[name] = self._decimal_property(value)
            else:
                output[name] = value
        return output

    def _extract_associations(self, record: Dict, table_name: str) -> Dict:
        """Extract association IDs from record"""
        associations_data = record.get("associations", {})
        config = self._get_object_config(table_name)
        expected_associations = config["associations"]
        result = {}

        for association_type in expected_associations:
            association_list = []
            if association_type in associations_data:
                assoc_data = associations_data[association_type]
                if isinstance(assoc_data, dict) and "results" in assoc_data:
                    association_list = [
                        item.get("id", "") for item in assoc_data["results"]
                    ]
                elif isinstance(assoc_data, list):
                    association_list = [
                        str(item) if not isinstance(item, dict) else item.get("id", "")
                        for item in assoc_data
                    ]

            result[association_type] = association_list

        return result

    def test_connection(self) -> dict:
        """Test the connection to HubSpot API"""
        try:
            url = f"{self.base_url}/crm/objects/2026-09/contacts?limit=1"
            resp = requests.get(url, headers=self.auth_header, timeout=60)

            if resp.status_code == 200:
                return {"status": "success", "message": "Connection successful"}
            else:
                return {
                    "status": "error",
                    "message": f"API error: {resp.status_code} {resp.text}",
                }
        except Exception as e:
            return {"status": "error", "message": f"Connection failed: {str(e)}"}
