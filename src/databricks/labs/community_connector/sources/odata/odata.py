"""OData v4 community connector for Lakeflow Connect.

Implements the LakeflowConnect interface for any OData v4 service. The
connector discovers tables and schemas from the service's ``$metadata``
endpoint, supports four auth methods (bearer / basic / api_key /
oauth2), and ingests each entity set either as a snapshot or as an
incremental CDC stream keyed off a user-supplied cursor field.

Connection options (set on the UC connection):
    service_url   required   OData service root, e.g.
                             https://services.odata.org/V4/Northwind/Northwind.svc/
    auth_type     optional   bearer | basic | api_key | oauth2
    token, username, password, api_key, api_key_header,
    oauth2_token_url, oauth2_client_id, oauth2_client_secret, oauth2_scope

Per-table options (allowlisted via externalOptionsAllowList):
    cursor_field          column to drive incremental reads; absent → snapshot
    select                comma-separated $select projection
    filter                additional $filter expression
    page_size             $top per request (default 1000)
    max_records_per_batch cap rows returned per read_table call (default 5000)
"""

import base64
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Iterator
from urllib.parse import urljoin
from xml.etree import ElementTree as ET

import requests
from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.interface.supports_namespaces import (
    SupportsNamespaces,
)


# ---------------------------------------------------------------------------
# EDM (CSDL) → Spark type mapping
# ---------------------------------------------------------------------------

_EDM_TO_SPARK = {
    "Edm.String": StringType(),
    "Edm.Boolean": BooleanType(),
    # Map every integer width to LongType. The Lakeflow framework's
    # parse_value supports IntegerType and LongType but not ShortType /
    # ByteType, and self-review's A8 prefers LongType for headroom — a
    # source-side `Edm.Int16` widening to `Int32` later would otherwise
    # be a schema-breaking change.
    "Edm.Byte": LongType(),
    "Edm.SByte": LongType(),
    "Edm.Int16": LongType(),
    "Edm.Int32": LongType(),
    "Edm.Int64": LongType(),
    "Edm.Single": FloatType(),
    "Edm.Double": DoubleType(),
    "Edm.Decimal": DecimalType(38, 18),
    "Edm.Date": DateType(),
    "Edm.DateTime": TimestampType(),
    "Edm.DateTimeOffset": TimestampType(),
    "Edm.TimeOfDay": StringType(),
    "Edm.Duration": StringType(),
    "Edm.Guid": StringType(),
    "Edm.Binary": BinaryType(),
}

_NS_EDMX = "{http://docs.oasis-open.org/odata/ns/edmx}"
_NS_EDM = "{http://docs.oasis-open.org/odata/ns/edm}"


class ODataLakeflowConnect(LakeflowConnect, SupportsNamespaces):
    """LakeflowConnect implementation for OData v4 services.

    OData ``$metadata`` documents can declare multiple ``<Schema>`` blocks,
    each with its own namespace and its own entity sets. Two schemas in the
    same service can re-use entity set names (e.g. ``Sales.Customers`` and
    ``HR.Customers``), so this connector exposes the schema namespace as a
    single-segment Lakeflow namespace path.

    Pipelines disambiguate by passing ``namespace`` in *table_options*. When
    only one schema declares a given table name, ``namespace`` may be omitted.
    """

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        self.service_url = _require(options, "service_url")
        self.timeout = int(options.get("timeout_seconds", "60"))
        self._session: requests.Session | None = None
        self._metadata_xml: str | None = None
        # Init-time ceiling so a single trigger never chases new data — a fresh
        # instance is constructed on every trigger anyway.
        self._init_ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    # ------------------------------------------------------------------
    # LakeflowConnect interface
    # ------------------------------------------------------------------

    def list_tables(self) -> list[str]:
        """Flat fallback used by the framework when SupportsNamespaces is absent.

        Returns deduped entity set names across every schema. The
        namespace-aware methods below are what the framework actually
        prefers when ``SupportsNamespaces`` is in the MRO.
        """
        names: set[str] = set()
        for ns, es_name in self._entity_set_index():
            names.add(es_name)
        return sorted(names)

    def list_namespaces(self, prefix: list[str] | None = None) -> list[list[str]]:
        # OData has a single, flat level of schema namespaces. Anything
        # below the root has no further children.
        if prefix:
            return []
        index = self._entity_set_index()
        seen = sorted({ns for ns, _ in index if ns})
        return [[ns] for ns in seen]

    def list_tables_in_namespace(self, namespace: list[str]) -> list[str]:
        index = self._entity_set_index()
        if not namespace:
            # Entity sets always live inside a Schema with a Namespace
            # attribute; root-level tables don't exist in OData v4.
            return []
        target = namespace[0]
        return sorted({es for ns, es in index if ns == target})

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        namespace = (table_options or {}).get("namespace")
        fields = self._fields_for(table_name, namespace)
        select = (table_options or {}).get("select")
        if select:
            wanted = {c.strip() for c in select.split(",")}
            fields = [f for f in fields if f.name in wanted]
        if not fields:
            raise ValueError(
                f"Could not derive a non-empty schema for entity set {table_name!r}. "
                f"Check the 'select' option."
            )
        return StructType(fields)

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        namespace = (table_options or {}).get("namespace")
        primary_keys = self._primary_keys_for(table_name, namespace)
        cursor_field = (table_options or {}).get("cursor_field")
        ingestion_type = "cdc" if cursor_field else "snapshot"
        return {
            "primary_keys": primary_keys,
            "cursor_field": cursor_field,
            "ingestion_type": ingestion_type,
        }

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        meta = self.read_table_metadata(table_name, table_options)
        if meta["ingestion_type"] == "snapshot":
            return self._read_snapshot(table_name, table_options or {})
        return self._read_incremental(
            table_name,
            start_offset or {},
            table_options or {},
            meta["cursor_field"],
        )

    # ------------------------------------------------------------------
    # Snapshot + incremental read paths
    # ------------------------------------------------------------------

    def _read_snapshot(
        self, table_name: str, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        url = self._build_url(table_name, table_options)
        records = list(self._fetch_pages(url))
        return iter(records), {}

    def _read_incremental(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
        cursor_field: str,
    ) -> tuple[Iterator[dict], dict]:
        since = start_offset.get("cursor") if start_offset else None
        # Already caught up to this trigger's init time — skip the API call.
        if since and since >= self._init_ts:
            return iter([]), start_offset

        extra_filter = self._cursor_filter(cursor_field, since)
        # Append primary-key columns as $orderby tie-breakers. Without a
        # fully unique sort, OData servers that paginate internally (via
        # `@odata.nextLink` with a value-based skiptoken) can split a
        # same-cursor cohort across pages: the skiptoken's strict-`>` on
        # the cursor value drops the unread tail. A unique total ordering
        # forces the skiptoken to use the key as well, so no rows are lost.
        namespace = (table_options or {}).get("namespace")
        order_terms = [f"{cursor_field} asc"]
        for pk in self._primary_keys_for(table_name, namespace):
            if pk != cursor_field:
                order_terms.append(f"{pk} asc")
        url = self._build_url(
            table_name,
            table_options,
            extra_filter=extra_filter,
            order_by=",".join(order_terms),
        )
        max_records = int(table_options.get("max_records_per_batch", "5000"))

        records: list[dict] = []
        truncated = False
        for row in self._fetch_pages(url):
            rec_cursor = row.get(cursor_field)
            if since is not None and rec_cursor is not None and rec_cursor <= since:
                continue
            records.append(row)
            if len(records) >= max_records:
                truncated = True
                break

        if not records:
            return iter([]), start_offset or {}

        # Cursor boundary safety: the next call resumes with
        # `cursor gt <last_cursor>`, so if the trailing records share that
        # cursor with unseen records on the next page — OR with concurrently
        # inserted siblings that arrive before the next call — the `gt`
        # filter would silently drop them. Trim back to the last distinct
        # cursor on every batch (not just truncated ones), so a stop/restart
        # or natural completion can't lose same-cursor inserts at the boundary.
        # Re-fetched rows on the next call are deduped at the destination
        # via apply_changes' MERGE on the primary key.
        trimmed = _trim_to_distinct_cursor_boundary(records, cursor_field)
        if not trimmed:
            # Every record in this batch shares one cursor value.
            if truncated:
                raise RuntimeError(
                    f"max_records_per_batch ({max_records}) is too small for "
                    f"{table_name!r}: every record in the batch shares cursor "
                    f"value {records[-1].get(cursor_field)!r}. Increase "
                    f"max_records_per_batch above the largest same-cursor "
                    f"cohort, or choose a higher-cardinality cursor field."
                )
            # Natural exhaustion of a single-cursor cohort. Emit as-is —
            # trimming would lose data with no way to re-fetch. There's a
            # residual race for same-cursor rows added between now and any
            # future call, which is unavoidable without finer cursor resolution.
        else:
            records = trimmed

        # OData responses ordered by the cursor — last record carries the max.
        last_cursor = records[-1].get(cursor_field)
        end_offset = {"cursor": last_cursor}
        if start_offset and start_offset == end_offset:
            return iter([]), start_offset
        return iter(records), end_offset

    # ------------------------------------------------------------------
    # URL + HTTP plumbing
    # ------------------------------------------------------------------

    def _build_url(
        self,
        table_name: str,
        table_options: dict[str, str],
        extra_filter: str | None = None,
        order_by: str | None = None,
    ) -> str:
        base = _join_url(self.service_url, table_name)
        params = []

        page_size = table_options.get("page_size", "1000")
        params.append(f"$top={page_size}")

        select = table_options.get("select")
        if select:
            params.append(f"$select={select}")

        filters = [f for f in (table_options.get("filter"), extra_filter) if f]
        if filters:
            joined = " and ".join(f"({f})" for f in filters)
            params.append(f"$filter={joined}")

        if order_by:
            params.append(f"$orderby={order_by}")

        return f"{base}?{'&'.join(params)}"

    def _fetch_pages(self, url: str) -> Iterator[dict]:
        """Walk @odata.nextLink, yielding raw JSON dicts (no coercion).

        The OData v4 spec allows @odata.nextLink to be either an absolute
        URL or a relative one (resolved against the request URL). Some
        services (e.g. SAP NetWeaver Gateway, certain self-hosted Olingo
        deployments) return just ``Customers?$skiptoken=...`` and rely on
        the client to prepend the service root. urljoin handles both
        cases — absolutes pass through unchanged.
        """
        session = self._get_session()
        next_url: str | None = url
        while next_url:
            resp = session.get(next_url, timeout=self.timeout)
            resp.raise_for_status()
            payload = resp.json()
            for item in payload.get("value", []):
                # Strip OData control properties that aren't real fields.
                yield {k: v for k, v in item.items() if not k.startswith("@odata.")}
            raw_next = payload.get("@odata.nextLink")
            next_url = urljoin(resp.url, raw_next) if raw_next else None

    # ------------------------------------------------------------------
    # Auth session
    # ------------------------------------------------------------------

    def _get_session(self) -> requests.Session:
        if self._session is not None:
            return self._session

        session = requests.Session()
        session.headers.update(
            {
                "Accept": "application/json",
                "OData-Version": "4.0",
                "OData-MaxVersion": "4.0",
            }
        )
        extra_headers = self.options.get("extra_headers")
        if extra_headers:
            for pair in extra_headers.split(","):
                if ":" in pair:
                    k, v = pair.split(":", 1)
                    session.headers[k.strip()] = v.strip()

        auth_type = (self.options.get("auth_type") or "").lower().strip()
        if not auth_type and self.options.get("token"):
            auth_type = "bearer"

        if auth_type == "bearer":
            session.headers["Authorization"] = f"Bearer {_require(self.options, 'token')}"
        elif auth_type == "basic":
            from requests.auth import HTTPBasicAuth

            session.auth = HTTPBasicAuth(
                _require(self.options, "username"),
                _require(self.options, "password"),
            )
        elif auth_type == "api_key":
            header = self.options.get("api_key_header", "x-api-key")
            session.headers[header] = _require(self.options, "api_key")
        elif auth_type == "oauth2":
            session.headers["Authorization"] = f"Bearer {self._oauth2_token()}"
        elif auth_type:
            raise ValueError(
                f"Unknown auth_type {auth_type!r}. "
                f"Expected one of: bearer, basic, api_key, oauth2."
            )

        self._session = session
        return session

    def _oauth2_token(self) -> str:
        data = {
            "grant_type": "client_credentials",
            "client_id": _require(self.options, "oauth2_client_id"),
            "client_secret": _require(self.options, "oauth2_client_secret"),
        }
        scope = self.options.get("oauth2_scope")
        if scope:
            data["scope"] = scope
        resp = requests.post(
            _require(self.options, "oauth2_token_url"),
            data=data,
            timeout=self.timeout,
        )
        resp.raise_for_status()
        token = resp.json().get("access_token")
        if not token:
            raise RuntimeError("OAuth2 token endpoint did not return access_token.")
        return token

    # ------------------------------------------------------------------
    # $metadata caching + parsing
    # ------------------------------------------------------------------

    def _metadata_root(self) -> ET.Element:
        if self._metadata_xml is None:
            session = self._get_session()
            url = _join_url(self.service_url, "$metadata")
            resp = session.get(
                url,
                timeout=self.timeout,
                headers={"Accept": "application/xml"},
            )
            resp.raise_for_status()
            self._metadata_xml = resp.text
        return ET.fromstring(self._metadata_xml)

    def _entity_set_index(self) -> list[tuple[str, str]]:
        """All (schema_namespace, entity_set_name) pairs declared in $metadata."""
        root = self._metadata_root()
        out: list[tuple[str, str]] = []
        for schema in root.iter(f"{_NS_EDM}Schema"):
            ns = schema.get("Namespace") or ""
            for container in schema.iter(f"{_NS_EDM}EntityContainer"):
                for es in container.iter(f"{_NS_EDM}EntitySet"):
                    out.append((ns, es.get("Name")))
        return out

    def _entity_type_for(self, table_name: str, namespace: str | None = None) -> ET.Element:
        """Find the EntityType element backing ``table_name``.

        When ``namespace`` is None and the same name is declared in more
        than one schema, this raises so the caller passes ``namespace``
        in *table_options* to disambiguate.
        """
        root = self._metadata_root()
        matches: list[tuple[str, str]] = []  # (schema_ns, qualified_type_ref)
        for schema in root.iter(f"{_NS_EDM}Schema"):
            ns = schema.get("Namespace") or ""
            if namespace is not None and ns != namespace:
                continue
            for container in schema.iter(f"{_NS_EDM}EntityContainer"):
                for es in container.iter(f"{_NS_EDM}EntitySet"):
                    if es.get("Name") == table_name:
                        matches.append((ns, es.get("EntityType")))

        if not matches:
            available = self._entity_set_index()
            if namespace is not None:
                hint = sorted({es for ns, es in available if ns == namespace})
                raise ValueError(
                    f"Entity set {table_name!r} not found in namespace "
                    f"{namespace!r}. Available in this namespace: {hint}"
                )
            raise ValueError(
                f"Entity set {table_name!r} not found in $metadata. "
                f"Available: {sorted({n for _, n in available})}"
            )
        if len(matches) > 1:
            namespaces = sorted({m[0] for m in matches})
            raise ValueError(
                f"Entity set {table_name!r} is declared in multiple namespaces: "
                f"{namespaces}. Set 'namespace' in table_options to disambiguate."
            )

        schema_ns, type_ref = matches[0]
        if "." in type_ref:
            type_ns, type_name = type_ref.rsplit(".", 1)
        else:
            type_ns, type_name = None, type_ref

        for schema in root.iter(f"{_NS_EDM}Schema"):
            if type_ns is not None and schema.get("Namespace") != type_ns:
                continue
            for et in schema.findall(f"{_NS_EDM}EntityType"):
                if et.get("Name") == type_name:
                    return et

        raise ValueError(
            f"EntityType {type_ref!r} (referenced by entity set "
            f"{table_name!r} in schema {schema_ns!r}) not found in $metadata."
        )

    def _fields_for(self, table_name: str, namespace: str | None = None) -> list[StructField]:
        et = self._entity_type_for(table_name, namespace)
        fields: list[StructField] = []
        for prop in et.findall(f"{_NS_EDM}Property"):
            name = prop.get("Name")
            edm_type = prop.get("Type", "Edm.String")
            nullable = prop.get("Nullable", "true").lower() != "false"
            spark_type = _EDM_TO_SPARK.get(edm_type, StringType())
            fields.append(StructField(name, spark_type, nullable))
        return fields

    def _primary_keys_for(self, table_name: str, namespace: str | None = None) -> list[str]:
        et = self._entity_type_for(table_name, namespace)
        key = et.find(f"{_NS_EDM}Key")
        if key is None:
            return []
        return [ref.get("Name") for ref in key.findall(f"{_NS_EDM}PropertyRef")]

    # ------------------------------------------------------------------
    # Cursor filter formatting
    # ------------------------------------------------------------------

    def _cursor_filter(self, cursor_field: str, since: Any) -> str:
        end_lit = _odata_literal(self._init_ts)
        if since is None:
            return f"{cursor_field} le {end_lit}"
        return f"{cursor_field} gt {_odata_literal(since)} and {cursor_field} le {end_lit}"


# ---------------------------------------------------------------------------
# Helpers (module-level, no class state — easier to unit-test)
# ---------------------------------------------------------------------------


def _trim_to_distinct_cursor_boundary(
    records: list[dict],
    cursor_field: str,
) -> list[dict]:
    """Drop trailing records that share the boundary cursor value.

    Walks back from the tail until the cursor value changes, leaving a clean
    boundary that the next call's ``cursor gt <last>`` filter will pick up
    cleanly. Drops the boundary record itself — we can't tell whether the
    next page (or a concurrent insert before the next call) holds more
    records sharing that cursor value, so we surrender the whole group and
    let `cursor gt <prev_distinct>` re-fetch them.

    Returns an empty list when every record shares one cursor value; the
    caller decides whether that's recoverable (natural exhaustion) or a hard
    failure (truncated batch with too-small cap).
    """
    boundary = records[-1].get(cursor_field)
    trim_idx = len(records)
    while trim_idx > 0 and records[trim_idx - 1].get(cursor_field) == boundary:
        trim_idx -= 1
    return records[:trim_idx]


def _require(options: dict[str, str], key: str) -> str:
    val = options.get(key)
    if not val:
        raise ValueError(f"Required option {key!r} is missing.")
    return val


def _join_url(base: str, suffix: str) -> str:
    if base.endswith("/"):
        return f"{base}{suffix}"
    return f"{base}/{suffix}"


def _odata_literal(value: Any) -> str:
    """Render a Python value as an OData v4 literal for $filter."""
    if isinstance(value, datetime):
        return value.isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | float | Decimal):
        return str(value)
    # ISO-8601 timestamp strings render bare; everything else is quoted.
    s = str(value)
    if _looks_like_iso8601(s):
        return s
    return "'" + s.replace("'", "''") + "'"


def _looks_like_iso8601(s: str) -> bool:
    if len(s) < 10 or s[4] != "-" or s[7] != "-":
        return False
    try:
        datetime.fromisoformat(s.replace("Z", "+00:00"))
        return True
    except ValueError:
        return False


# Re-export base64/binary helper for any downstream caller that wants
# to materialize Edm.Binary fields into Python bytes prior to Spark.
def _decode_binary(value: str) -> bytes:
    return base64.b64decode(value)
