"""OData v4 community connector for Lakeflow Connect.

Implements the LakeflowConnect interface for any OData v4 service. The
connector discovers tables and schemas from the service's ``$metadata``
endpoint, supports four auth methods (bearer / basic / api_key /
oauth2), and ingests each entity set either as a snapshot, an
incremental CDC stream keyed off a user-supplied cursor field, or
(when the service supports it) a server-driven delta query stream.

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
    max_records_per_batch cap rows returned per read_table call (default 100000)
    delta_tracking        disabled (default) | auto | enabled. Opt-in.
                          When the source honours ``Prefer: odata.track-changes``
                          (MS Graph, Dataverse, SAP S/4HANA Cloud …), the
                          connector reads via the OData delta link instead of
                          cursor filtering, and emits removals as in-band
                          ``_deleted=True`` rows. ``auto`` probes once per
                          table and falls back to cursor/snapshot if the
                          server doesn't acknowledge; ``enabled`` requires
                          support and errors if the server doesn't acknowledge;
                          ``disabled`` skips the probe entirely.
"""

import base64
import itertools
import time
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
    IntegerType,
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
    # Widen integers up to Int32 to IntegerType (the framework's
    # parse_value doesn't support ShortType or ByteType, so the narrow
    # EDM widths can't map to their natural Spark types). Int64 needs
    # the full 64-bit range, so it stays as LongType.
    "Edm.Byte": IntegerType(),
    "Edm.SByte": IntegerType(),
    "Edm.Int16": IntegerType(),
    "Edm.Int32": IntegerType(),
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

# Delta tracking constants.
#
# Synthetic columns appended to the schema when delta is active so the
# destination MERGE (apply_changes) has a sequence column and a tombstone
# flag. Their names are namespaced so they can't collide with any real
# OData property — OData property names start with a letter, never an
# underscore.
_DELTA_PREFER = "odata.track-changes"
_DELETED_COL = "_deleted"
_SEQUENCE_COL = "_lc_sequence"
# Monotonic across the whole process — guarantees each emitted record
# has a strictly increasing sequence value, so apply_changes can pick a
# deterministic winner when the same primary key appears multiple times
# in one batch (e.g. update then delete arriving back-to-back).
_SEQUENCE_COUNTER = itertools.count()


def _next_sequence() -> str:
    """Strictly-increasing per-record sequence value for apply_changes.

    ISO-8601 prefix keeps the value human-readable and sortable
    lexicographically across pipeline restarts; the integer suffix
    distinguishes records emitted in the same microsecond.
    """
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return f"{now}_{next(_SEQUENCE_COUNTER):012d}"


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
        # Monotonic deadline (seconds) for the current OAuth access token.
        # Set when the token endpoint returns ``expires_in``; `None` means we
        # don't know the expiry (user-supplied access token without metadata)
        # so we fall through to the 401-retry path only.
        self._access_token_expires_at: float | None = None
        # Delta-tracking capability cache, keyed by (namespace_or_empty,
        # table_name). Populated lazily by ``_probe_delta_support`` on the
        # first metadata-resolution call for each table in ``auto`` mode.
        # ``enabled`` mode trusts the user and skips the cache; ``disabled``
        # mode never touches it.
        self._delta_capable: dict[tuple[str, str], bool] = {}

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
        # When delta tracking is active for this table the connector emits
        # two synthetic columns alongside the entity's own properties:
        # ``_deleted`` (in-band tombstone flag) and ``_lc_sequence`` (the
        # cursor column apply_changes uses to order updates). Both must be
        # in the declared schema so Spark accepts the records.
        if self._delta_active_for(table_name, table_options):
            fields = list(fields) + [
                StructField(_DELETED_COL, BooleanType(), False),
                StructField(_SEQUENCE_COL, StringType(), False),
            ]
        return StructType(fields)

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        namespace = (table_options or {}).get("namespace")
        primary_keys = self._primary_keys_for(table_name, namespace)
        user_cursor = (table_options or {}).get("cursor_field")
        if self._delta_active_for(table_name, table_options):
            # Delta-tracked tables: server delivers change events, the
            # connector adds ``_deleted`` for tombstones and uses the
            # synthetic ``_lc_sequence`` so apply_changes has a
            # deterministic sequence column. ``cdc`` (not
            # ``cdc_with_deletes``) — following the microsoft_teams
            # convention of in-band deletes — keeps the framework's
            # read_table / read_table_deletes split out of the picture.
            return {
                "primary_keys": primary_keys,
                "cursor_field": _SEQUENCE_COL,
                "ingestion_type": "cdc",
            }
        return {
            "primary_keys": primary_keys,
            "cursor_field": user_cursor,
            "ingestion_type": "cdc" if user_cursor else "snapshot",
        }

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        opts = table_options or {}
        offset = start_offset or {}
        # Dispatch on (a) what the prior offset says we're mid-stream of,
        # and (b) whether delta tracking is active for this table. The
        # offset-shape check covers both modes resuming from a
        # checkpointed offset and the very first call after delta got
        # turned on (offset is empty either way until delta produces a
        # link).
        if (
            "delta_link" in offset
            or "next_link" in offset
            or self._delta_active_for(table_name, opts)
        ):
            return self._read_incremental_delta(table_name, offset, opts)
        if opts.get("cursor_field"):
            return self._read_incremental(table_name, offset, opts, opts["cursor_field"])
        return self._read_snapshot(table_name, opts)

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
        # No wall-clock upper bound on the cursor — `max_records_per_batch`
        # is the only per-call cap. Each call fetches `cursor gt since`
        # (no `le` clause), advances the offset, and Spark drives the
        # call loop. Two consequences worth knowing:
        #   * Continuous SDP pipelines pick up new rows as they arrive,
        #     because we never freeze a "snapshot at startup" timestamp.
        #     The connector instance can live for the whole stream and
        #     each batch still sees fresh source state.
        #   * Cursor type doesn't matter for the filter. Timestamps,
        #     monotonic integer IDs, GUIDs — anything the server can
        #     order in `$orderby` and compare in `$filter` works the
        #     same way. There is no type mismatch between the cursor
        #     literal and the server's column type because we don't
        #     manufacture a timestamp ceiling out of wall-clock time.
        since = start_offset.get("cursor") if start_offset else None
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
        max_records = int(table_options.get("max_records_per_batch", "100000"))

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

    def _read_incremental_delta(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Delta-tracked read via OData ``Prefer: odata.track-changes``.

        Three offset shapes, three entry behaviours:

        * No ``delta_link`` and no ``next_link`` — bootstrap. Send the
          initial entity-set GET with ``Prefer: odata.track-changes``,
          verify the server acknowledges via ``Preference-Applied``, and
          stream the full snapshot. The terminal page carries
          ``@odata.deltaLink``.
        * ``next_link`` set — we hit ``max_records_per_batch`` mid-
          pagination on a previous call. Resume by walking from that
          link.
        * ``delta_link`` set — server's "changes since" cursor. Walk
          ``@odata.nextLink`` chain (if any) until the terminal page
          delivers a fresh ``@odata.deltaLink``.

        Records emitted carry two synthetic columns: ``_deleted`` (bool,
        in-band tombstone flag — set ``True`` for ``@removed`` entries)
        and ``_lc_sequence`` (monotonic per-record string used as
        apply_changes' sequence column). The microsoft_teams connector
        established this convention in this repo; we follow it so the
        framework's standard ``cdc`` path handles tombstones without
        needing ``cdc_with_deletes`` + ``read_table_deletes`` split.
        """
        prev_delta_link = (start_offset or {}).get("delta_link")
        prev_next_link = (start_offset or {}).get("next_link")
        is_bootstrap = prev_delta_link is None and prev_next_link is None
        url, initial_headers = self._delta_initial_request(
            table_name, table_options, prev_delta_link, prev_next_link
        )

        namespace = (table_options or {}).get("namespace")
        primary_keys = self._primary_keys_for(table_name, namespace)
        max_records = int((table_options or {}).get("max_records_per_batch", "100000"))

        records, new_delta_link, carry_next_link, rebootstrap = self._delta_walk_pages(
            url=url,
            initial_headers=initial_headers,
            is_bootstrap=is_bootstrap,
            prev_delta_link=prev_delta_link,
            prev_next_link=prev_next_link,
            table_name=table_name,
            table_options=table_options,
            primary_keys=primary_keys,
            max_records=max_records,
        )
        if rebootstrap:
            # 410 Gone surfaced during pagination → re-bootstrap from
            # scratch. ``MERGE``-on-PK + ``_lc_sequence`` ordering
            # reconciles re-fetched rows at the destination; no data
            # loss, only HTTP cost.
            return self._read_incremental_delta(table_name, {}, table_options)

        # Graph-rotation guard. Some servers (notably Microsoft Graph)
        # mint a fresh ``@odata.deltaLink`` on every response, even when
        # the change set is empty. If we already had a delta link and
        # produced no records this call, hand back the prior link so the
        # framework sees ``end_offset == start_offset`` and a trigger
        # like AvailableNow can terminate. Following microsoft_teams.py.
        if prev_delta_link is not None and not records and not carry_next_link:
            return iter([]), {"delta_link": prev_delta_link}

        if carry_next_link:
            offset: dict = {"next_link": carry_next_link}
            # Preserve the prior delta_link as a fallback if the
            # next_link expires before the cap-resume call lands.
            if prev_delta_link is not None:
                offset["delta_link"] = prev_delta_link
            return iter(records), offset

        if new_delta_link is None:
            # Reached end of stream without a deltaLink. Server is
            # misbehaving (spec requires the terminal page to carry one).
            # Resume from the prior delta_link if we have one — better
            # than losing the offset entirely.
            if prev_delta_link is not None:
                return iter(records), {"delta_link": prev_delta_link}
            raise RuntimeError(
                f"OData delta bootstrap for {table_name!r} ended without an "
                f"@odata.deltaLink. The server may have aborted change "
                f"tracking. Set delta_tracking=disabled to fall back to "
                f"snapshot or cursor-based reads."
            )

        return iter(records), {"delta_link": new_delta_link}

    def _build_delta_record(self, item: dict, primary_keys: list[str]) -> dict:
        """Translate one delta payload entry into the emitted record shape.

        - ``@removed`` entries become tombstones: a record carrying the
          primary-key fields plus ``_deleted=True``.
        - Regular adds/changes pass through with all ``@odata.*`` control
          properties stripped and ``_deleted=False``.

        Every emitted record gets ``_lc_sequence`` — a strictly monotonic
        string — so apply_changes has a deterministic sequence_by column.
        """
        if "@removed" in item:
            record: dict = {pk: item.get(pk) for pk in primary_keys}
            record[_DELETED_COL] = True
        else:
            record = {k: v for k, v in item.items() if not k.startswith("@odata.")}
            record[_DELETED_COL] = False
        record[_SEQUENCE_COL] = _next_sequence()
        return record

    def _delta_initial_request(
        self,
        table_name: str,
        table_options: dict[str, str] | None,
        prev_delta_link: str | None,
        prev_next_link: str | None,
    ) -> tuple[str, dict[str, str] | None]:
        """Pick the first URL + headers for a delta read.

        ``next_link`` wins over ``delta_link`` when both are present —
        we were mid-pagination on a cap hit, finish that before
        consulting the prior change cursor.
        """
        if prev_next_link is not None:
            return prev_next_link, None
        if prev_delta_link is not None:
            return prev_delta_link, None
        return self._build_url(table_name, table_options or {}), {"Prefer": _DELTA_PREFER}

    def _delta_walk_pages(
        self,
        *,
        url: str,
        initial_headers: dict[str, str] | None,
        is_bootstrap: bool,
        prev_delta_link: str | None,
        prev_next_link: str | None,
        table_name: str,
        table_options: dict[str, str] | None,
        primary_keys: list[str],
        max_records: int,
    ) -> tuple[list[dict], str | None, str | None, bool]:
        """Walk the ``@odata.nextLink`` chain until a deltaLink, cap, or 410.

        Returns ``(records, new_delta_link, carry_next_link, rebootstrap)``:

        * ``records`` — emitted records (already passed through
          :py:meth:`_build_delta_record`).
        * ``new_delta_link`` — the freshest ``@odata.deltaLink`` seen.
        * ``carry_next_link`` — set when ``max_records`` capped the read
          mid-pagination; the caller persists this in the offset.
        * ``rebootstrap`` — True iff a 410 fired on a stored link; the
          caller re-issues from a fresh empty offset.
        """
        session = self._get_session()
        records: list[dict] = []
        new_delta_link: str | None = None
        carry_next_link: str | None = None
        page_index = 0
        bootstrap_verified = not is_bootstrap
        sparse_checked = False
        current_url: str | None = url

        while current_url:
            headers = initial_headers if (page_index == 0 and initial_headers) else None
            kwargs: dict[str, Any] = {"headers": headers} if headers else {}
            resp = self._http_get(session, current_url, **kwargs)
            if resp.status_code == 410 and (prev_delta_link or prev_next_link):
                return [], None, None, True
            resp.raise_for_status()

            if not bootstrap_verified:
                self._verify_delta_bootstrap(resp, table_name)
                bootstrap_verified = True

            payload = resp.json()
            sparse_checked = self._delta_collect_page_records(
                payload=payload,
                records=records,
                primary_keys=primary_keys,
                table_name=table_name,
                table_options=table_options,
                sparse_checked=sparse_checked,
                max_records=max_records,
            )
            current_url, new_delta_link, carry_next_link = self._delta_advance_links(
                payload=payload,
                resp_url=resp.url,
                records=records,
                max_records=max_records,
                new_delta_link=new_delta_link,
                carry_next_link=carry_next_link,
            )
            page_index += 1

        return records, new_delta_link, carry_next_link, False

    def _verify_delta_bootstrap(self, resp: requests.Response, table_name: str) -> None:
        """Confirm the server actually honored ``Prefer: odata.track-changes``."""
        applied = resp.headers.get("Preference-Applied", "")
        if _DELTA_PREFER not in applied.lower():
            raise RuntimeError(
                f"OData server did not honor 'Prefer: odata.track-changes' "
                f"for {table_name!r} (response missing 'Preference-Applied' "
                f"header). The probe in delta_tracking=auto should have "
                f"caught this — your service may have inconsistent support. "
                f"Set delta_tracking=disabled and use cursor-based "
                f"incremental instead."
            )

    def _delta_collect_page_records(
        self,
        *,
        payload: dict,
        records: list[dict],
        primary_keys: list[str],
        table_name: str,
        table_options: dict[str, str] | None,
        sparse_checked: bool,
        max_records: int,
    ) -> bool:
        """Append delta records from ``payload`` until cap or page exhaustion.

        Returns the updated ``sparse_checked`` flag so the caller can
        continue to skip the check on later pages once it's already
        passed for this call.
        """
        for item in payload.get("value", []):
            if not sparse_checked and "@removed" not in item:
                self._check_no_sparse_entity(item, table_name, table_options)
                sparse_checked = True
            records.append(self._build_delta_record(item, primary_keys))
            if len(records) >= max_records:
                break
        return sparse_checked

    def _delta_advance_links(
        self,
        *,
        payload: dict,
        resp_url: str,
        records: list[dict],
        max_records: int,
        new_delta_link: str | None,
        carry_next_link: str | None,
    ) -> tuple[str | None, str | None, str | None]:
        """Resolve the next URL + offset bookkeeping after one page.

        Returns ``(next_url, new_delta_link, carry_next_link)``.
        ``next_url`` is ``None`` when pagination should stop (either we
        hit the cap, saw a terminal deltaLink, or the server omitted
        both pagination links).
        """
        raw_delta = payload.get("@odata.deltaLink")
        raw_next = payload.get("@odata.nextLink")
        cap_hit = len(records) >= max_records

        if cap_hit:
            if raw_next:
                carry_next_link = urljoin(resp_url, raw_next)
            # Capture the deltaLink even on a cap-hit page — when the
            # cap lines up exactly with the terminal page we still want
            # the next-batch resume to follow ``delta_link`` rather
            # than re-walk the whole bootstrap.
            if raw_delta and new_delta_link is None:
                new_delta_link = urljoin(resp_url, raw_delta)
            return None, new_delta_link, carry_next_link

        if raw_delta:
            new_delta_link = urljoin(resp_url, raw_delta)
            return None, new_delta_link, carry_next_link
        if raw_next:
            return urljoin(resp_url, raw_next), new_delta_link, carry_next_link
        return None, new_delta_link, carry_next_link

    def _check_no_sparse_entity(
        self,
        item: dict,
        table_name: str,
        table_options: dict[str, str] | None,
    ) -> None:
        """Refuse silently-corrupting sparse delta responses.

        OData v4 §11.4 lets a delta payload return only the *changed*
        properties on an updated entity. That sounds harmless until you
        realize the connector emits the dict as-is to Spark, which
        treats absent fields as NULL — overwriting good destination
        values with nulls on every partial update. The damage is silent
        and not recoverable from the destination table alone.

        We can't safely apply partial updates in v1, so refuse them up
        front with an actionable error. Run only on the first
        non-tombstone entry per call.

        The expected key set is the declared schema for the table, less
        any selection imposed by ``$select`` and less the synthetic
        ``_deleted`` / ``_lc_sequence`` columns we add ourselves.
        """
        select = (table_options or {}).get("select")
        if select:
            expected = {c.strip() for c in select.split(",") if c.strip()}
        else:
            namespace = (table_options or {}).get("namespace")
            expected = {f.name for f in self._fields_for(table_name, namespace)}
        expected -= {_DELETED_COL, _SEQUENCE_COL}
        actual = {k for k in item.keys() if not k.startswith("@odata.")}
        missing = expected - actual
        if missing:
            raise RuntimeError(
                f"OData delta response for {table_name!r} returned a sparse "
                f"entity: missing properties {sorted(missing)}. The connector "
                f"cannot safely apply partial updates — every missing field "
                f"would write NULL at the destination, silently corrupting "
                f"data. Set delta_tracking=disabled to use cursor-based "
                f"incremental, or restrict the schema with $select to only "
                f"the fields the server always returns in delta payloads."
            )

    # ------------------------------------------------------------------
    # Delta tracking capability
    # ------------------------------------------------------------------

    def _delta_setting(self, table_options: dict[str, str] | None) -> str:
        """Resolve the delta_tracking option, normalised to lower case.

        Defaults to ``disabled``. Delta tracking is opt-in because most
        OData services don't honor ``Prefer: odata.track-changes``, and
        a default-``auto`` would burn one wasted HTTP probe per table
        per pipeline trigger on the common case where the user doesn't
        want this feature anyway.
        """
        raw = ((table_options or {}).get("delta_tracking") or "disabled").strip().lower()
        if raw not in {"auto", "enabled", "disabled"}:
            raise ValueError(
                f"Invalid delta_tracking={raw!r}. Expected one of: auto, enabled, disabled."
            )
        return raw

    def _delta_cache_key(
        self, table_name: str, table_options: dict[str, str] | None
    ) -> tuple[str, str]:
        """Cache key for :py:attr:`_delta_capable` keyed on (namespace, table).

        Namespace defaults to the empty string when omitted so multi-schema
        services with a single un-namespaced declaration also key cleanly.
        """
        namespace = (table_options or {}).get("namespace") or ""
        return (namespace, table_name)

    def _delta_active_for(self, table_name: str, table_options: dict[str, str] | None) -> bool:
        """Whether delta tracking is the read mode for this table.

        Resolution order:
          1. ``delta_tracking=disabled`` → never.
          2. ``cursor_field`` set + ``delta_tracking=enabled`` → ValueError
             (the two are mutually exclusive — delta tracking provides
             its own sequencing).
          3. ``cursor_field`` set + ``delta_tracking=auto`` → cursor wins;
             delta is left dormant, no probe.
          4. ``delta_tracking=enabled`` → assume support; a probe failure
             surfaces at read time rather than here.
          5. ``delta_tracking=auto`` → probe once, cache, decide.
        """
        setting = self._delta_setting(table_options)
        if setting == "disabled":
            return False
        if (table_options or {}).get("cursor_field"):
            if setting == "enabled":
                raise ValueError(
                    "delta_tracking=enabled is mutually exclusive with "
                    "cursor_field; the server-driven delta stream provides "
                    "its own sequencing. Remove cursor_field, or switch to "
                    "delta_tracking=disabled to use cursor-based incremental."
                )
            return False
        if setting == "enabled":
            return True
        key = self._delta_cache_key(table_name, table_options)
        if key not in self._delta_capable:
            self._delta_capable[key] = self._probe_delta_support(table_name, table_options)
        return self._delta_capable[key]

    def _probe_delta_support(self, table_name: str, table_options: dict[str, str] | None) -> bool:
        """Light-touch capability probe.

        Sends a small GET against the entity set with the
        ``Prefer: odata.track-changes`` header and inspects the response
        headers for ``Preference-Applied: odata.track-changes``. That
        header is the spec's positive acknowledgement that the server is
        honoring change tracking on this request.

        Returns ``False`` for every failure mode (non-200, missing
        header, malformed body, network error). The cache is populated
        with that ``False`` so we don't retry the probe per call — the
        connector falls back to whatever cursor/snapshot path the
        user's options imply.
        """
        # Force ``$top=1`` for the probe so the response stays small even
        # against entity sets with millions of rows. We only care about
        # headers.
        probe_options = {**(table_options or {}), "page_size": "1"}
        url = self._build_url(table_name, probe_options)
        try:
            session = self._get_session()
            resp = self._http_get(
                session,
                url,
                headers={"Prefer": _DELTA_PREFER},
            )
        except (requests.RequestException, ValueError, RuntimeError, PermissionError):
            return False
        if resp.status_code != 200:
            return False
        applied = resp.headers.get("Preference-Applied", "")
        return _DELTA_PREFER in applied.lower()

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
            resp = self._http_get(session, next_url)
            resp.raise_for_status()
            payload = resp.json()
            for item in payload.get("value", []):
                # Strip OData control properties that aren't real fields.
                yield {k: v for k, v in item.items() if not k.startswith("@odata.")}
            raw_next = payload.get("@odata.nextLink")
            next_url = urljoin(resp.url, raw_next) if raw_next else None

    def _http_get(self, session: requests.Session, url: str, **kwargs: Any) -> requests.Response:
        """GET with auth-aware 401/403 handling.

        Three layers, escalating in cost:

        1. **Pre-emptive refresh** — when the token endpoint gave us
           ``expires_in`` and we're past the recorded deadline (with a
           60 s safety buffer), swap the bearer header *before* sending
           the request. Avoids a wasted round-trip on long-running
           paginated reads that straddle an expiry boundary.
        2. **Reactive refresh** — caught 401 from the source, OAuth
           refresh path is available → mint a fresh token and retry
           once. A second 401 here means the access token is fine but
           the principal lacks read access (different error path).
        3. **Actionable no-refresh-path failure** — 401 or 403 from
           the source and no automatic refresh is configured (bearer,
           basic, api_key, or OAuth without a refresh-issuing token
           endpoint). Raise a :class:`PermissionError` whose message
           names the specific connection options the operator should
           check for the configured auth mode. ``requests.HTTPError``
           with no context is too opaque to triage from a pipeline log.
        """
        if self._should_preemptively_refresh():
            session.headers["Authorization"] = f"Bearer {self._oauth2_token()}"
        resp = session.get(url, timeout=self.timeout, **kwargs)
        if resp.status_code == 401 and self._has_oauth_refresh_path():
            session.headers["Authorization"] = f"Bearer {self._oauth2_token()}"
            resp = session.get(url, timeout=self.timeout, **kwargs)
            if resp.status_code == 401:
                # We just minted a token straight from the OAuth provider
                # and the source still rejected it — the access token isn't
                # the problem. Most likely the principal lacks read access
                # to this entity set, the scope is insufficient, or the
                # tenant is mis-mapped. Surface that explicitly so the user
                # doesn't chase a non-existent token issue.
                raise PermissionError(
                    f"OData service returned 401 for {url!r} even after "
                    f"refreshing the OAuth2 access token. The new token "
                    f"reached the server, so the access token itself is "
                    f"not the problem. Check that the OAuth principal has "
                    f"read access to this entity set, that 'oauth2_scope' "
                    f"grants the right permissions, and that any "
                    f"tenant/instance identifier in 'service_url' or "
                    f"'extra_headers' matches the credentials. Server "
                    f"response: {_truncate(resp.text, 300)}"
                )
            return resp
        if resp.status_code in (401, 403):
            raise PermissionError(self._no_refresh_auth_error(resp, url))
        return resp

    def _no_refresh_auth_error(self, resp: requests.Response, url: str) -> str:
        """Construct an actionable auth-failure message for the no-refresh path.

        Different auth modes have very different failure modes — bearer
        tokens expire, OAuth scopes can be too narrow, basic creds rot —
        and bundling them all into one generic "401 Unauthorized" makes
        triage from a pipeline log nearly impossible. This method picks
        the relevant remediation hints based on which auth mode is
        active on the connection.
        """
        status = resp.status_code
        body = _truncate(resp.text, 300) or "(empty body)"
        auth = (self.options.get("auth_type") or "").lower().strip()
        if not auth and self.options.get("token"):
            auth = "bearer"
        prefix = (
            f"OData service returned {status} for {url!r} and no "
            f"automatic token-refresh path is configured. "
        )
        if auth == "bearer":
            return (
                f"{prefix}"
                f"With auth_type=bearer the pre-acquired access token cannot "
                f"be refreshed by the connector — either it has expired "
                f"(typical lifetime ~1 h), or the principal that issued it "
                f"lacks read access to this entity set. Fixes: replace "
                f"'token' on the connection with a fresh one; or switch to "
                f"auth_type=oauth2 with 'oauth2_client_id' + "
                f"'oauth2_client_secret' so the connector mints and "
                f"refreshes tokens automatically. For Microsoft Graph "
                f"high-privilege endpoints (identityProviders, auditLogs, "
                f"etc.), ensure the token carries the required scope and "
                f"admin consent. Server response: {body}"
            )
        if auth == "basic":
            return (
                f"{prefix}"
                f"With auth_type=basic the credentials are sent on every "
                f"request unchanged. Check 'username' and 'password' on "
                f"the connection — the password may have expired or been "
                f"rotated — and confirm the user has read access to this "
                f"entity set at the source. Server response: {body}"
            )
        if auth == "api_key":
            return (
                f"{prefix}"
                f"With auth_type=api_key the key is sent on every request "
                f"unchanged. Check 'api_key' (may have been rotated or "
                f"revoked) and 'api_key_header' (some services expect a "
                f"non-default header name). Confirm the key's scope "
                f"includes this entity set. Server response: {body}"
            )
        if auth == "oauth2":
            return (
                f"{prefix}"
                f"With auth_type=oauth2 but no refresh path available "
                f"(missing 'oauth2_client_id' / 'oauth2_client_secret' "
                f"for client-credentials, and no 'oauth2_refresh_token' "
                f"for user-flow refresh), the connector can't mint a "
                f"fresh access token. Provide one of those pairs, or "
                f"replace 'oauth2_access_token' with a fresh value. "
                f"Also confirm 'oauth2_scope' grants read on this entity "
                f"set. Server response: {body}"
            )
        return (
            f"{prefix}"
            f"No authentication is configured on this connection but the "
            f"OData service requires it. Set 'auth_type' to one of: "
            f"bearer, basic, api_key, oauth2 (with the matching parameter "
            f"set). Server response: {body}"
        )

    def _should_preemptively_refresh(self) -> bool:
        """True iff a known-expiry token has hit its 60 s safety window."""
        if self._access_token_expires_at is None:
            return False
        return time.monotonic() >= self._access_token_expires_at

    def _has_oauth_refresh_path(self) -> bool:
        """True iff `_oauth2_token()` can mint a fresh access token.

        Either a refresh token is on hand (user flow) or
        ``oauth2_client_id`` + ``oauth2_client_secret`` are present
        for the client-credentials grant.
        """
        if self.options.get("oauth2_refresh_token"):
            return True
        return bool(
            self.options.get("oauth2_client_id") and self.options.get("oauth2_client_secret")
        )

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
            # Two sub-modes share this branch:
            #  * **User flow** — `oauth2_refresh_token` is set. A
            #    pre-supplied `oauth2_access_token` is used as-is if
            #    present (avoids an unnecessary round-trip); otherwise
            #    `_oauth2_token()` runs the refresh-token grant to
            #    mint one. Expired tokens mid-run are caught in
            #    `_http_get` and refreshed once.
            #  * **Client-credentials flow** — no refresh token; the
            #    connector mints a fresh access token via
            #    `client_credentials` at session start.
            initial_token = self.options.get("oauth2_access_token") or self._oauth2_token()
            session.headers["Authorization"] = f"Bearer {initial_token}"
        elif auth_type:
            raise ValueError(
                f"Unknown auth_type {auth_type!r}. "
                f"Expected one of: bearer, basic, api_key, oauth2."
            )

        self._session = session
        return session

    def _oauth2_token(self) -> str:
        """Mint an OAuth2 access token.

        Picks the grant type from what's available in `self.options`:
          * `oauth2_refresh_token` present -> `refresh_token` grant
            (user-flow refresh). Client id/secret are required so the
            token endpoint can authenticate the client.
          * Otherwise -> `client_credentials` grant (server-to-server).

        Some providers issue a rotated refresh token in the response;
        when that happens, the new value is written back into
        `self.options` so the next refresh uses it.
        """
        refresh_token = self.options.get("oauth2_refresh_token")
        if refresh_token:
            data = {
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": _require(self.options, "oauth2_client_id"),
                "client_secret": _require(self.options, "oauth2_client_secret"),
            }
        else:
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
        # Surface a precise, actionable error when the token endpoint
        # itself rejects the request. raise_for_status() would otherwise
        # produce a terse "401 Client Error: Unauthorized for url ..."
        # that doesn't tell the user *which* credential is the problem.
        if resp.status_code in (400, 401):
            grant = data["grant_type"]
            hint = _extract_oauth_error_hint(resp)
            if grant == "refresh_token":
                raise ValueError(
                    f"OAuth2 token endpoint returned {resp.status_code} when "
                    f"refreshing the access token. The refresh token may be "
                    f"expired, revoked, or paired with a different OAuth "
                    f"client. Check that 'oauth2_refresh_token' was issued by "
                    f"the same 'oauth2_client_id' configured on this "
                    f"connection, and re-run the authorization-code flow if "
                    f"needed. Server response: {hint}"
                ) from None
            raise ValueError(
                f"OAuth2 token endpoint returned {resp.status_code} for the "
                f"client_credentials grant. Check 'oauth2_client_id', "
                f"'oauth2_client_secret', 'oauth2_token_url', and "
                f"'oauth2_scope' on this connection. Server response: {hint}"
            ) from None
        resp.raise_for_status()
        payload = resp.json()
        token = payload.get("access_token")
        if not token:
            raise RuntimeError("OAuth2 token endpoint did not return access_token.")
        rotated_refresh = payload.get("refresh_token")
        if rotated_refresh:
            self.options["oauth2_refresh_token"] = rotated_refresh
        # Track wall-clock deadline so `_http_get` can refresh the token
        # *before* the source returns 401. Subtract a 60 s safety margin
        # to cover clock skew + in-flight request latency. Absent
        # `expires_in` means the provider didn't tell us — fall back to
        # the lazy 401-retry path.
        expires_in = payload.get("expires_in")
        if expires_in is not None:
            try:
                self._access_token_expires_at = time.monotonic() + int(expires_in) - 60
            except (TypeError, ValueError):
                self._access_token_expires_at = None
        else:
            self._access_token_expires_at = None
        return token

    # ------------------------------------------------------------------
    # $metadata caching + parsing
    # ------------------------------------------------------------------

    def _metadata_root(self) -> ET.Element:
        if self._metadata_xml is None:
            session = self._get_session()
            url = _join_url(self.service_url, "$metadata")
            resp = self._http_get(session, url, headers={"Accept": "application/xml"})
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
        et = self._resolve_type_ref(type_ref)
        if et is None:
            raise ValueError(
                f"EntityType {type_ref!r} (referenced by entity set "
                f"{table_name!r} in schema {schema_ns!r}) not found in $metadata."
            )
        return et

    def _schema_alias_map(self) -> dict[str, str]:
        """Build {namespace_or_alias → actual_schema_namespace}.

        CSDL allows each ``<Schema>`` to declare both a ``Namespace`` and
        a shorter ``Alias``; downstream type references can use either.
        Microsoft Graph for instance declares
        ``Namespace="microsoft.graph" Alias="graph"`` and then writes
        ``BaseType="graph.directoryObject"``. We can't resolve those
        references without honoring the alias.
        """
        root = self._metadata_root()
        out: dict[str, str] = {}
        for schema in root.iter(f"{_NS_EDM}Schema"):
            ns = schema.get("Namespace") or ""
            if ns:
                out[ns] = ns
            alias = schema.get("Alias")
            if alias:
                out[alias] = ns
        return out

    def _resolve_type_ref(self, type_ref: str) -> ET.Element | None:
        """Find the ``<EntityType>`` element for a qualified type reference.

        Accepts both fully-qualified namespace references
        (``microsoft.graph.user``) and alias-based references
        (``graph.user``). Returns ``None`` if no matching declaration
        exists — callers decide whether that's a hard error or worth
        falling back to a shallower lookup.
        """
        if "." not in type_ref:
            return None
        prefix, type_name = type_ref.rsplit(".", 1)
        aliases = self._schema_alias_map()
        target_ns = aliases.get(prefix)
        if target_ns is None:
            return None
        root = self._metadata_root()
        for schema in root.iter(f"{_NS_EDM}Schema"):
            if schema.get("Namespace") != target_ns:
                continue
            for et in schema.findall(f"{_NS_EDM}EntityType"):
                if et.get("Name") == type_name:
                    return et
        return None

    def _resolve_base_chain(self, et: ET.Element) -> list[ET.Element]:
        """Walk the ``BaseType`` chain starting at ``et``.

        Returns ``[et, parent, grandparent, …]`` until ``BaseType`` is
        absent or unresolvable. OData v4 §8.4: derived entity types
        inherit Keys and Properties from their base. Real-world
        services (Microsoft Graph, Microsoft Dataverse, most SAP
        deployments) lean on this heavily — Graph's ``user`` extends
        ``directoryObject`` extends ``entity``, and ``entity`` is the
        type that actually declares ``<Key>id</Key>``.

        Cycles are guarded against (cyclic CSDL is malformed but won't
        crash the connector).
        """
        chain = [et]
        current = et
        seen: set[str] = set()
        while True:
            base_ref = current.get("BaseType")
            if not base_ref or base_ref in seen:
                break
            seen.add(base_ref)
            parent = self._resolve_type_ref(base_ref)
            if parent is None:
                break
            chain.append(parent)
            current = parent
        return chain

    def _fields_for(self, table_name: str, namespace: str | None = None) -> list[StructField]:
        et = self._entity_type_for(table_name, namespace)
        # Aggregate properties across the inheritance chain. Walk from
        # the root base type down to the leaf so inherited fields
        # (typically ``id`` and any timestamp fields on
        # ``graph.directoryObject``) appear before the leaf's own
        # additions — matches the order a developer reading the CSDL
        # would expect. Derived types can ADD properties; spec forbids
        # them from REDEFINING base properties, so we de-dupe by name
        # and let the closest-to-root declaration win.
        fields: list[StructField] = []
        seen_names: set[str] = set()
        for type_el in reversed(self._resolve_base_chain(et)):
            for prop in type_el.findall(f"{_NS_EDM}Property"):
                name = prop.get("Name")
                if name in seen_names:
                    continue
                seen_names.add(name)
                edm_type = prop.get("Type", "Edm.String")
                nullable = prop.get("Nullable", "true").lower() != "false"
                spark_type = _EDM_TO_SPARK.get(edm_type, StringType())
                fields.append(StructField(name, spark_type, nullable))
        return fields

    def _primary_keys_for(self, table_name: str, namespace: str | None = None) -> list[str]:
        et = self._entity_type_for(table_name, namespace)
        # OData v4 §8.4: derived types inherit Keys from their base.
        # First Key found while walking from this type up to its root
        # ancestor wins — that's the spec's overriding-derived-wins
        # semantics for the rare case where multiple layers in the
        # chain redeclare Key (technically forbidden but services do
        # ship malformed CSDL).
        for type_el in self._resolve_base_chain(et):
            key = type_el.find(f"{_NS_EDM}Key")
            if key is not None:
                return [ref.get("Name") for ref in key.findall(f"{_NS_EDM}PropertyRef")]
        return []

    # ------------------------------------------------------------------
    # Cursor filter formatting
    # ------------------------------------------------------------------

    def _cursor_filter(self, cursor_field: str, since: Any) -> str | None:
        """Build the `$filter` clause for an incremental fetch.

        Strict `cursor gt since` once the offset has advanced; `None` on
        the very first call so the server returns the natural start of
        the table. `max_records_per_batch` is the per-call cap — there
        is no wall-clock ceiling, which is what makes continuous polling
        work and what keeps the connector type-agnostic over the cursor
        column.
        """
        if since is None:
            return None
        return f"{cursor_field} gt {_odata_literal(since)}"


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


def _extract_oauth_error_hint(resp: requests.Response) -> str:
    """Pull the most informative error description out of an OAuth2 response.

    Token endpoints conventionally return JSON with ``error`` (machine code,
    e.g. ``invalid_grant``) and often ``error_description`` (human-readable).
    Fall back to the raw body when the response isn't JSON, and truncate so
    we never dump a 50 KB error page into a user-facing message.
    """
    try:
        payload = resp.json()
    except ValueError:
        return _truncate(resp.text, 300) or "Unauthorized"
    if isinstance(payload, dict):
        description = payload.get("error_description")
        code = payload.get("error")
        if description and code:
            return f"{code}: {description}"
        if description:
            return str(description)
        if code:
            return str(code)
    return _truncate(resp.text, 300) or "Unauthorized"


def _truncate(text: str, limit: int) -> str:
    """Cap a string at ``limit`` chars with a trailing ellipsis when clipped."""
    if text is None:
        return ""
    if len(text) <= limit:
        return text
    return text[:limit] + "..."


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
