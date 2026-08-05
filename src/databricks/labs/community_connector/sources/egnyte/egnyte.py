"""Egnyte content-collaboration connector (read-only).

Implements ``LakeflowConnect`` + ``SupportsPartitionedStream``.

Why partitioned?
    Two of Egnyte's read paths are genuine **server-side range queries**, which
    is the signal the partitioned-stream pattern exists for:

    * ``GET /pubapi/v2/links`` accepts ``created_after`` / ``created_before``;
    * the Audit Reporting v1 job API takes ``date_start`` / ``date_end`` and
      each window produces an independent report job.

    Those four tables (``links``, ``audit_logins``, ``audit_files``,
    ``audit_permissions``) split their time range into windows that executors
    fetch in parallel. The audit tables benefit the most: each partition is a
    create → poll → fetch round trip that spends most of its wall time waiting
    on Egnyte's report generator, so running windows concurrently converts
    dead polling time into throughput.

    The remaining tables opt out via ``is_partitioned`` and use the
    single-driver ``read_table`` path, because they have no range filter to
    split on:

    * ``files`` / ``folders`` — the File System API has no domain-wide
      "changed since" endpoint; reads are a recursive tree walk (which *is*
      partitioned for **batch** reads, by subtree — see ``get_partitions``).
    * ``users`` / ``groups`` — SCIM-style ``filter`` supports only exact match
      on name-like fields, so there is nothing to window on.
    * ``events`` — a strictly sequential ``id`` cursor feed. Partitioning it
      would require knowing the id ranges up front, and the API only exposes
      "give me what follows this id".

Auth:
    OAuth 2.0. Preferred (and the project convention) is the refresh-token
    flow: the connector holds ``client_id`` / ``client_secret`` /
    ``refresh_token`` and mints a fresh access token per run. A pre-issued
    ``access_token`` (e.g. injected by a Unity Catalog connection) is accepted
    directly and skips the exchange entirely. The token is resolved on the
    driver and travels to executors with the pickled reader, so the
    100-requests/hour token endpoint is hit at most once per run.

Tenancy:
    Every call is against a customer-specific hostname; ``domain`` is a
    required option with no discovery path (see ``egnyte_client.resolve_base_url``).

Termination (Trigger.AvailableNow):
    ``__init__`` records ``self._init_time``. ``latest_offset`` never returns
    anything past it, so once the stream drains up to that snapshot the offset
    stops moving and the trigger finishes; later data is picked up by the next
    trigger's fresh instance. Audit tables cap one step tighter — at the start
    of the current UTC day — because report windows are whole calendar days
    and re-requesting a partial day would duplicate append-only rows.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator, Sequence

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import (
    LakeflowConnect,
    SupportsPartitionedStream,
)
from databricks.labs.community_connector.sources.egnyte.egnyte_audit import (
    read_audit_window,
)
from databricks.labs.community_connector.sources.egnyte.egnyte_client import (
    EgnyteClient,
    encode_fs_path,
    fetch_access_token,
    normalize_fs_path,
    resolve_base_url,
)
from databricks.labs.community_connector.sources.egnyte.egnyte_schemas import (
    AUDIT_TABLE_TYPES,
    DEFAULT_AUDIT_BACKFILL_DAYS,
    DEFAULT_AUDIT_WINDOW_DAYS,
    DEFAULT_EVENTS_API_VERSION,
    DEFAULT_EVENTS_PAGE_SIZE,
    DEFAULT_FS_PAGE_SIZE,
    DEFAULT_LINKS_PAGE_SIZE,
    DEFAULT_LINKS_WINDOW_DAYS,
    DEFAULT_MAX_DEPTH,
    DEFAULT_MAX_PARTITIONS_PER_BATCH,
    DEFAULT_MAX_RECORDS_PER_BATCH,
    DEFAULT_MIN_REQUEST_INTERVAL,
    DEFAULT_ROOT_PATHS,
    DEFAULT_SCIM_PAGE_SIZE,
    DEFAULT_TIMEOUT,
    MAX_LINKS_PAGE_SIZE,
    MAX_PAGES_PER_READ,
    MAX_SCIM_PAGE_SIZE,
    PARTITIONED_TABLES,
    SUPPORTED_TABLES,
    TABLE_EVENTS,
    TABLE_FILES,
    TABLE_FOLDERS,
    TABLE_GROUPS,
    TABLE_LINKS,
    TABLE_METADATA,
    TABLE_SCHEMAS,
    TABLE_USERS,
)
from databricks.labs.community_connector.sources.egnyte.egnyte_utils import (
    EPOCH_ISO,
    FIRST_RUN_THRESHOLD_ISO,
    api_timestamp,
    format_date,
    format_iso,
    parent_path_of,
    parse_bool,
    parse_date,
    parse_float,
    parse_int,
    parse_iso,
    parse_optional_int,
    parse_optional_iso,
    records_from,
)

_LOG = logging.getLogger(__name__)

_TREE_TABLES = frozenset({TABLE_FILES, TABLE_FOLDERS})


class EgnyteLakeflowConnect(LakeflowConnect, SupportsPartitionedStream):
    """LakeflowConnect implementation for the Egnyte Public API.

    Connection options:
        domain              (required) tenant subdomain (``acmecorp``), a full
                            custom hostname (``files.acme.com``), or a full URL.
        access_token        Pre-issued OAuth bearer token. When present the
                            refresh exchange is skipped.
        client_id           OAuth API key, used with client_secret +
        client_secret       refresh_token to mint an access token per run.
        refresh_token
        timeout             Per-request timeout in seconds (default 30).
        min_request_interval
                            Seconds between successive calls on one client
                            (default 0.5 — Egnyte allows 2 calls/sec/token).

    Common table options:
        max_records_per_batch      Soft microbatch cap on the single-driver
                                   path (default 5000).
        max_partitions_per_batch   Time windows consumed per single-driver
                                   microbatch for links/audit (default 1).

    Per-table options are documented on each ``_read_*`` method.
    """

    # ------------------------------------------------------------------ #
    # Construction
    # ------------------------------------------------------------------ #

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)

        self.base_url = resolve_base_url(options.get("domain", ""))
        self.domain = self._domain_label(self.base_url)

        self._client_id = options.get("client_id")
        self._client_secret = options.get("client_secret")
        self._refresh_token = options.get("refresh_token")
        self._access_token = options.get("access_token") or options.get("token")

        if not self._access_token and not (
            self._client_id and self._client_secret and self._refresh_token
        ):
            raise ValueError(
                "Egnyte connector requires either 'access_token' (a pre-issued "
                "OAuth bearer token) or the trio 'client_id', 'client_secret' "
                "and 'refresh_token' for the refresh-token flow"
            )

        self._timeout = parse_int(options.get("timeout"), DEFAULT_TIMEOUT, minimum=1)
        self._min_request_interval = parse_float(
            options.get("min_request_interval"), DEFAULT_MIN_REQUEST_INTERVAL
        )

        # Termination guard. Offsets never advance past this snapshot, so a
        # Trigger.AvailableNow run drains only what existed when it started.
        now = datetime.now(timezone.utc)
        self._init_time = format_iso(now)
        # Audit reports are requested by whole calendar day; capping at
        # midnight keeps every ingested window complete and non-overlapping.
        self._audit_cap_date = format_date(now.date())

    def __getstate__(self) -> dict:
        """Drop the live HTTP client before Spark pickles us to an executor.

        The resolved ``_access_token`` *is* kept, so executors reuse the
        driver's token instead of each re-hitting the 100/hour token endpoint.
        """
        state = dict(self.__dict__)
        state.pop("_client_obj", None)
        return state

    @property
    def _client(self) -> EgnyteClient:
        client = self.__dict__.get("_client_obj")
        if client is None:
            client = EgnyteClient(
                self.base_url,
                self._ensure_access_token(),
                timeout=self._timeout,
                min_request_interval=self._min_request_interval,
            )
            self.__dict__["_client_obj"] = client
        return client

    def _ensure_access_token(self) -> str:
        """Return a bearer token, exchanging the refresh token once if needed."""
        if self._access_token:
            return self._access_token
        self._access_token = fetch_access_token(
            self.base_url,
            self._client_id or "",
            self._client_secret or "",
            self._refresh_token or "",
            timeout=self._timeout,
        )
        return self._access_token

    @staticmethod
    def _domain_label(base_url: str) -> str:
        """Stable per-tenant label stamped onto every row."""
        try:
            return base_url.split("://", 1)[-1].split("/", 1)[0]
        except (AttributeError, IndexError):
            return base_url

    # ------------------------------------------------------------------ #
    # LakeflowConnect: discovery
    # ------------------------------------------------------------------ #

    def list_tables(self) -> list[str]:
        # Egnyte exposes no machine-readable object catalog; the set of
        # resource families is fixed and documented.
        return list(SUPPORTED_TABLES)

    def get_table_schema(
        self, table_name: str, table_options: dict[str, str]
    ) -> StructType:
        self._validate_table(table_name)
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(
        self, table_name: str, table_options: dict[str, str]
    ) -> dict:
        self._validate_table(table_name)
        return dict(TABLE_METADATA[table_name])

    def _validate_table(self, table_name: str) -> None:
        if table_name not in TABLE_SCHEMAS:
            raise ValueError(
                f"Unsupported table {table_name!r}; supported: {SUPPORTED_TABLES}"
            )

    # ------------------------------------------------------------------ #
    # SupportsPartitionedStream
    # ------------------------------------------------------------------ #

    def is_partitioned(self, table_name: str) -> bool:
        """Only the range-queryable tables stream through executors."""
        return table_name in PARTITIONED_TABLES

    def latest_offset(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict | None = None,
    ) -> dict:
        """Return the high-water mark, capped at init time.

        Metadata only — no records are read here. The cap is what makes the
        offset stabilise so ``Trigger.AvailableNow`` terminates.
        """
        self._validate_table(table_name)
        if table_name in AUDIT_TABLE_TYPES:
            # Exclusive upper bound: everything strictly before today UTC.
            return {"date": self._audit_cap_date}
        if table_name == TABLE_LINKS:
            return {"cursor": self._init_time}
        if table_name == TABLE_EVENTS:
            latest = self._events_latest_id(table_options)
            return {"cursor": latest} if latest is not None else {}
        # Snapshot tables have no meaningful stream offset.
        return {}

    def get_partitions(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict | None = None,
        end_offset: dict | None = None,
    ) -> Sequence[dict]:
        """Split work into executor-sized units.

        With both offsets ``None`` this is a batch read and covers the whole
        table; with offsets it covers ``(start, end]`` only.
        """
        self._validate_table(table_name)

        if table_name in AUDIT_TABLE_TYPES:
            return self._audit_partitions(table_options, start_offset, end_offset)
        if table_name == TABLE_LINKS:
            return self._link_partitions(table_options, start_offset, end_offset)
        if table_name in _TREE_TABLES:
            # Batch-only: parallelise the recursive walk by subtree.
            return self._tree_partitions(table_options)
        # users / groups / events have no natural split — one unit of work.
        return [{"kind": "full"}]

    def read_partition(
        self, table_name: str, partition: dict, table_options: dict[str, str]
    ) -> Iterator[dict]:
        """Read one partition. Runs on an executor; must be self-contained."""
        self._validate_table(table_name)

        if table_name in AUDIT_TABLE_TYPES:
            yield from self._read_audit_window(table_name, partition, table_options)
        elif table_name == TABLE_LINKS:
            yield from self._read_links_window(
                partition.get("since"), partition.get("until"), table_options
            )
        elif table_name in _TREE_TABLES:
            yield from self._read_tree_partition(table_name, partition, table_options)
        elif table_name == TABLE_USERS:
            yield from self._read_users(table_options)
        elif table_name == TABLE_GROUPS:
            yield from self._read_groups(table_options)
        elif table_name == TABLE_EVENTS:
            records, _ = self._read_events(None, table_options)
            yield from records
        else:  # pragma: no cover - _validate_table already guards this
            raise ValueError(f"Unsupported table: {table_name!r}")

    # ------------------------------------------------------------------ #
    # LakeflowConnect: single-driver read path
    # ------------------------------------------------------------------ #

    def read_table(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        self._validate_table(table_name)

        # Snapshot tables stream lazily: a full tenant tree walk or user list
        # must not be materialised on the driver before the first row lands.
        if table_name in _TREE_TABLES:
            return (
                self._read_tree_partition(
                    table_name, {"path": None, "recursive": True}, table_options
                ),
                {},
            )
        if table_name == TABLE_USERS:
            return self._read_users(table_options), {}
        if table_name == TABLE_GROUPS:
            return self._read_groups(table_options), {}
        if table_name == TABLE_EVENTS:
            return self._read_events(start_offset, table_options)
        return self._read_windowed_fallback(table_name, start_offset, table_options)

    def _read_windowed_fallback(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Sequential single-driver equivalent of the partitioned stream.

        Used when the table is read through ``simpleStreamReader`` or an
        un-partitioned batch scan. Consumes at most
        ``max_partitions_per_batch`` windows per call (and stops early once
        ``max_records_per_batch`` is reached) so a microbatch stays bounded,
        then returns the offset at the last consumed window boundary.
        """
        start_offset = start_offset or {}
        end_offset = self.latest_offset(table_name, table_options)
        partitions = list(
            self.get_partitions(table_name, table_options, start_offset, end_offset)
        )
        if not partitions:
            # Nothing new — end_offset == start_offset makes the trigger stop.
            return iter([]), start_offset or end_offset

        max_partitions = parse_int(
            table_options.get("max_partitions_per_batch"),
            DEFAULT_MAX_PARTITIONS_PER_BATCH,
            minimum=1,
        )
        max_records = parse_int(
            table_options.get("max_records_per_batch"),
            DEFAULT_MAX_RECORDS_PER_BATCH,
            minimum=1,
        )

        records: list[dict] = []
        consumed: dict | None = None
        for partition in partitions[:max_partitions]:
            records.extend(self.read_partition(table_name, partition, table_options))
            consumed = partition
            # Append-only tables must never be truncated mid-window; we stop
            # taking *more* windows instead.
            if len(records) >= max_records:
                break

        assert consumed is not None
        return iter(records), self._offset_after(table_name, consumed)

    @staticmethod
    def _offset_after(table_name: str, partition: dict) -> dict:
        """The checkpointable offset that follows a fully-consumed partition."""
        if table_name in AUDIT_TABLE_TYPES:
            # date_end is inclusive, so resume on the following day.
            end = parse_date(partition["date_end"]) + timedelta(days=1)
            return {"date": format_date(end)}
        return {"cursor": partition["until"]}

    # ------------------------------------------------------------------ #
    # Files & folders — recursive fs tree walk
    # ------------------------------------------------------------------ #
    #
    # Table options:
    #   root_paths     Comma-separated roots to walk (default "/Shared").
    #   fs_page_size   Items per folder listing page (default 100).
    #   max_depth      Recursion depth guard (default 20).
    #   include_perms  "true" adds the per-folder permissions block.
    #   include_locks  "true" adds file lock info.

    def _tree_partitions(self, table_options: dict[str, str]) -> list[dict]:
        """One partition per immediate subtree of each configured root.

        Costs one listing call per root on the driver. The root itself gets a
        non-recursive partition (its own record plus its direct files) so no
        subtree is walked twice.
        """
        partitions: list[dict] = []
        for root in self._root_paths(table_options):
            partitions.append({"path": root, "recursive": False})
            listing = self._fs_list(root, table_options)
            for child in listing.get("folders") or []:
                child_path = child.get("path")
                if child_path:
                    partitions.append({"path": child_path, "recursive": True})
        return partitions or [{"path": DEFAULT_ROOT_PATHS, "recursive": True}]

    def _read_tree_partition(
        self, table_name: str, partition: dict, table_options: dict[str, str]
    ) -> Iterator[dict]:
        """Walk one subtree and yield only the rows this table wants."""
        recursive = bool(partition.get("recursive", True))
        path = partition.get("path")
        roots = [path] if path else self._root_paths(table_options)
        wanted = "file" if table_name == TABLE_FILES else "folder"

        for root in roots:
            for kind, record in self._walk_tree(root, table_options, recursive):
                if kind == wanted:
                    yield record

    def _walk_tree(
        self, root_path: str, table_options: dict[str, str], recursive: bool
    ) -> Iterator[tuple[str, dict]]:
        """Depth-first walk yielding ``("folder"|"file", record)`` pairs.

        There is no domain-wide "changed since" endpoint on the File System
        API, so every run re-walks the tree — hence the snapshot ingestion
        type. ``visited`` guards against symlink-ish cycles and against a
        malformed listing pointing back at an ancestor.
        """
        max_depth = parse_int(
            table_options.get("max_depth"), DEFAULT_MAX_DEPTH, minimum=0
        )
        stack: list[tuple[str, int, dict | None, str | None]] = [
            (root_path, 0, None, None)
        ]
        visited: set[str] = set()

        while stack:
            path, depth, stub, parent_path = stack.pop()
            canonical = normalize_fs_path(path)
            if canonical in visited:
                continue
            visited.add(canonical)

            listing = self._fs_list(path, table_options)
            children = listing.pop("folders", None) or []
            files = listing.pop("files", None) or []

            if listing.get("is_folder") is False and not listing.get("folder_id"):
                # The configured root turned out to be a file, not a folder.
                yield "file", self._shape_file(
                    listing, parent_path or parent_path_of(canonical)
                )
                continue

            # The child stub from the parent listing is the only place
            # `lastModified` appears; the folder's own envelope wins on
            # everything else.
            merged = {**(stub or {}), **listing}
            yield "folder", self._shape_folder(
                merged, parent_path or parent_path_of(canonical)
            )

            for record in files:
                yield "file", self._shape_file(record, canonical)

            if not recursive or depth >= max_depth:
                continue
            for child in children:
                child_path = child.get("path")
                if child_path:
                    stack.append((child_path, depth + 1, child, canonical))

    def _fs_list(self, path: str, table_options: dict[str, str]) -> dict:
        """Fetch one folder listing, merging every ``offset``/``count`` page.

        The envelope's ``folders``/``files`` arrays are concatenated across
        pages; scalar envelope fields come from the first page.
        """
        page_size = parse_int(
            table_options.get("fs_page_size"), DEFAULT_FS_PAGE_SIZE, minimum=1
        )
        base_params: dict[str, Any] = {"list_content": "true", "count": page_size}
        if parse_bool(table_options.get("include_perms")):
            base_params["perms"] = "true"
        if parse_bool(table_options.get("include_locks")):
            base_params["include_locks"] = "true"

        endpoint = f"/pubapi/v1/fs/{encode_fs_path(path)}"
        merged: dict[str, Any] | None = None
        offset = 0

        for _ in range(MAX_PAGES_PER_READ):
            params = {**base_params, "offset": offset}
            body = self._client.get_json(endpoint, params=params)
            if merged is None:
                merged = {
                    k: v for k, v in body.items() if k not in ("folders", "files")
                }
                merged["folders"] = []
                merged["files"] = []

            page_folders = body.get("folders") or []
            page_files = body.get("files") or []
            merged["folders"].extend(page_folders)
            merged["files"].extend(page_files)

            page_len = len(page_folders) + len(page_files)
            if page_len == 0:
                break
            offset += page_len
            total = parse_optional_int(body.get("total_count"))
            if total is not None and offset >= total:
                break
            if page_len < page_size:
                break

        return merged or {}

    def _root_paths(self, table_options: dict[str, str]) -> list[str]:
        raw = table_options.get("root_paths") or DEFAULT_ROOT_PATHS
        paths = [normalize_fs_path(p) for p in raw.split(",") if p.strip()]
        return paths or [DEFAULT_ROOT_PATHS]

    def _shape_file(self, raw: dict, parent_path: str | None) -> dict:
        record = {k: v for k, v in raw.items() if k not in ("folders", "files")}
        record["parent_path"] = parent_path
        record["egnyte_domain"] = self.domain
        return record

    def _shape_folder(self, raw: dict, parent_path: str | None) -> dict:
        record = {k: v for k, v in raw.items() if k not in ("folders", "files")}
        record["parent_path"] = parent_path
        record["egnyte_domain"] = self.domain
        return record

    # ------------------------------------------------------------------ #
    # Users & groups — SCIM-style paging
    # ------------------------------------------------------------------ #
    #
    # Table options:
    #   page_size        Records per page, capped at 100 by the API.
    #   filter           Verbatim SCIM filter, e.g. 'email eq "a@b.com"'.
    #   include_members  groups only: fan out one GET per group for members.

    def _scim_pages(
        self, endpoint: str, table_options: dict[str, str]
    ) -> Iterator[dict]:
        """Yield records from a ``startIndex``/``count`` SCIM list endpoint.

        ``startIndex`` is 1-based. Paging stops on an empty page, once
        ``totalResults`` has been seen, or on a short page — whichever comes
        first, so a server that ignores or clamps the page size cannot spin
        this loop forever.
        """
        page_size = min(
            MAX_SCIM_PAGE_SIZE,
            parse_int(
                table_options.get("page_size"), DEFAULT_SCIM_PAGE_SIZE, minimum=1
            ),
        )
        scim_filter = table_options.get("filter")

        start_index = 1
        seen = 0
        for _ in range(MAX_PAGES_PER_READ):
            params: dict[str, Any] = {"startIndex": start_index, "count": page_size}
            if scim_filter:
                params["filter"] = scim_filter

            body = self._client.get_json(endpoint, params=params)
            resources = body.get("resources") or body.get("Resources") or []
            if not resources:
                break

            yield from resources
            seen += len(resources)

            total = parse_optional_int(body.get("totalResults"))
            if total is not None and seen >= total:
                break
            if len(resources) < page_size:
                break
            start_index += len(resources)

    def _read_users(self, table_options: dict[str, str]) -> Iterator[dict]:
        """Full snapshot of ``/pubapi/v2/users``.

        The SCIM ``filter`` only supports exact ``eq`` on
        ``email``/``externalId``/``userName`` — there is no modified-since
        filter — so change detection is a full pull plus a downstream diff.
        """
        for raw in self._scim_pages("/pubapi/v2/users", table_options):
            record = dict(raw)
            record["egnyte_domain"] = self.domain
            yield record

    def _read_groups(self, table_options: dict[str, str]) -> Iterator[dict]:
        """Full snapshot of ``/pubapi/v2/groups``.

        The list endpoint returns only ``id`` and ``displayName``. Membership
        needs one ``GET /pubapi/v2/groups/{id}`` per group, which is an N+1
        fan-out against a 1,000-call/day quota — hence opt-in via
        ``include_members``.
        """
        include_members = parse_bool(table_options.get("include_members"))
        for raw in self._scim_pages("/pubapi/v2/groups", table_options):
            record = dict(raw)
            record.pop("schemas", None)
            if include_members and record.get("id"):
                detail = self._client.get_json(
                    f"/pubapi/v2/groups/{record['id']}"
                )
                record["members"] = detail.get("members")
            else:
                record.setdefault("members", None)
            record["egnyte_domain"] = self.domain
            yield record

    # ------------------------------------------------------------------ #
    # Links — partitioned on created_after / created_before
    # ------------------------------------------------------------------ #
    #
    # Table options:
    #   window_days      Partition width in days (default 7).
    #   start_timestamp  ISO lower bound for the very first read. Without it
    #                    the first run is one open-ended partition.
    #   page_size        Records per page (default 100, API max 500).
    #   link_path / link_type / accessibility  Server-side filters.

    def _link_partitions(
        self,
        table_options: dict[str, str],
        start_offset: dict | None,
        end_offset: dict | None,
    ) -> list[dict]:
        if start_offset is None and end_offset is None:
            start_iso = table_options.get("start_timestamp") or EPOCH_ISO
            end_iso = self._init_time
        else:
            start_iso = (
                (start_offset or {}).get("cursor")
                or table_options.get("start_timestamp")
                or EPOCH_ISO
            )
            end_iso = (end_offset or {}).get("cursor") or self._init_time

        start_dt = parse_iso(start_iso)
        end_dt = parse_iso(end_iso)
        if start_dt >= end_dt:
            return []

        # First run: one unbounded-below partition rather than decades of
        # empty weekly windows.
        if start_dt <= parse_iso(FIRST_RUN_THRESHOLD_ISO):
            return [{"since": None, "until": format_iso(end_dt)}]

        window_days = parse_int(
            table_options.get("window_days"), DEFAULT_LINKS_WINDOW_DAYS, minimum=1
        )
        partitions: list[dict] = []
        cursor = start_dt
        while cursor < end_dt:
            nxt = min(cursor + timedelta(days=window_days), end_dt)
            partitions.append({"since": format_iso(cursor), "until": format_iso(nxt)})
            cursor = nxt
        return partitions

    def _read_links_window(
        self,
        since_iso: str | None,
        until_iso: str,
        table_options: dict[str, str],
    ) -> Iterator[dict]:
        """Read one ``[since, until)`` window of ``/pubapi/v2/links``.

        v2 is used over v1 deliberately: v1's list returns bare ids and needs
        a ``GET /links/{id}`` per row, while v2 returns full Link objects.

        ``created_after``/``created_before`` are passed through ``params`` so
        requests percent-encodes the ``+`` of the UTC offset as ``%2B`` — the
        doc calls that out as a real gotcha for hand-built query strings.

        The window is treated as half-open client-side (``since <= creation
        < until``) because Egnyte's own bound inclusivity is not documented;
        without that, adjacent windows would double-count boundary rows in an
        append-only table.
        """
        page_size = min(
            MAX_LINKS_PAGE_SIZE,
            parse_int(
                table_options.get("page_size"), DEFAULT_LINKS_PAGE_SIZE, minimum=1
            ),
        )
        since_dt = parse_iso(since_iso) if since_iso else None
        until_dt = parse_iso(until_iso)

        params: dict[str, Any] = {
            "count": page_size,
            "created_before": api_timestamp(until_dt),
        }
        if since_dt is not None:
            params["created_after"] = api_timestamp(since_dt)
        for option_key, param_key in (
            ("link_path", "path"),
            ("link_type", "type"),
            ("accessibility", "accessibility"),
        ):
            if table_options.get(option_key):
                params[param_key] = table_options[option_key]

        offset = 0
        for _ in range(MAX_PAGES_PER_READ):
            body = self._client.get_json(
                "/pubapi/v2/links", params={**params, "offset": offset}
            )
            batch = records_from(body, ("links", "results", "resources", "ids"))
            if not batch:
                break

            for raw in batch:
                if not isinstance(raw, dict):
                    continue
                created = parse_optional_iso(raw.get("creation_date"))
                if created is not None:
                    if since_dt is not None and created < since_dt:
                        continue
                    if created >= until_dt:
                        continue
                record = dict(raw)
                record["egnyte_domain"] = self.domain
                yield record

            offset += len(batch)
            total = parse_optional_int(body.get("total_count"))
            if total is not None and offset >= total:
                break
            if len(batch) < page_size:
                break

    # ------------------------------------------------------------------ #
    # Events — sequential id cursor
    # ------------------------------------------------------------------ #
    #
    # Table options:
    #   events_api_version  "v1" (default) or "v2" (adds permission_change).
    #   events_page_size    Events per call (default 100).
    #   start_event_id      Bootstrap cursor; defaults to the retained oldest.
    #   event_type / folder / suppress   Server-side filters.
    #   max_records_per_batch            Microbatch cap (full pages only).

    def _events_path(self, table_options: dict[str, str], suffix: str = "") -> str:
        version = table_options.get(
            "events_api_version", DEFAULT_EVENTS_API_VERSION
        ).strip().lower()
        if version not in ("v1", "v2"):
            version = DEFAULT_EVENTS_API_VERSION
        return f"/pubapi/{version}/events{suffix}"

    def _events_cursor_info(self, table_options: dict[str, str]) -> dict:
        """Fetch (and memoize) ``/events/cursor`` for this connector instance."""
        cached = self.__dict__.get("_events_cursor_cache")
        if cached is None:
            cached = self._client.get_json(
                self._events_path(table_options, "/cursor")
            )
            self.__dict__["_events_cursor_cache"] = cached
        return cached

    def _events_latest_id(self, table_options: dict[str, str]) -> int | None:
        """The init-time high-water event id — this table's termination cap."""
        info = self._events_cursor_info(table_options)
        return parse_optional_int(info.get("latest_event_id"))

    def _events_oldest_id(self, table_options: dict[str, str]) -> int:
        info = self._events_cursor_info(table_options)
        oldest = parse_optional_int(info.get("oldest_event_id"))
        # `id` means "everything strictly after this", so step one back to
        # include the oldest retained event itself.
        return max(0, (oldest - 1)) if oldest is not None else 0

    def _read_events(
        self, start_offset: dict | None, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read forward from the stored event id.

        Append-only, so pages are never truncated client-side: we stop issuing
        *further* calls once ``max_records_per_batch`` is reached and let the
        last page land whole. Truncating would re-deliver the tail on the next
        run as permanent duplicates.

        Retention is 30 days (300K or 500K events depending on which Egnyte
        page you read), and a cursor that ages out answers ``404``. Per
        official guidance that is recovered by re-reading ``/events/cursor``
        and resuming from ``oldest_event_id``, accepting the gap.
        """
        start_offset = start_offset or {}
        cursor = parse_optional_int(start_offset.get("cursor"))
        if cursor is None:
            cursor = parse_optional_int(table_options.get("start_event_id"))
        if cursor is None:
            cursor = self._events_oldest_id(table_options)

        cap = self._events_latest_id(table_options)
        if cap is not None and cursor >= cap:
            # Caught up to the init-time snapshot — end_offset == start_offset.
            return iter([]), {"cursor": cursor}

        page_size = parse_int(
            table_options.get("events_page_size"),
            DEFAULT_EVENTS_PAGE_SIZE,
            minimum=1,
        )
        max_records = parse_int(
            table_options.get("max_records_per_batch"),
            DEFAULT_MAX_RECORDS_PER_BATCH,
            minimum=1,
        )

        base_params: dict[str, Any] = {"count": page_size}
        for option_key, param_key in (
            ("event_type", "type"),
            ("folder", "folder"),
            ("suppress", "suppress"),
        ):
            if table_options.get(option_key):
                base_params[param_key] = table_options[option_key]

        endpoint = self._events_path(table_options)
        records: list[dict] = []
        recovered = False

        while len(records) < max_records:
            response = self._client.get_raw(
                endpoint,
                params={**base_params, "id": cursor},
                expected=(200, 204, 404),
            )
            if response.status_code == 204:
                break  # documented "no new events", not an error
            if response.status_code == 404:
                if recovered:
                    break
                # Cursor fell outside the retention window.
                self.__dict__.pop("_events_cursor_cache", None)
                recovered = True
                cursor = self._events_oldest_id(table_options)
                _LOG.warning(
                    "Egnyte events cursor aged out; resuming from oldest "
                    "retained event id %s (a gap in coverage is expected)",
                    cursor,
                )
                continue

            body = response.json() if (response.content or b"").strip() else {}
            batch = body.get("events") or []
            if not batch:
                break

            for raw in batch:
                record = dict(raw)
                record["egnyte_domain"] = self.domain
                records.append(record)

            batch_max = max(
                (parse_optional_int(e.get("id")) or cursor) for e in batch
            )
            if batch_max <= cursor:
                break  # server is not advancing; avoid an infinite loop
            cursor = batch_max

            latest_id = parse_optional_int(body.get("latest_id"))
            if latest_id is not None and cursor >= latest_id:
                break
            if cap is not None and cursor >= cap:
                break

        end_offset = {"cursor": cursor}
        if start_offset == end_offset:
            return iter([]), start_offset
        return iter(records), end_offset

    # ------------------------------------------------------------------ #
    # Audit reporting v1 — job-based, partitioned by date window
    # ------------------------------------------------------------------ #
    #
    # Table options:
    #   window_days                   Days per report job (default 7).
    #   initial_backfill_days         First-run lookback (default 7).
    #   start_date                    Explicit YYYY-MM-DD first-run start.
    #   audit_page_size               Report rows per fetch (default 100).
    #   audit_poll_interval_seconds   Job poll spacing (default 120).
    #   audit_poll_max_attempts       Poll attempts before giving up (30).
    #   audit_events / audit_folders / audit_users / audit_transaction_types /
    #   audit_assigners / audit_assignee_users / audit_assignee_groups
    #                                 Per-report-type filters (comma lists).
    #
    # Requires an admin or "can run reports" power user — a standard-user
    # token gets 403 here even though it works fine elsewhere.
    #
    # v2 (`/pubapi/v2/audit/stream`) is deliberately NOT used: it only serves
    # the trailing 7 days and carries its own 10/min, 100/hour limit, so it
    # cannot backfill. It stays a future low-latency add-on, not a
    # replacement.

    def _audit_partitions(
        self,
        table_options: dict[str, str],
        start_offset: dict | None,
        end_offset: dict | None,
    ) -> list[dict]:
        """Split into whole-calendar-day report windows.

        Offsets are dates, and ``date_end`` is inclusive, so windows tile the
        range with no overlap and no gap — which matters because audit rows
        are append-only with no server-assigned id to dedup on.
        """
        end_date_str = (end_offset or {}).get("date") or self._audit_cap_date
        end_date = parse_date(end_date_str)  # exclusive

        start_raw = (start_offset or {}).get("date") if start_offset else None
        if not start_raw:
            start_raw = table_options.get("start_date")
        if start_raw:
            start_date = parse_date(start_raw)
        else:
            backfill = parse_int(
                table_options.get("initial_backfill_days"),
                DEFAULT_AUDIT_BACKFILL_DAYS,
                minimum=1,
            )
            start_date = end_date - timedelta(days=backfill)

        if start_date >= end_date:
            return []

        window_days = parse_int(
            table_options.get("window_days"), DEFAULT_AUDIT_WINDOW_DAYS, minimum=1
        )
        partitions: list[dict] = []
        cursor = start_date
        while cursor < end_date:
            window_end = min(cursor + timedelta(days=window_days), end_date)
            partitions.append(
                {
                    "date_start": format_date(cursor),
                    # date_end is inclusive on the API.
                    "date_end": format_date(window_end - timedelta(days=1)),
                }
            )
            cursor = window_end
        return partitions

    def _read_audit_window(
        self, table_name: str, partition: dict, table_options: dict[str, str]
    ) -> Iterator[dict]:
        """Delegate one date window to the audit job lifecycle (egnyte_audit)."""
        yield from read_audit_window(
            self._client,
            self.domain,
            AUDIT_TABLE_TYPES[table_name],
            partition["date_start"],
            partition["date_end"],
            table_options,
        )
