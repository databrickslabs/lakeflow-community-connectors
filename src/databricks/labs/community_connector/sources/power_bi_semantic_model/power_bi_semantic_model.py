"""Power BI semantic model (dataset) connector.

Reads semantic-model *metadata* out of the Power BI REST API:

* ``workspaces``               — ``GET /admin/groups`` (fallback ``GET /groups``)
* ``datasets``                 — ``GET /admin/datasets`` (fallback per-workspace
                                 ``GET /groups/{groupId}/datasets``)
* ``dataset_tables``           \\
* ``dataset_columns``           >  Admin metadata scanner
* ``dataset_measures``         /   (``POST /admin/workspaces/getInfo`` ->
                                   ``GET .../scanStatus/{id}`` ->
                                   ``GET .../scanResult/{id}``)
* ``dataset_refresh_history``  — ``GET /groups/{groupId}/datasets/{id}/refreshes``

...plus one table that reads actual *row data* rather than metadata:

* ``dax_query_result``         — ``POST /groups/{groupId}/datasets/{id}/executeQueries``

``dax_query_result`` is **opt-in and user-defined**: the pipeline supplies a DAX
query (``dax_query``) and the model to run it against (``workspace_id`` +
``dataset_id``), and the connector emits that query's rows.  One configured
query is one table's worth of data — the connector never fans DAX out across
datasets, both because a DAX query is only meaningful against the model it was
written for and because the endpoint's budget is 120 requests/minute for the
entire tenant.  With no ``dax_query`` configured the table simply yields
nothing, so a pipeline that has not opted in is unaffected.

Auth is Entra ID, either as a service principal (OAuth2 client-credentials)
or as a "master user" (Entra ID ROPC), both with
``scope=https://analysis.windows.net/powerbi/api/.default``.

**Why partitioned.**  The Power BI REST API exposes no ``since``/``until``
range filters, so there is no time axis to split on.  It does, however, force
a *fan-out* shape on almost every expensive read: the scanner accepts at most
100 workspace IDs per scan, and refresh history has to be fetched once per
(workspace, dataset) pair.  Those fan-outs are the natural partition axis and
they parallelise cleanly across executors, so this connector implements
``SupportsPartitionedStream`` with entity-batch partitions rather than time
windows.  ``latest_offset`` is pinned to the instance's init time, which is
what makes ``Trigger.AvailableNow`` converge after a single micro-batch.
"""

import time
from typing import Any, Iterator, Sequence

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import (
    LakeflowConnect,
    SupportsPartitionedStream,
)
from databricks.labs.community_connector.sources.power_bi_semantic_model.power_bi_semantic_model_schemas import (  # noqa: E501  pylint: disable=line-too-long
    DATASET_COLUMNS,
    DATASET_MEASURES,
    DATASET_TABLES,
    DATASETS,
    DAX_MAX_ROWS,
    DAX_MAX_VALUES,
    DAX_QUERY_RESULT,
    DEFAULT_DATASETS_PER_PARTITION,
    DEFAULT_MAX_PAGES,
    DEFAULT_PAGE_SIZE,
    DEFAULT_REFRESH_TOP,
    DEFAULT_SCAN_BATCH_SIZE,
    DEFAULT_SCAN_MAX_POLL_SECONDS,
    DEFAULT_SCAN_POLL_SECONDS,
    DEFAULT_SCAN_TIMEOUT_SECONDS,
    DEFAULT_TIMEOUT_SECONDS,
    EPOCH_ISO,
    MAX_ADMIN_PAGE_SIZE,
    MAX_SCAN_WORKSPACES,
    POWER_BI_API_BASE,
    SCANNER_TABLES,
    SNAPSHOT_TABLES,
    STRUCT_FIELDS_BY_TABLE,
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
    WORKSPACES,
    build_dax_query_result_schema,
)
from databricks.labs.community_connector.sources.power_bi_semantic_model.power_bi_semantic_model_utils import (  # noqa: E501  pylint: disable=line-too-long
    PowerBiAdminAccessDenied,
    PowerBiApiError,
    PowerBiClient,
    chunked,
    json_encode,
    none_if_empty,
    parse_bool_option,
    parse_csv_option,
    parse_dax_columns_option,
    parse_int_option,
    parse_iso,
    query_fingerprint,
    stringify,
    utc_now_iso,
)

# Partition descriptor kinds.
_KIND_WORKSPACES = "workspaces"
_KIND_ADMIN_DATASETS = "admin_datasets"
_KIND_WORKSPACE_DATASETS = "workspace_datasets"
_KIND_SCAN = "scan"
_KIND_REFRESHES = "refreshes"
_KIND_DAX_QUERY = "dax_query"


class PowerBiSemanticModelLakeflowConnect(LakeflowConnect, SupportsPartitionedStream):
    """LakeflowConnect implementation for Power BI semantic models.

    Auth: Power BI's own docs describe two ways to call its REST API, both
    supported here (``client_secret`` takes precedence when both are given):
        service_principal   tenant_id, client_id, client_secret — OAuth 2.0
                             client-credentials (app-only). Preferred: no
                             user account required, works with MFA-enforced
                             tenants.
        user                tenant_id, client_id, username, password — the
                             classic "master user" pattern via Entra ID's
                             Resource Owner Password Credentials (ROPC)
                             grant. Microsoft treats ROPC as legacy; the
                             account must not have MFA enabled.

    Required connection options:
        tenant_id       Entra ID (Azure AD) tenant ID
        client_id       Application (client) ID of the app registration
        client_secret   Client secret (service_principal method)
        username        Power BI user's UPN (user method)
        password        Power BI user's password (user method)

    Optional connection options:
        use_admin_api   "true" (default) to prefer the tenant-wide ``/admin``
                        endpoints; "false" forces the membership-scoped ones.
        base_url        Override the API root (default
                        ``https://api.powerbi.com/v1.0/myorg``)
        timeout_seconds Per-request timeout (default 60)

    Per-table options (all optional):
        workspace_ids           Comma-separated allow-list of workspace IDs.
                                Scopes every fan-out; strongly recommended for
                                tests and for large tenants.
        dataset_ids             Comma-separated allow-list of dataset IDs.
        workspace_filter        OData ``$filter`` passed to /admin/groups,
                                e.g. ``state eq 'Active'``.
        page_size               ``$top`` used when paging Admin list endpoints
                                (default 1000, max 5000).
        max_pages               Safety cap on Admin list paging (default 50).
        scan_batch_size         Workspace IDs per metadata scan (default/max 100).
        datasets_per_partition  Datasets per refresh-history partition (default 20).
        top                     ``$top`` on the refreshes endpoint (default 60).
        max_records_per_batch   Caps a single non-partitioned ``read_table``
                                micro-batch for ``dataset_refresh_history``.
        scan_poll_seconds       Initial scanner poll interval (default 3).
        scan_timeout_seconds    Scanner poll timeout (default 600).

    Options specific to ``dax_query_result``:
        dax_query               The DAX ``EVALUATE`` statement to run.  Required
                                for this table to emit anything; without it the
                                table stays empty.
        workspace_id            Workspace (group) holding the model.  Required
                                once ``dax_query`` is set; falls back to
                                ``workspace_ids`` when that names exactly one.
        dataset_id              Semantic model to query.  Required once
                                ``dax_query`` is set; falls back to
                                ``dataset_ids`` when that names exactly one.
        dax_columns             JSON array declaring the query's columns, e.g.
                                ``[{"dax": "Sales[Region]", "name": "region",
                                "type": "string"}]``.  Turns the result into
                                properly typed Spark columns; omit it to get the
                                ``columns`` string map instead.
        include_nulls           "true" (default) sends
                                ``serializerSettings.includeNulls``, so BLANK()
                                cells arrive as JSON nulls rather than being
                                dropped from the row object.
        impersonated_user_name  UPN to evaluate the query as, for models with
                                row-level security.
    """

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)

        self._base_url = str(options.get("base_url") or POWER_BI_API_BASE).rstrip("/")
        self._use_admin_api = parse_bool_option(options, "use_admin_api", True)
        self._client = PowerBiClient(
            tenant_id=options.get("tenant_id"),
            client_id=options.get("client_id"),
            client_secret=options.get("client_secret"),
            username=options.get("username"),
            password=options.get("password"),
            timeout=parse_int_option(
                options, "timeout_seconds", DEFAULT_TIMEOUT_SECONDS, minimum=1
            ),
        )

        # Pin every offset this instance hands back to its construction time.
        # Trigger.AvailableNow terminates when latest_offset stops moving, and
        # since none of these endpoints accept a server-side time filter, an
        # instance-scoped cap is the only thing that makes it stop.  The next
        # trigger builds a fresh instance with a newer cap.
        self._init_time = utc_now_iso()

        # Driver-side caches — the workspace/dataset enumerations feed
        # get_partitions and would otherwise burn through the Admin API's
        # 50-requests-per-hour budget.
        self._workspace_ids_cache: list[str] | None = None
        self._dataset_pairs_cache: list[list[str]] | None = None

        # Snapshot tables carry no cursor, so their offset is always {} and the
        # framework can't tell "already emitted this trigger" from "resuming a
        # new trigger". Instance state can: a new trigger builds a new
        # connector and re-emits the snapshot.
        self._snapshot_emitted: set[str] = set()

    # ------------------------------------------------------------------
    # LakeflowConnect — discovery
    # ------------------------------------------------------------------

    def list_tables(self) -> list[str]:
        return list(SUPPORTED_TABLES)

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        self._validate_table(table_name)
        if table_name == DAX_QUERY_RESULT:
            # The only table whose columns come from configuration rather than
            # the API — see the schema module for why they can't be discovered.
            return build_dax_query_result_schema(parse_dax_columns_option(table_options or {}))
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        self._validate_table(table_name)
        return dict(TABLE_METADATA[table_name])

    # ------------------------------------------------------------------
    # SupportsPartitionedStream
    # ------------------------------------------------------------------

    def is_partitioned(self, table_name: str) -> bool:
        return table_name in SUPPORTED_TABLES

    def latest_offset(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict | None = None,
    ) -> dict:
        """Return the init-time cap.

        Metadata-only call by design: none of the Power BI list endpoints
        expose a high-water mark, and probing one would cost a request against
        a 50/hour budget for no benefit.  Returning a constant makes the second
        call match the first, which is exactly the "no more data" signal
        Trigger.AvailableNow waits for.
        """
        self._validate_table(table_name)
        return {"cursor": self._init_time}

    def get_partitions(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict | None = None,
        end_offset: dict | None = None,
    ) -> Sequence[dict]:
        """Split a table's fan-out into executor-sized units of work."""
        self._validate_table(table_name)
        table_options = table_options or {}

        start_cursor = (start_offset or {}).get("cursor")
        end_cursor = (end_offset or {}).get("cursor") or self._init_time

        if start_cursor and not self._cursor_lt(start_cursor, end_cursor):
            return []

        if table_name == WORKSPACES:
            return [{"kind": _KIND_WORKSPACES}]

        if table_name == DAX_QUERY_RESULT:
            # executeQueries is one POST returning one result set — it is not
            # pageable and takes no range filter, so there is nothing to split.
            # No configured query means no work at all.
            if not str(table_options.get("dax_query") or "").strip():
                return []
            return [{"kind": _KIND_DAX_QUERY}]

        if table_name == DATASETS:
            if self._use_admin(table_options):
                return [{"kind": _KIND_ADMIN_DATASETS}]
            batch = parse_int_option(table_options, "workspaces_per_partition", 10, minimum=1)
            return [
                {"kind": _KIND_WORKSPACE_DATASETS, "workspace_ids": chunk}
                for chunk in chunked(self._list_workspace_ids(table_options), batch)
            ]

        if table_name in SCANNER_TABLES:
            batch = parse_int_option(
                table_options,
                "scan_batch_size",
                DEFAULT_SCAN_BATCH_SIZE,
                minimum=1,
                maximum=MAX_SCAN_WORKSPACES,
            )
            return [
                {"kind": _KIND_SCAN, "workspace_ids": chunk}
                for chunk in chunked(self._list_workspace_ids(table_options), batch)
            ]

        # dataset_refresh_history — fan out over (workspace, dataset) pairs and
        # carry the (since, until] bounds so each executor filters identically.
        batch = parse_int_option(
            table_options,
            "datasets_per_partition",
            DEFAULT_DATASETS_PER_PARTITION,
            minimum=1,
        )
        return [
            {
                "kind": _KIND_REFRESHES,
                "pairs": chunk,
                "since": start_cursor,
                "until": end_cursor,
            }
            for chunk in chunked(self._list_dataset_pairs(table_options), batch)
        ]

    def read_partition(
        self,
        table_name: str,
        partition: dict,
        table_options: dict[str, str],
    ) -> Iterator[dict]:
        """Read one partition.  Runs on an executor — no driver state used."""
        self._validate_table(table_name)
        table_options = table_options or {}
        kind = partition.get("kind")

        if kind == _KIND_WORKSPACES:
            yield from self._fetch_workspaces(table_options)
        elif kind == _KIND_ADMIN_DATASETS:
            yield from self._fetch_datasets_admin(table_options)
        elif kind == _KIND_WORKSPACE_DATASETS:
            # Dedupe within the partition: a dataset visible from two
            # workspaces would otherwise break the ``id`` primary key.
            seen: set[str] = set()
            for workspace_id in partition.get("workspace_ids") or []:
                for row in self._fetch_datasets_for_workspace(workspace_id, table_options):
                    dataset_id = row.get("id")
                    if dataset_id in seen:
                        continue
                    seen.add(dataset_id)
                    yield row
        elif kind == _KIND_SCAN:
            yield from self._scan_and_flatten(
                table_name, partition.get("workspace_ids") or [], table_options
            )
        elif kind == _KIND_DAX_QUERY:
            yield from self._fetch_dax_rows(table_options)
        elif kind == _KIND_REFRESHES:
            since = partition.get("since")
            until = partition.get("until") or self._init_time
            for pair in partition.get("pairs") or []:
                workspace_id, dataset_id = pair[0], pair[1]
                yield from self._fetch_refreshes(
                    workspace_id, dataset_id, since, until, table_options
                )
        else:
            raise ValueError(f"Unknown partition kind {kind!r} for table {table_name!r}")

    # ------------------------------------------------------------------
    # LakeflowConnect.read_table — single-driver fallback
    # ------------------------------------------------------------------

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        self._validate_table(table_name)
        table_options = table_options or {}

        if table_name in SNAPSHOT_TABLES:
            if table_name in self._snapshot_emitted:
                # Second call within the same trigger: an empty offset that
                # matches start_offset ends the micro-batch loop.
                return iter([]), {}
            records: list[dict] = []
            for partition in self.get_partitions(table_name, table_options):
                records.extend(self.read_partition(table_name, partition, table_options))
            self._snapshot_emitted.add(table_name)
            return iter(records), {}

        return self._read_refresh_history(start_offset, table_options)

    def _read_refresh_history(
        self, start_offset: dict | None, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Append-only read of refresh history, resumable mid fan-out.

        The API has no ``since`` filter, so the (since, until] bound is applied
        client-side on ``startTime``.  Batches are bounded by *datasets*, never
        by truncating a dataset's response — truncation would duplicate rows on
        an append-only table, since the next call would re-read the same
        ``$top`` window.
        """
        start_offset = start_offset or {}
        since = start_offset.get("cursor")
        try:
            index = int(start_offset.get("dataset_index", 0) or 0)
        except (TypeError, ValueError):
            index = 0

        until = self._init_time
        if index == 0 and since and not self._cursor_lt(since, until):
            return iter([]), start_offset

        pairs = self._list_dataset_pairs(table_options)
        max_records = parse_int_option(table_options, "max_records_per_batch", 1000, minimum=1)

        records: list[dict] = []
        cursor_index = min(index, len(pairs))
        while cursor_index < len(pairs) and len(records) < max_records:
            workspace_id, dataset_id = pairs[cursor_index]
            records.extend(
                self._fetch_refreshes(workspace_id, dataset_id, since, until, table_options)
            )
            cursor_index += 1

        if cursor_index >= len(pairs):
            end_offset = {"cursor": until, "dataset_index": 0}
        else:
            # Mid fan-out: keep the same window and resume at the next dataset.
            end_offset = {
                "cursor": since or EPOCH_ISO,
                "dataset_index": cursor_index,
            }

        if start_offset and end_offset == start_offset:
            return iter([]), start_offset
        return iter(records), end_offset

    # ------------------------------------------------------------------
    # workspaces
    # ------------------------------------------------------------------

    def _fetch_workspaces(self, table_options: dict[str, str]) -> list[dict]:
        allow = set(parse_csv_option(table_options, "workspace_ids"))

        rows: list[dict] | None = None
        if self._use_admin(table_options):
            params: dict[str, str] = {}
            workspace_filter = table_options.get("workspace_filter")
            if workspace_filter:
                params["$filter"] = workspace_filter
            try:
                rows = self._paginate_list(f"{self._base_url}/admin/groups", table_options, params)
            except PowerBiAdminAccessDenied:
                # Service principal not enabled for the Admin APIs — fall back
                # to the workspaces it has been added to as a member.
                rows = None

        if rows is None:
            rows = self._paginate_list(f"{self._base_url}/groups", table_options)

        if allow:
            rows = [row for row in rows if row.get("id") in allow]

        deduped: list[dict] = []
        seen: set[str] = set()
        for row in rows:
            workspace_id = row.get("id")
            if not workspace_id or workspace_id in seen:
                continue
            seen.add(workspace_id)
            deduped.append(row)
        return deduped

    def _list_workspace_ids(self, table_options: dict[str, str]) -> list[str]:
        """Enumerate workspace IDs once per connector instance."""
        explicit = parse_csv_option(table_options, "workspace_ids")
        if explicit:
            return explicit
        if self._workspace_ids_cache is None:
            self._workspace_ids_cache = [
                row["id"] for row in self._fetch_workspaces(table_options) if row.get("id")
            ]
        return list(self._workspace_ids_cache)

    # ------------------------------------------------------------------
    # datasets
    # ------------------------------------------------------------------

    def _fetch_datasets_admin(self, table_options: dict[str, str]) -> list[dict]:
        try:
            rows = self._paginate_list(f"{self._base_url}/admin/datasets", table_options)
        except PowerBiAdminAccessDenied:
            rows = []
            seen: set[str] = set()
            for workspace_id in self._list_workspace_ids(table_options):
                for row in self._fetch_datasets_for_workspace(workspace_id, table_options):
                    dataset_id = row.get("id")
                    if dataset_id in seen:
                        continue
                    seen.add(dataset_id)
                    rows.append(row)
        return self._filter_datasets(rows, table_options)

    def _fetch_datasets_for_workspace(
        self, workspace_id: str, table_options: dict[str, str]
    ) -> list[dict]:
        body = self._client.get(f"{self._base_url}/groups/{workspace_id}/datasets")
        rows = []
        for raw in body.get("value") or []:
            row = dict(raw)
            # The non-admin endpoint does not echo the workspace back.
            row.setdefault("workspaceId", workspace_id)
            rows.append(row)
        return self._filter_datasets(rows, table_options)

    def _filter_datasets(self, rows: list[dict], table_options: dict[str, str]) -> list[dict]:
        workspace_allow = set(parse_csv_option(table_options, "workspace_ids"))
        dataset_allow = set(parse_csv_option(table_options, "dataset_ids"))

        out = []
        for raw in rows:
            if workspace_allow and raw.get("workspaceId") not in workspace_allow:
                continue
            if dataset_allow and raw.get("id") not in dataset_allow:
                continue
            row = dict(raw)
            for field in STRUCT_FIELDS_BY_TABLE.get(DATASETS, ()):
                if field in row:
                    row[field] = none_if_empty(row[field])
            out.append(row)
        return out

    def _list_dataset_pairs(self, table_options: dict[str, str]) -> list[list[str]]:
        """Enumerate ``[workspace_id, dataset_id]`` pairs once per instance."""
        if self._dataset_pairs_cache is not None:
            return list(self._dataset_pairs_cache)

        if self._use_admin(table_options):
            rows = self._fetch_datasets_admin(table_options)
        else:
            rows = []
            for workspace_id in self._list_workspace_ids(table_options):
                rows.extend(self._fetch_datasets_for_workspace(workspace_id, table_options))

        pairs: list[list[str]] = []
        seen: set[tuple[str, str]] = set()
        for row in rows:
            workspace_id, dataset_id = row.get("workspaceId"), row.get("id")
            if not workspace_id or not dataset_id:
                continue
            key = (workspace_id, dataset_id)
            if key in seen:
                continue
            seen.add(key)
            pairs.append([workspace_id, dataset_id])

        self._dataset_pairs_cache = pairs
        return list(pairs)

    # ------------------------------------------------------------------
    # scanner API (dataset_tables / dataset_columns / dataset_measures)
    # ------------------------------------------------------------------

    def _scan_and_flatten(
        self,
        table_name: str,
        workspace_ids: list[str],
        table_options: dict[str, str],
    ) -> Iterator[dict]:
        if not workspace_ids:
            return
        scan_result = self._run_scan(workspace_ids, table_options)
        yield from self._flatten_scan_result(table_name, scan_result)

    def _run_scan(self, workspace_ids: list[str], table_options: dict[str, str]) -> dict:
        """Drive the three-call async scanner workflow to completion.

        ``getInfo`` returns 202 with a scan ID; the scan then runs server-side
        for anywhere between a second and several minutes, so ``scanStatus``
        has to be polled until it reports ``Succeeded``.  Only then does
        ``scanResult`` hold the metadata tree (and only for 24 hours after).
        """
        trigger = self._client.post(
            f"{self._base_url}/admin/workspaces/getInfo",
            params={
                "lineage": "True",
                "datasourceDetails": "True",
                # Both flags additionally require the tenant's metadata-scanning
                # settings to be enabled, otherwise the scan succeeds but the
                # schema arrays come back empty.
                "datasetSchema": "True",
                "datasetExpressions": "True",
            },
            json_body={"workspaces": list(workspace_ids)},
        )
        scan_id = trigger.get("id")
        if not scan_id:
            raise PowerBiApiError(f"POST /admin/workspaces/getInfo returned no scan id: {trigger}")

        self._await_scan(scan_id, table_options)
        return self._client.get(f"{self._base_url}/admin/workspaces/scanResult/{scan_id}")

    def _await_scan(self, scan_id: str, table_options: dict[str, str]) -> None:
        poll_seconds = parse_int_option(
            table_options, "scan_poll_seconds", DEFAULT_SCAN_POLL_SECONDS, minimum=0
        )
        timeout_seconds = parse_int_option(
            table_options,
            "scan_timeout_seconds",
            DEFAULT_SCAN_TIMEOUT_SECONDS,
            minimum=1,
        )

        deadline = time.monotonic() + timeout_seconds
        delay = poll_seconds
        while True:
            status_body = self._client.get(
                f"{self._base_url}/admin/workspaces/scanStatus/{scan_id}"
            )
            status = str(status_body.get("status") or "")
            if status.lower() == "succeeded":
                return
            if status.lower() == "failed":
                raise PowerBiApiError(f"Metadata scan {scan_id} failed: {status_body}")
            if time.monotonic() >= deadline:
                raise PowerBiApiError(
                    f"Metadata scan {scan_id} did not complete within "
                    f"{timeout_seconds}s (last status: {status or 'unknown'})"
                )
            if delay:
                time.sleep(delay)
                delay = min(delay * 2, DEFAULT_SCAN_MAX_POLL_SECONDS)

    def _flatten_scan_result(self, table_name: str, scan_result: dict) -> Iterator[dict]:
        """Turn the nested scan tree into flat rows for one of the three tables."""
        for workspace in scan_result.get("workspaces") or []:
            workspace_id = workspace.get("id")
            for dataset in workspace.get("datasets") or []:
                dataset_id = dataset.get("id")
                if not dataset_id:
                    continue
                for table in dataset.get("tables") or []:
                    table_label = table.get("name")
                    if not table_label:
                        continue
                    if table_name == DATASET_TABLES:
                        yield {
                            "workspace_id": workspace_id,
                            "dataset_id": dataset_id,
                            "name": table_label,
                            "isHidden": table.get("isHidden"),
                            "description": table.get("description"),
                            "source": self._shape_table_source(table.get("source")),
                        }
                    elif table_name == DATASET_COLUMNS:
                        yield from self._flatten_columns(
                            table, workspace_id, dataset_id, table_label
                        )
                    elif table_name == DATASET_MEASURES:
                        yield from self._flatten_measures(
                            table, workspace_id, dataset_id, table_label
                        )

    @staticmethod
    def _flatten_columns(
        table: dict, workspace_id: str, dataset_id: str, table_label: str
    ) -> Iterator[dict]:
        for column in table.get("columns") or []:
            if not column.get("name"):
                continue
            yield {
                "workspace_id": workspace_id,
                "dataset_id": dataset_id,
                "table_name": table_label,
                "name": column.get("name"),
                "dataType": column.get("dataType"),
                "dataCategory": column.get("dataCategory"),
                "formatString": column.get("formatString"),
                "isHidden": column.get("isHidden"),
                "sortByColumn": column.get("sortByColumn"),
                "summarizeBy": column.get("summarizeBy"),
            }

    @staticmethod
    def _flatten_measures(
        table: dict, workspace_id: str, dataset_id: str, table_label: str
    ) -> Iterator[dict]:
        for measure in table.get("measures") or []:
            if not measure.get("name"):
                continue
            yield {
                "workspace_id": workspace_id,
                "dataset_id": dataset_id,
                "table_name": table_label,
                "name": measure.get("name"),
                "expression": measure.get("expression"),
                "description": measure.get("description"),
                "formatString": measure.get("formatString"),
                "isHidden": measure.get("isHidden"),
            }

    @staticmethod
    def _shape_table_source(source: Any) -> list[dict] | None:
        """Keep only well-formed ``{expression: ...}`` entries.

        An empty dict inside a StructType array is rejected by the framework's
        type coercion, so anything without an expression is dropped.
        """
        if not isinstance(source, list):
            return None
        shaped = [
            {"expression": entry.get("expression")}
            for entry in source
            if isinstance(entry, dict) and entry
        ]
        return shaped or None

    # ------------------------------------------------------------------
    # refresh history
    # ------------------------------------------------------------------

    def _fetch_refreshes(
        self,
        workspace_id: str,
        dataset_id: str,
        since: str | None,
        until: str | None,
        table_options: dict[str, str],
    ) -> list[dict]:
        top = parse_int_option(table_options, "top", DEFAULT_REFRESH_TOP, minimum=1)
        body = self._client.get(
            f"{self._base_url}/groups/{workspace_id}/datasets/{dataset_id}/refreshes",
            params={"$top": str(top)},
        )

        since_dt = parse_iso(since)
        until_dt = parse_iso(until)

        rows = []
        for raw in body.get("value") or []:
            started = parse_iso(raw.get("startTime"))
            if started is None:
                # No usable cursor: emit it only on an unbounded first read so
                # an append-only table can never see it twice.
                if since_dt is not None:
                    continue
            else:
                if since_dt is not None and started <= since_dt:
                    continue
                if until_dt is not None and started > until_dt:
                    continue

            row = dict(raw)
            row["workspace_id"] = workspace_id
            row["dataset_id"] = dataset_id
            row["refreshAttempts"] = self._shape_refresh_attempts(raw.get("refreshAttempts"))
            rows.append(row)
        return rows

    @staticmethod
    def _shape_refresh_attempts(attempts: Any) -> list[dict] | None:
        if not attempts:
            return None
        shaped = []
        for attempt in attempts:
            if not isinstance(attempt, dict):
                continue
            shaped.append(
                {
                    "attemptId": attempt.get("attemptId"),
                    "startTime": attempt.get("startTime"),
                    "endTime": attempt.get("endTime"),
                    "type": attempt.get("type"),
                    "serviceExceptionJson": attempt.get("serviceExceptionJson"),
                    "executionMetrics": json_encode(attempt.get("executionMetrics")),
                }
            )
        return shaped or None

    # ------------------------------------------------------------------
    # dax_query_result (executeQueries)
    # ------------------------------------------------------------------

    def _fetch_dax_rows(self, table_options: dict[str, str]) -> Iterator[dict]:
        """Execute the configured DAX query and emit its rows.

        Exactly one POST.  The endpoint is not pageable, accepts no range
        filter, and costs against a 120-requests-per-minute *tenant-wide*
        budget, so it is deliberately called once per micro-batch and never
        fanned out; the shared Retry-After/backoff handling in the client covers
        the 429 case.
        """
        query = str(table_options.get("dax_query") or "").strip()
        if not query:
            # Table not opted into. Emitting nothing beats raising: the other
            # six tables in the same pipeline should not fail over a table this
            # pipeline never configured.
            return

        workspace_id = self._resolve_single_id(table_options, "workspace_id", "workspace_ids")
        dataset_id = self._resolve_single_id(table_options, "dataset_id", "dataset_ids")

        body: dict[str, Any] = {
            "queries": [{"query": query}],
            "serializerSettings": {
                "includeNulls": parse_bool_option(table_options, "include_nulls", True)
            },
        }
        impersonated = str(table_options.get("impersonated_user_name") or "").strip()
        if impersonated:
            # Required for models with row-level security; without it a service
            # principal evaluates the query under its own (unfiltered or
            # unauthorised) identity.
            body["impersonatedUserName"] = impersonated

        url = f"{self._base_url}/groups/{workspace_id}/datasets/{dataset_id}/executeQueries"
        try:
            response = self._client.post(url, json_body=body)
        except PowerBiAdminAccessDenied as exc:
            # Not an admin endpoint, so the client's admin-fallback exception is
            # the wrong signal here — there is nothing to fall back to. Re-raise
            # with the causes that actually apply to executeQueries.
            raise PowerBiApiError(
                f"executeQueries was rejected for dataset {dataset_id} in "
                f"workspace {workspace_id}. Check that the 'Dataset Execute "
                f"Queries REST API' tenant setting is enabled, that the service "
                f"principal has at least read access to the model, and note that "
                f"the endpoint is unsupported for Azure Analysis Services-hosted "
                f"models and for RLS/SSO-enabled models under service-principal "
                f"auth. Underlying error: {exc}"
            ) from exc

        rows = self._extract_dax_rows(response)
        truncated = self._looks_truncated(rows)
        column_specs = parse_dax_columns_option(table_options)
        query_hash = query_fingerprint(query)
        ingested_at = utc_now_iso()

        for index, raw in enumerate(rows):
            record = {
                "workspace_id": workspace_id,
                "dataset_id": dataset_id,
                "query_hash": query_hash,
                "row_index": index,
                "row_json": json_encode(raw),
                "truncated": truncated,
                "ingestion_timestamp": ingested_at,
            }
            if column_specs:
                for spec in column_specs:
                    # Absent key and JSON null both land as None. A DAX column
                    # the user named but the query never returned is a config
                    # error, but nulling it keeps the batch readable rather than
                    # failing every row.
                    record[spec["name"]] = raw.get(spec["dax"])
            else:
                record["columns"] = {str(key): stringify(value) for key, value in raw.items()}
            yield record

    @staticmethod
    def _extract_dax_rows(response: dict) -> list[dict]:
        """Pull the row objects out of the executeQueries envelope.

        Shape is ``results[].tables[].rows[]``. Only one query is ever sent, so
        this is normally a single result with a single table, but the response
        is walked generically. Per-result/per-table ``error`` objects are raised
        rather than silently yielding an empty table.
        """
        error = response.get("error")
        if error:
            raise PowerBiApiError(f"executeQueries returned an error: {error}")

        rows: list[dict] = []
        for result in response.get("results") or []:
            if not isinstance(result, dict):
                continue
            if result.get("error"):
                raise PowerBiApiError(f"executeQueries returned an error: {result['error']}")
            for table in result.get("tables") or []:
                if not isinstance(table, dict):
                    continue
                if table.get("error"):
                    raise PowerBiApiError(f"executeQueries returned an error: {table['error']}")
                rows.extend(row for row in (table.get("rows") or []) if isinstance(row, dict))
        return rows

    @staticmethod
    def _looks_truncated(rows: list[dict]) -> bool:
        """Best-effort truncation detection.

        The API silently caps a result at 100,000 rows or 1,000,000 total values
        (and 15 MB of response body) and reports it as a warning, not an error,
        so a truncated result is indistinguishable from a complete one by shape
        alone. Landing on either countable ceiling is treated as truncated.
        """
        if len(rows) >= DAX_MAX_ROWS:
            return True
        return sum(len(row) for row in rows) >= DAX_MAX_VALUES

    @staticmethod
    def _resolve_single_id(table_options: dict[str, str], singular: str, plural: str) -> str:
        """Resolve ``workspace_id`` / ``dataset_id`` for the DAX table.

        Falls back to the connector's existing comma-separated allow-list option
        when it names exactly one value, so a pipeline already scoping tables
        with ``dataset_ids`` does not have to repeat itself. Ambiguity is an
        error: one DAX query is only meaningful against one model.
        """
        value = str(table_options.get(singular) or "").strip()
        if value:
            return value

        candidates = parse_csv_option(table_options, plural)
        if len(candidates) == 1:
            return candidates[0]

        if candidates:
            raise ValueError(
                f"Table option '{plural}' names {len(candidates)} values, so "
                f"'{singular}' cannot be inferred. Set '{singular}' explicitly "
                f"on the {DAX_QUERY_RESULT} table — one DAX query runs against "
                f"exactly one semantic model."
            )
        raise ValueError(
            f"Table option '{singular}' is required on the {DAX_QUERY_RESULT} "
            f"table whenever 'dax_query' is set."
        )

    # ------------------------------------------------------------------
    # shared helpers
    # ------------------------------------------------------------------

    def _paginate_list(
        self,
        url: str,
        table_options: dict[str, str],
        extra_params: dict[str, str] | None = None,
    ) -> list[dict]:
        """Page an OData list endpoint via ``$top``/``$skip``.

        The Admin list endpoints make ``$top`` mandatory and cap it at 5000;
        paging stops as soon as a page comes back short, with ``max_pages`` as
        a backstop against a server that ignores ``$skip``.
        """
        page_size = parse_int_option(
            table_options,
            "page_size",
            DEFAULT_PAGE_SIZE,
            minimum=1,
            maximum=MAX_ADMIN_PAGE_SIZE,
        )
        max_pages = parse_int_option(table_options, "max_pages", DEFAULT_MAX_PAGES, minimum=1)

        rows: list[dict] = []
        skip = 0
        for _ in range(max_pages):
            params = dict(extra_params or {})
            params["$top"] = str(page_size)
            params["$skip"] = str(skip)
            body = self._client.get(url, params=params)
            page = body.get("value") or []
            rows.extend(page)
            if len(page) < page_size:
                break
            skip += page_size
        return rows

    def _use_admin(self, table_options: dict[str, str]) -> bool:
        return parse_bool_option(table_options, "use_admin_api", self._use_admin_api)

    @staticmethod
    def _cursor_lt(left: str, right: str) -> bool:
        """Compare two ISO cursors, tolerating differing precision/offsets."""
        left_dt = parse_iso(left)
        right_dt = parse_iso(right)
        if left_dt is None or right_dt is None:
            return str(left) < str(right)
        return left_dt < right_dt

    def _validate_table(self, table_name: str) -> None:
        if table_name not in SUPPORTED_TABLES:
            raise ValueError(
                f"Table '{table_name}' is not supported. Supported tables: {SUPPORTED_TABLES}"
            )
