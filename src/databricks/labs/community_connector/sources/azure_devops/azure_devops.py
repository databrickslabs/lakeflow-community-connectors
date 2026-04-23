import base64
import json
import logging
from typing import Any, Iterator

import requests
from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.azure_devops.azure_devops_schemas import (
    TABLE_SCHEMAS,
    TABLE_METADATA,
    SUPPORTED_TABLES,
)
from databricks.labs.community_connector.sources.azure_devops.azure_devops_utils import (
    api_get,
    api_get_list,
    request_with_retry,
    resolve_projects,
    fetch_repos,
    fetch_prs,
    nullify_empty,
    for_each_pr,
)


_LOG = logging.getLogger(__name__)


class AzureDevopsLakeflowConnect(LakeflowConnect):
    def __init__(self, options: dict[str, str]) -> None:
        """Initialize the Azure DevOps connector.

        Expected options:
            - organization: Azure DevOps organization name.
            - project: Project name or ID (optional).
            - personal_access_token: PAT for authentication.
        """
        organization = options.get("organization")
        project = options.get("project")
        personal_access_token = options.get("personal_access_token")

        if not organization:
            raise ValueError(
                "Azure DevOps connector requires 'organization'"
            )
        if not personal_access_token:
            raise ValueError(
                "Azure DevOps connector requires 'personal_access_token'"
            )

        self.organization = organization
        self.project = project
        self.base_url = f"https://dev.azure.com/{organization}"
        self.vssps_base_url = (
            f"https://vssps.dev.azure.com/{organization}"
        )

        auth_b64 = base64.b64encode(
            f":{personal_access_token}".encode("ascii")
        ).decode("ascii")

        self._session = requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Basic {auth_b64}",
                "Accept": "application/json",
            }
        )

    # ------------------------------------------------------------------ #
    # Interface methods
    # ------------------------------------------------------------------ #

    def list_tables(self) -> list[str]:
        return SUPPORTED_TABLES

    def get_table_schema(
        self, table_name: str, table_options: dict[str, str]
    ) -> StructType:
        if table_name not in TABLE_SCHEMAS:
            raise ValueError(f"Unsupported table: {table_name!r}")
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(
        self, table_name: str, table_options: dict[str, str]
    ) -> dict:
        if table_name not in TABLE_METADATA:
            raise ValueError(f"Unsupported table: {table_name!r}")
        return TABLE_METADATA[table_name]

    def read_table(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        dispatch = {
            "projects": self._read_projects,
            "repositories": self._read_repositories,
            "commits": self._read_commits,
            "pullrequests": self._read_pullrequests,
            "refs": self._read_refs,
            "pushes": self._read_pushes,
            "users": self._read_users,
            "pullrequest_threads": self._read_pullrequest_threads,
            "pullrequest_workitems": self._read_pr_workitems,
            "pullrequest_commits": self._read_pr_commits,
            "pullrequest_reviewers": self._read_pr_reviewers,
            "workitems": self._read_workitems,
            "workitem_revisions": self._read_workitem_revisions,
            "workitem_types": self._read_workitem_types,
        }
        handler = dispatch.get(table_name)
        if handler is None:
            raise ValueError(f"Unsupported table: {table_name!r}")
        return handler(start_offset, table_options)

    # ------------------------------------------------------------------ #
    # Helper: resolve project
    # ------------------------------------------------------------------ #

    def _resolve_project(
        self, table_options: dict[str, str]
    ) -> str | None:
        return table_options.get("project") or self.project

    def _resolve_projects(
        self, table_options: dict[str, str]
    ) -> list[str]:
        return resolve_projects(
            self._session,
            self.base_url,
            self._resolve_project(table_options),
        )

    # ------------------------------------------------------------------ #
    # Helper: iterate over all repos
    # ------------------------------------------------------------------ #

    def _for_all_repos(
        self,
        start_offset: dict,
        table_options: dict[str, str],
        reader,
    ) -> tuple[Iterator[dict], dict]:
        """Call *reader* for every repo across projects."""
        repos_iter, _ = self._read_repositories({}, {})
        all_records: list[dict[str, Any]] = []
        for repo in repos_iter:
            repo_id = repo.get("id")
            proj = repo.get("project_name")
            if not repo_id or not proj:
                continue
            opts = {**table_options, "repository_id": repo_id, "project": proj}
            try:
                records_iter, _ = reader(start_offset, opts)
                all_records.extend(records_iter)
            except RuntimeError as exc:
                # api_get raises RuntimeError on non-200 responses;
                # log and continue so one repo doesn't fail the batch.
                _LOG.warning(
                    "Skipping %s/%s for %s: %s",
                    proj, repo_id, reader.__name__, exc,
                )
                continue
        return iter(all_records), {}

    # ------------------------------------------------------------------ #
    # Table readers
    # ------------------------------------------------------------------ #

    def _read_projects(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        projects = api_get_list(
            self._session,
            f"{self.base_url}/_apis/projects",
            {"api-version": "7.1"},
            "projects",
        )
        records = []
        for p in projects:
            rec = dict(p)
            rec["organization"] = self.organization
            records.append(rec)
        return iter(records), {}

    def _read_repositories(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        project = self._resolve_project(table_options)
        if not project:
            return self._read_repos_all_projects(table_options)

        repos = api_get_list(
            self._session,
            f"{self.base_url}/{project}/_apis/git/repositories",
            {
                "api-version": "7.1",
                "includeLinks": "true",
                "includeAllUrls": "true",
            },
            "repositories",
        )
        records = []
        for r in repos:
            rec = dict(r)
            rec["organization"] = self.organization
            rec["project_name"] = project
            nullify_empty(rec, "parentRepository", "project", "_links")
            records.append(rec)
        return iter(records), {}

    def _read_repos_all_projects(
        self, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        projects_iter, _ = self._read_projects({}, {})
        all_records: list[dict[str, Any]] = []
        for p in projects_iter:
            name = p.get("name")
            if not name:
                continue
            try:
                opts = {**table_options, "project": name}
                it, _ = self._read_repositories({}, opts)
                all_records.extend(it)
            except RuntimeError as exc:
                # Projects without Git enabled return 404 — log and
                # continue so one bad project doesn't fail the batch.
                _LOG.warning(
                    "Skipping repositories for project %s: %s", name, exc,
                )
                continue
        return iter(all_records), {}

    # -- Git objects (commits, PRs, refs, pushes) ---------------------- #

    def _read_commits(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read commits incrementally with per-repo committer-date watermarks.

        Uses ADO's ``searchCriteria.fromDate`` (timestamp-precision,
        inclusive) for server-side filtering. A client-side strict
        ``date > watermark`` check prevents re-emitting the boundary
        commit on the next run (append tables don't de-duplicate on PK).
        """
        start_offset = start_offset or {}
        watermarks: dict[str, str] = dict(start_offset.get("watermarks", {}))

        repo_pairs = self._resolve_repo_pairs(table_options)
        all_records: list[dict[str, Any]] = []

        for project, repo_id in repo_pairs:
            key = f"{project}/{repo_id}"
            since = watermarks.get(key)
            records, max_date = self._fetch_commits(project, repo_id, since)
            all_records.extend(records)
            if max_date and (not since or max_date > since):
                watermarks[key] = max_date

        return iter(all_records), {"watermarks": watermarks}

    def _fetch_commits(
        self, project: str, repo_id: str, since: str | None
    ) -> tuple[list[dict[str, Any]], str | None]:
        """Fetch all commits for a single repo since *since* (exclusive).

        Paginates via ``$skip``. Applies client-side ``> since`` to make
        the ADO server-side ``>=`` filter strict, so we don't re-emit
        the boundary commit.
        """
        url = (
            f"{self.base_url}/{project}/_apis/git/repositories"
            f"/{repo_id}/commits"
        )
        top = 1000
        skip = 0
        max_date: str | None = since
        records: list[dict[str, Any]] = []

        while True:
            params: dict[str, str] = {
                "api-version": "7.1",
                "$top": str(top),
                "$skip": str(skip),
            }
            if since:
                params["searchCriteria.fromDate"] = since

            try:
                page = api_get_list(
                    self._session, url, params, "commits"
                )
            except RuntimeError:
                # Skip repos we can't read (e.g. disabled, no permission)
                return records, max_date

            for commit in page:
                commit_date = commit.get("committer", {}).get("date")
                if since and commit_date and commit_date <= since:
                    continue
                rec = dict(commit)
                rec["organization"] = self.organization
                rec["project_name"] = project
                rec["repository_id"] = repo_id
                nullify_empty(rec, "author", "committer", "changeCounts")
                records.append(rec)
                if commit_date and (
                    max_date is None or commit_date > max_date
                ):
                    max_date = commit_date

            if len(page) < top:
                break
            skip += top

        return records, max_date

    def _resolve_repo_pairs(
        self, table_options: dict[str, str]
    ) -> list[tuple[str, str]]:
        """Return the (project, repo_id) pairs to read for this table.

        Priority:
          1. ``repository_id`` + ``project`` in options -> single pair.
          2. ``project`` only -> all repos in that project.
          3. Neither -> all repos across all discovered projects.
        """
        repo_id = table_options.get("repository_id")
        project = self._resolve_project(table_options)

        if repo_id:
            if not project:
                raise ValueError(
                    "Project must be specified when repository_id is provided"
                )
            return [(project, repo_id)]

        repos_iter, _ = self._read_repositories({}, {"project": project} if project else {})
        pairs: list[tuple[str, str]] = []
        for repo in repos_iter:
            rid = repo.get("id")
            proj = repo.get("project_name")
            if rid and proj:
                pairs.append((proj, rid))
        return pairs

    def _read_pullrequests(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read pullrequests incrementally with per-repo watermarks.

        Uses ADO's ``searchCriteria.queryTimeRangeType=updated`` with
        ``searchCriteria.minTime=watermark`` so each run returns only
        PRs whose state has changed since the last sync. The watermark
        is the max of ``closedDate`` (for closed PRs) or ``creationDate``
        (for open PRs) across returned records.

        Subject to one caveat: modifications to open PRs (title edits,
        reviewer votes, etc.) are picked up only if ADO's "updated"
        query reflects those changes. Full refresh recovers anything
        that slips through.
        """
        status_filter = table_options.get("status_filter", "all")

        start_offset = start_offset or {}
        watermarks: dict[str, str] = dict(start_offset.get("watermarks", {}))

        repo_pairs = self._resolve_repo_pairs(table_options)
        all_records: list[dict[str, Any]] = []

        for project, repo_id in repo_pairs:
            key = f"{project}/{repo_id}"
            since = watermarks.get(key)
            records, max_time = self._fetch_pullrequests(
                project, repo_id, status_filter, since
            )
            all_records.extend(records)
            if max_time and (not since or max_time > since):
                watermarks[key] = max_time

        return iter(all_records), {"watermarks": watermarks}

    def _fetch_pullrequests(
        self,
        project: str,
        repo_id: str,
        status_filter: str,
        since: str | None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        """Fetch pullrequests for a single repo, optionally filtered by *since*."""
        url = (
            f"{self.base_url}/{project}/_apis/git/repositories"
            f"/{repo_id}/pullrequests"
        )
        params: dict[str, str] = {
            "api-version": "7.1",
            "searchCriteria.status": status_filter,
        }
        if since:
            params["searchCriteria.queryTimeRangeType"] = "updated"
            params["searchCriteria.minTime"] = since

        try:
            prs = api_get_list(
                self._session, url, params, "pullrequests"
            )
        except RuntimeError as exc:
            _LOG.warning(
                "Skipping pullrequests for %s/%s: %s",
                project, repo_id, exc,
            )
            return [], None

        records: list[dict[str, Any]] = []
        max_time: str | None = since
        for pr in prs:
            # Client-side strict `>` to make the inclusive server-side
            # filter behave exclusively for CDC-merge purposes.
            effective_time = pr.get("closedDate") or pr.get("creationDate")
            if since and effective_time and effective_time <= since:
                continue
            rec = dict(pr)
            rec["organization"] = self.organization
            rec["project_name"] = project
            rec["repository_id"] = repo_id
            nullify_empty(
                rec, "createdBy",
                "lastMergeSourceCommit", "lastMergeTargetCommit",
                "lastMergeCommit",
            )
            records.append(rec)
            if effective_time and (
                max_time is None or effective_time > max_time
            ):
                max_time = effective_time

        return records, max_time

    def _read_refs(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        repo_id = table_options.get("repository_id")
        if not repo_id:
            return self._for_all_repos(
                start_offset, table_options, self._read_refs
            )

        project = self._resolve_project(table_options)
        if not project:
            raise ValueError(
                "Project must be specified when repository_id is provided"
            )

        url = (
            f"{self.base_url}/{project}/_apis/git/repositories"
            f"/{repo_id}/refs"
        )
        params: dict[str, str] = {"api-version": "7.1"}
        ref_filter = table_options.get("filter")
        if ref_filter:
            params["filter"] = ref_filter

        refs = api_get_list(
            self._session, url, params, "refs"
        )

        records = []
        for ref in refs:
            rec = dict(ref)
            rec["organization"] = self.organization
            rec["project_name"] = project
            rec["repository_id"] = repo_id
            nullify_empty(rec, "creator")
            records.append(rec)
        return iter(records), {}

    def _read_pushes(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read pushes incrementally with per-repo push-date watermarks.

        Same pattern as _read_commits — server-side
        ``searchCriteria.fromDate`` plus client-side strict ``>`` to
        make the filter exclusive for append semantics.
        """
        start_offset = start_offset or {}
        watermarks: dict[str, str] = dict(start_offset.get("watermarks", {}))

        repo_pairs = self._resolve_repo_pairs(table_options)
        all_records: list[dict[str, Any]] = []

        for project, repo_id in repo_pairs:
            key = f"{project}/{repo_id}"
            since = watermarks.get(key)
            records, max_date = self._fetch_pushes(project, repo_id, since)
            all_records.extend(records)
            if max_date and (not since or max_date > since):
                watermarks[key] = max_date

        return iter(all_records), {"watermarks": watermarks}

    def _fetch_pushes(
        self, project: str, repo_id: str, since: str | None
    ) -> tuple[list[dict[str, Any]], str | None]:
        """Fetch all pushes for a single repo since *since* (exclusive)."""
        url = (
            f"{self.base_url}/{project}/_apis/git/repositories"
            f"/{repo_id}/pushes"
        )
        top = 1000
        skip = 0
        max_date: str | None = since
        records: list[dict[str, Any]] = []

        while True:
            params: dict[str, str] = {
                "api-version": "7.1",
                "$top": str(top),
                "$skip": str(skip),
            }
            if since:
                params["searchCriteria.fromDate"] = since

            try:
                page = api_get_list(
                    self._session, url, params, "pushes"
                )
            except RuntimeError:
                return records, max_date

            for push in page:
                push_date = push.get("date")
                if since and push_date and push_date <= since:
                    continue
                rec = dict(push)
                rec["organization"] = self.organization
                rec["project_name"] = project
                rec["repository_id"] = repo_id
                nullify_empty(rec, "pushedBy")
                records.append(rec)
                if push_date and (
                    max_date is None or push_date > max_date
                ):
                    max_date = push_date

            if len(page) < top:
                break
            skip += top

        return records, max_date

    # -- Users --------------------------------------------------------- #

    def _read_users(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        url = f"{self.vssps_base_url}/_apis/graph/users"
        params: dict[str, str] = {"api-version": "7.1-preview.1"}

        token = (start_offset or {}).get("continuationToken")
        if token:
            params["continuationToken"] = token

        response = request_with_retry(self._session, url, params)
        if response.status_code != 200:
            raise RuntimeError(
                f"Azure DevOps API error for users: "
                f"{response.status_code} {response.text}"
            )

        users = response.json().get("value", [])
        records = []
        for u in users:
            rec = dict(u)
            rec["organization"] = self.organization
            nullify_empty(rec, "_links")
            records.append(rec)

        next_token = response.headers.get("X-MS-ContinuationToken")
        if not next_token:
            # Drained — return start_offset unchanged so end_offset ==
            # start_offset and the framework terminates pagination
            # cleanly without an extra empty round-trip.
            return iter(records), start_offset or {}
        return iter(records), {"continuationToken": next_token}

    # -- PR sub-resources (threads, workitems, commits, reviewers) ----- #

    def _read_pullrequest_threads(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        projects = self._resolve_projects(table_options)

        def _fetch(project: str, repo_id: str, pr_id: int):
            url = (
                f"{self.base_url}/{project}/_apis/git/repositories"
                f"/{repo_id}/pullRequests/{pr_id}/threads"
            )
            threads = api_get_list(
                self._session, url,
                {"api-version": "7.1"}, "pullrequest_threads",
            )
            results = []
            for t in threads:
                rec = dict(t)
                rec["organization"] = self.organization
                rec["project_name"] = project
                rec["repository_id"] = repo_id
                rec["pullrequest_id"] = pr_id
                results.append(rec)
            return results

        records = for_each_pr(
            self._session, self.base_url,
            projects, table_options, _fetch,
        )
        return iter(records), {}

    def _read_pr_workitems(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        projects = self._resolve_projects(table_options)

        def _fetch(project: str, repo_id: str, pr_id: int):
            url = (
                f"{self.base_url}/{project}/_apis/git/repositories"
                f"/{repo_id}/pullRequests/{pr_id}/workitems"
            )
            items = api_get_list(
                self._session, url,
                {"api-version": "7.1"}, "pullrequest_workitems",
            )
            results = []
            for item in items:
                rec = dict(item)
                rec["organization"] = self.organization
                rec["project_name"] = project
                rec["repository_id"] = repo_id
                rec["pullrequest_id"] = pr_id
                results.append(rec)
            return results

        records = for_each_pr(
            self._session, self.base_url,
            projects, table_options, _fetch,
        )
        return iter(records), {}

    def _read_pr_commits(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        projects = self._resolve_projects(table_options)

        def _fetch(project: str, repo_id: str, pr_id: int):
            url = (
                f"{self.base_url}/{project}/_apis/git/repositories"
                f"/{repo_id}/pullRequests/{pr_id}/commits"
            )
            commits = api_get_list(
                self._session, url,
                {"api-version": "7.1"}, "pullrequest_commits",
            )
            results = []
            for c in commits:
                rec = dict(c)
                rec["organization"] = self.organization
                rec["project_name"] = project
                rec["repository_id"] = repo_id
                rec["pullrequest_id"] = pr_id
                results.append(rec)
            return results

        records = for_each_pr(
            self._session, self.base_url,
            projects, table_options, _fetch,
        )
        return iter(records), {}

    def _read_pr_reviewers(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        projects = self._resolve_projects(table_options)

        def _fetch(project: str, repo_id: str, pr_id: int):
            url = (
                f"{self.base_url}/{project}/_apis/git/repositories"
                f"/{repo_id}/pullRequests/{pr_id}/reviewers"
            )
            reviewers = api_get_list(
                self._session, url,
                {"api-version": "7.1"}, "pullrequest_reviewers",
            )
            results = []
            for r in reviewers:
                rec = dict(r)
                rec["organization"] = self.organization
                rec["project_name"] = project
                rec["repository_id"] = repo_id
                rec["pullrequest_id"] = pr_id
                results.append(rec)
            return results

        records = for_each_pr(
            self._session, self.base_url,
            projects, table_options, _fetch,
        )
        return iter(records), {}

    # -- Work items ---------------------------------------------------- #

    def _read_workitems(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read workitems — full fetch with CDC merge for incremental sync.

        WIQL's ``[System.ChangedDate]`` comparisons use date-only
        precision (YYYY-MM-DD) and reject timestamp literals, so we
        can't do a fine-grained server-side incremental filter. Instead
        the connector returns the current state of every work item on
        each call; the framework's CDC merge uses the top-level ``rev``
        cursor to apply deltas idempotently — new items are inserted,
        updated items are upserted when their rev moves forward.

        The offset's watermark is the max ``System.ChangedDate`` across
        returned items. It advances whenever any item in a project
        changes, which keeps end_offset != start_offset and signals
        forward progress to the streaming framework.

        When the ``ids`` table option is set, bypasses this path and
        fetches those specific items (snapshot-style).
        """
        explicit_ids = table_options.get("ids")
        projects = self._resolve_projects(table_options)

        start_offset = start_offset or {}
        watermarks: dict[str, str] = dict(start_offset.get("watermarks", {}))
        all_records: list[dict[str, Any]] = []

        for project in projects:
            if explicit_ids:
                ids_to_fetch = [
                    i.strip() for i in explicit_ids.split(",") if i.strip()
                ]
                records, _ = self._fetch_workitems_by_ids(
                    project, ids_to_fetch
                )
                all_records.extend(records)
                continue

            ids_to_fetch = self._discover_workitem_ids(project)
            if not ids_to_fetch:
                continue

            records, max_changed = self._fetch_workitems_by_ids(
                project, ids_to_fetch
            )
            all_records.extend(records)
            # Advance watermark monotonically so the streaming framework
            # sees end_offset != start_offset whenever any item changed.
            if max_changed and (
                not watermarks.get(project)
                or max_changed > watermarks[project]
            ):
                watermarks[project] = max_changed

        if explicit_ids:
            # Explicit-IDs path is snapshot-like — stable offset terminates
            return iter(all_records), start_offset
        return iter(all_records), {"watermarks": watermarks}

    def _discover_workitem_ids(self, project: str) -> list[str]:
        """Return all work item IDs belonging to *project* via WIQL.

        WIQL is organization-scoped by default, so we filter by
        ``[System.TeamProject]`` to avoid cross-project contamination.
        Note: we intentionally don't filter by ``System.ChangedDate`` —
        WIQL requires date-only precision there and rejects timestamp
        literals with a 400 error. Incremental behavior is handled by
        the framework's CDC merge on the ``rev`` cursor.
        """
        url = f"{self.base_url}/{project}/_apis/wit/wiql"
        # Escape single quotes in the project name (e.g. "Bob's Team")
        # so they don't break the WIQL literal or enable injection.
        safe_project = project.replace("'", "''")
        query = (
            "SELECT [System.Id] FROM WorkItems"
            f" WHERE [System.TeamProject] = '{safe_project}'"
            " ORDER BY [System.ChangedDate] ASC"
        )

        response = self._session.post(
            url,
            json={"query": query},
            params={"api-version": "7.1"},
            timeout=30,
        )
        if response.status_code != 200:
            return []
        refs = response.json().get("workItems", [])
        return [str(r["id"]) for r in refs if r.get("id")]

    def _fetch_workitems_by_ids(
        self, project: str, ids: list[str]
    ) -> tuple[list[dict[str, Any]], str | None]:
        """Fetch work items by ID in batches of 200 (ADO bulk GET limit)."""
        records: list[dict[str, Any]] = []
        max_changed: str | None = None
        batch_size = 200  # ADO bulk workitems GET hard limit

        for i in range(0, len(ids), batch_size):
            batch = ids[i : i + batch_size]
            url = f"{self.base_url}/{project}/_apis/wit/workitems"
            items = api_get_list(
                self._session, url,
                {
                    "api-version": "7.1",
                    "ids": ",".join(batch),
                    "$expand": "relations",
                },
                "workitems",
            )
            for item in items:
                rec = dict(item)
                rec["organization"] = self.organization
                rec["project_name"] = project
                changed_date: str | None = None
                if "fields" in rec and isinstance(rec["fields"], dict):
                    changed_date = rec["fields"].get("System.ChangedDate")
                    rec["fields"] = json.dumps(rec["fields"])
                records.append(rec)
                if changed_date and (
                    max_changed is None or changed_date > max_changed
                ):
                    max_changed = changed_date

        return records, max_changed

    def _read_workitem_revisions(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read workitem_revisions with per-project watermarks.

        The Azure DevOps Reporting API's ``continuationToken`` doubles as:
          - A pagination cursor within a sync (when ``isLastBatch`` is false)
          - A persistent watermark for the next sync (when ``isLastBatch``
            is true — passing that token back later returns only revisions
            that occurred since)

        The offset carries two things:
          - ``watermarks``: per-project tokens that survive across pipeline
            runs so subsequent runs fetch only new revisions.
          - ``resume``: the in-flight pagination state of the current
            project within a single sync.
        """
        projects = self._resolve_projects(table_options)
        if not projects:
            return iter([]), start_offset or {}

        watermarks, resume = self._parse_wir_offset(start_offset)

        start_idx = 0
        current_token: str | None = None
        if resume and resume.get("project") in projects:
            start_idx = projects.index(resume["project"])
            current_token = resume.get("continuationToken")

        all_records: list[dict[str, Any]] = []

        for i in range(start_idx, len(projects)):
            project = projects[i]
            url = (
                f"{self.base_url}/{project}"
                "/_apis/wit/reporting/workitemrevisions"
            )
            params: dict[str, str] = {
                "api-version": "7.1",
                "includeDeleted": "true",
            }
            # In-flight pagination token wins; otherwise fall back to the
            # saved watermark for this project (incremental from last run).
            token = current_token or watermarks.get(project)
            if token:
                params["continuationToken"] = token

            try:
                data = api_get(
                    self._session, url, params, "workitem_revisions"
                )
            except RuntimeError:
                # Skip projects we can't read (permissions, disabled, etc.)
                current_token = None
                continue

            for rev in data.get("values", []):
                rec = dict(rev)
                rec["organization"] = self.organization
                rec["project_name"] = project
                # The reporting endpoint's top-level response only
                # includes `id`, `rev`, and `fields`. Populate the
                # declared schema columns from the data we have:
                #   - workItemId: same as `id` (reporting endpoint uses
                #     `id` as the work item ID, no separate revision ID)
                #   - revisedDate: surfaced inside `fields` as
                #     System.RevisedDate — hoist it to the top level
                #   - url: not returned by the reporting endpoint;
                #     construct the revision resource URL so the column
                #     is still useful for navigation
                rec["workItemId"] = rec.get("id")
                fields_dict = rec.get("fields")
                if isinstance(fields_dict, dict):
                    rec["revisedDate"] = fields_dict.get(
                        "System.RevisedDate"
                    )
                    rec["fields"] = json.dumps(fields_dict)
                rec["url"] = (
                    f"{self.base_url}/{project}/_apis/wit/workItems/"
                    f"{rec.get('id')}/revisions/{rec.get('rev')}"
                )
                all_records.append(rec)

            is_last_batch = data.get("isLastBatch", True)
            next_token = data.get("continuationToken")
            if not is_last_batch and next_token:
                # Mid-pagination — resume this project on the next call.
                # Watermarks stay untouched until the project fully drains.
                return iter(all_records), {
                    "watermarks": watermarks,
                    "resume": {
                        "project": project,
                        "continuationToken": next_token,
                    },
                }

            # Project fully drained — persist its final token as the
            # watermark so the next pipeline run fetches only new revisions.
            if next_token:
                watermarks[project] = next_token
            current_token = None

        # All projects drained for this pass — stable offset signals "done".
        # On the next pipeline run the watermarks drive an incremental sync.
        return iter(all_records), {"watermarks": watermarks}

    @staticmethod
    def _parse_wir_offset(
        start_offset: dict | None,
    ) -> tuple[dict, dict | None]:
        """Parse workitem_revisions offset with backwards compatibility."""
        start_offset = start_offset or {}
        watermarks = dict(start_offset.get("watermarks", {}))
        resume = start_offset.get("resume")
        # Back-compat: older offset shapes used {project, continuationToken}
        # flat at the top level. Treat those as in-flight pagination.
        if not watermarks and not resume:
            old_project = start_offset.get("project")
            old_token = start_offset.get("continuationToken")
            if old_project and old_token:
                resume = {
                    "project": old_project,
                    "continuationToken": old_token,
                }
        return watermarks, resume

    def _read_workitem_types(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        projects = self._resolve_projects(table_options)
        all_records: list[dict[str, Any]] = []

        for project in projects:
            url = (
                f"{self.base_url}/{project}"
                "/_apis/wit/workitemtypes"
            )
            types = api_get_list(
                self._session, url,
                {"api-version": "7.1"}, "workitem_types",
            )
            for wt in types:
                rec = dict(wt)
                rec["organization"] = self.organization
                rec["project_name"] = project
                all_records.append(rec)

        return iter(all_records), {}
