import base64
import json
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
    resolve_projects,
    fetch_repos,
    fetch_prs,
    nullify_empty,
    for_each_pr,
)


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
            except Exception:  # pylint: disable=broad-except
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
            except Exception:  # pylint: disable=broad-except
                continue
        return iter(all_records), {}

    # -- Git objects (commits, PRs, refs, pushes) ---------------------- #

    def _read_commits(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        repo_id = table_options.get("repository_id")
        if not repo_id:
            return self._for_all_repos(
                start_offset, table_options, self._read_commits
            )

        project = self._resolve_project(table_options)
        if not project:
            raise ValueError(
                "Project must be specified when repository_id is provided"
            )

        skip = (start_offset or {}).get("skip", 0)
        top = 1000
        url = (
            f"{self.base_url}/{project}/_apis/git/repositories"
            f"/{repo_id}/commits"
        )
        commits = api_get_list(
            self._session, url,
            {"api-version": "7.1", "$top": str(top), "$skip": str(skip)},
            "commits",
        )

        records = []
        for c in commits:
            rec = dict(c)
            rec["organization"] = self.organization
            rec["project_name"] = project
            rec["repository_id"] = repo_id
            nullify_empty(rec, "author", "committer", "changeCounts")
            records.append(rec)

        new_offset = {"skip": skip + top} if len(commits) == top else {}
        return iter(records), new_offset

    def _read_pullrequests(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        repo_id = table_options.get("repository_id")
        if not repo_id:
            return self._for_all_repos(
                start_offset, table_options, self._read_pullrequests
            )

        project = self._resolve_project(table_options)
        if not project:
            raise ValueError(
                "Project must be specified when repository_id is provided"
            )

        status_filter = table_options.get("status_filter", "all")
        url = (
            f"{self.base_url}/{project}/_apis/git/repositories"
            f"/{repo_id}/pullrequests"
        )
        prs = api_get_list(
            self._session, url,
            {"api-version": "7.1", "searchCriteria.status": status_filter},
            "pullrequests",
        )

        records = []
        for pr in prs:
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
        return iter(records), {}

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
        repo_id = table_options.get("repository_id")
        if not repo_id:
            return self._for_all_repos(
                start_offset, table_options, self._read_pushes
            )

        project = self._resolve_project(table_options)
        if not project:
            raise ValueError(
                "Project must be specified when repository_id is provided"
            )

        skip = (start_offset or {}).get("skip", 0)
        top = 1000
        url = (
            f"{self.base_url}/{project}/_apis/git/repositories"
            f"/{repo_id}/pushes"
        )
        pushes = api_get_list(
            self._session, url,
            {"api-version": "7.1", "$top": str(top), "$skip": str(skip)},
            "pushes",
        )

        records = []
        for push in pushes:
            rec = dict(push)
            rec["organization"] = self.organization
            rec["project_name"] = project
            rec["repository_id"] = repo_id
            nullify_empty(rec, "pushedBy")
            records.append(rec)

        new_offset = {"skip": skip + top} if len(pushes) == top else {}
        return iter(records), new_offset

    # -- Users --------------------------------------------------------- #

    def _read_users(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        url = f"{self.vssps_base_url}/_apis/graph/users"
        params: dict[str, str] = {"api-version": "7.1-preview.1"}

        token = (start_offset or {}).get("continuationToken")
        if token:
            params["continuationToken"] = token

        response = self._session.get(url, params=params, timeout=30)
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

        new_offset: dict[str, str] = {}
        next_token = response.headers.get("X-MS-ContinuationToken")
        if next_token:
            new_offset["continuationToken"] = next_token
        return iter(records), new_offset

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
        ids = table_options.get("ids")
        projects = self._resolve_projects(table_options)
        all_records: list[dict[str, Any]] = []

        for project in projects:
            if not ids:
                ids_to_fetch = self._discover_workitem_ids(project)
                if not ids_to_fetch:
                    continue
            else:
                ids_to_fetch = ids

            url = f"{self.base_url}/{project}/_apis/wit/workitems"
            items = api_get_list(
                self._session, url,
                {
                    "api-version": "7.1",
                    "ids": ids_to_fetch,
                    "$expand": "relations",
                },
                "workitems",
            )
            for item in items:
                rec = dict(item)
                rec["organization"] = self.organization
                rec["project_name"] = project
                if "fields" in rec and isinstance(rec["fields"], dict):
                    rec["fields"] = json.dumps(rec["fields"])
                all_records.append(rec)

        return iter(all_records), {}

    def _discover_workitem_ids(self, project: str) -> str | None:
        """Use WIQL to discover all work item IDs in a project."""
        url = f"{self.base_url}/{project}/_apis/wit/wiql"
        response = self._session.post(
            url,
            json={"query": "SELECT [System.Id] FROM WorkItems"},
            params={"api-version": "7.1"},
            timeout=30,
        )
        if response.status_code != 200:
            return None
        refs = response.json().get("workItems", [])
        if not refs:
            return None
        return ",".join(
            str(r["id"]) for r in refs if r.get("id")
        )

    def _read_workitem_revisions(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        projects = self._resolve_projects(table_options)
        all_records: list[dict[str, Any]] = []

        for project in projects:
            url = (
                f"{self.base_url}/{project}"
                "/_apis/wit/reporting/workitemrevisions"
            )
            params: dict[str, str] = {
                "api-version": "7.1",
                "includeDeleted": "true",
            }
            token = (start_offset or {}).get("continuationToken")
            if token:
                params["continuationToken"] = token

            data = api_get(
                self._session, url, params, "workitem_revisions"
            )
            for rev in data.get("values", []):
                rec = dict(rev)
                rec["organization"] = self.organization
                rec["project_name"] = project
                if "fields" in rec and isinstance(rec["fields"], dict):
                    rec["fields"] = json.dumps(rec["fields"])
                all_records.append(rec)

            next_token = data.get("continuationToken")
            if next_token:
                return iter(all_records), {
                    "continuationToken": next_token
                }

        return iter(all_records), {}

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
