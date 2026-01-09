from __future__ import annotations

import requests
import base64
from typing import Iterator, Any

from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    BooleanType,
    IntegerType,
    ArrayType,
)


class LakeflowConnect:
    def __init__(self, options: dict[str, str]) -> None:
        """
        Initialize the Azure DevOps connector with connection-level options.

        Expected options:
            - organization: Azure DevOps organization name.
            - project: Project name or ID (optional for organization-level objects like users).
            - personal_access_token: Personal access token (PAT) for authentication.
        """
        organization = options.get("organization")
        project = options.get("project")
        personal_access_token = options.get("personal_access_token")

        if not organization:
            raise ValueError(
                "Azure DevOps connector requires 'organization' in options"
            )
        # Note: project is optional for organization-level objects (e.g., users)
        if not personal_access_token:
            raise ValueError(
                "Azure DevOps connector requires 'personal_access_token' in options"
            )

        self.organization = organization
        self.project = project  # May be None for organization-level objects
        self.base_url = f"https://dev.azure.com/{organization}"
        self.vssps_base_url = f"https://vssps.dev.azure.com/{organization}"

        # Encode PAT as Base64 in the format :{pat}
        auth_string = f":{personal_access_token}"
        auth_bytes = auth_string.encode("ascii")
        base64_auth = base64.b64encode(auth_bytes).decode("ascii")

        # Configure a session with proper headers for Azure DevOps REST API
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Basic {base64_auth}",
                "Accept": "application/json",
            }
        )

    def list_tables(self) -> list[str]:
        """
        List names of all tables supported by this connector.

        Supported tables:
        - projects: Projects within an organization
        - repositories: Git repositories within a project
        - commits: Git commits across repositories
        - pullrequests: Pull requests across repositories
        - refs: Git references (branches and tags)
        - pushes: Git push events to repositories
        - users: User identities and profiles in the organization
        """
        return ["projects", "repositories", "commits", "pullrequests", "refs", "pushes", "users"]

    def get_table_schema(
        self, table_name: str, table_options: dict[str, str]
    ) -> StructType:
        """
        Fetch the schema of a table.

        The schema is static and derived from the Azure DevOps REST API documentation
        and connector design for the `repositories` object.
        """
        if table_name not in self.list_tables():
            raise ValueError(f"Unsupported table: {table_name!r}")

        # Nested `project` struct schema
        project_struct = StructType(
            [
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("url", StringType(), True),
                StructField("state", StringType(), True),
                StructField("revision", LongType(), True),
                StructField("visibility", StringType(), True),
                StructField("lastUpdateTime", StringType(), True),
            ]
        )

        # Nested `parentRepository` struct schema (only present for forks)
        parent_repository_struct = StructType(
            [
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("url", StringType(), True),
                StructField("project", project_struct, True),
            ]
        )

        # Nested `_links` struct schema (HAL-style hypermedia links)
        # Each link has an 'href' field
        link_struct = StructType([StructField("href", StringType(), True)])

        links_struct = StructType(
            [
                StructField("self", link_struct, True),
                StructField("project", link_struct, True),
                StructField("web", link_struct, True),
                StructField("ssh", link_struct, True),
                StructField("commits", link_struct, True),
                StructField("refs", link_struct, True),
                StructField("pullRequests", link_struct, True),
                StructField("items", link_struct, True),
                StructField("pushes", link_struct, True),
            ]
        )

        # Nested identity struct (for createdBy, author, committer, pushedBy, etc.)
        identity_struct = StructType(
            [
                StructField("id", StringType(), True),
                StructField("displayName", StringType(), True),
                StructField("uniqueName", StringType(), True),
                StructField("url", StringType(), True),
                StructField("imageUrl", StringType(), True),
            ]
        )

        # Nested author/committer struct (for commits)
        git_user_struct = StructType(
            [
                StructField("name", StringType(), True),
                StructField("email", StringType(), True),
                StructField("date", StringType(), True),
            ]
        )

        # Nested changeCounts struct (for commits)
        change_counts_struct = StructType(
            [
                StructField("Add", IntegerType(), True),
                StructField("Edit", IntegerType(), True),
                StructField("Delete", IntegerType(), True),
            ]
        )

        # Nested commit reference struct (for pull requests)
        commit_ref_struct = StructType(
            [
                StructField("commitId", StringType(), True),
                StructField("url", StringType(), True),
            ]
        )

        # Nested refUpdate struct (for pushes)
        ref_update_struct = StructType(
            [
                StructField("name", StringType(), True),
                StructField("oldObjectId", StringType(), True),
                StructField("newObjectId", StringType(), True),
            ]
        )

        if table_name == "projects":
            projects_schema = StructType(
                [
                    StructField("id", StringType(), False),
                    StructField("name", StringType(), True),
                    StructField("description", StringType(), True),
                    StructField("url", StringType(), True),
                    StructField("state", StringType(), True),
                    StructField("revision", LongType(), True),
                    StructField("visibility", StringType(), True),
                    StructField("lastUpdateTime", StringType(), True),
                    StructField("organization", StringType(), False),
                ]
            )
            return projects_schema

        if table_name == "repositories":
            repositories_schema = StructType(
                [
                    StructField("id", StringType(), False),
                    StructField("name", StringType(), True),
                    StructField("url", StringType(), True),
                    StructField("project", project_struct, True),
                    StructField("defaultBranch", StringType(), True),
                    StructField("size", LongType(), True),
                    StructField("remoteUrl", StringType(), True),
                    StructField("sshUrl", StringType(), True),
                    StructField("webUrl", StringType(), True),
                    StructField("isDisabled", BooleanType(), True),
                    StructField("isInMaintenance", BooleanType(), True),
                    StructField("isFork", BooleanType(), True),
                    StructField("parentRepository", parent_repository_struct, True),
                    StructField("_links", links_struct, True),
                    StructField("organization", StringType(), False),
                    StructField("project_name", StringType(), False),
                ]
            )
            return repositories_schema

        if table_name == "commits":
            commits_schema = StructType(
                [
                    StructField("commitId", StringType(), False),
                    StructField("author", git_user_struct, True),
                    StructField("committer", git_user_struct, True),
                    StructField("comment", StringType(), True),
                    StructField("commentTruncated", BooleanType(), True),
                    StructField("changeCounts", change_counts_struct, True),
                    StructField("url", StringType(), True),
                    StructField("remoteUrl", StringType(), True),
                    StructField("treeId", StringType(), True),
                    StructField("parents", ArrayType(StringType()), True),
                    StructField("organization", StringType(), False),
                    StructField("project_name", StringType(), False),
                    StructField("repository_id", StringType(), False),
                ]
            )
            return commits_schema

        if table_name == "pullrequests":
            pullrequests_schema = StructType(
                [
                    StructField("pullRequestId", LongType(), False),
                    StructField("codeReviewId", LongType(), True),
                    StructField("status", StringType(), True),
                    StructField("createdBy", identity_struct, True),
                    StructField("creationDate", StringType(), True),
                    StructField("closedDate", StringType(), True),
                    StructField("title", StringType(), True),
                    StructField("description", StringType(), True),
                    StructField("sourceRefName", StringType(), True),
                    StructField("targetRefName", StringType(), True),
                    StructField("mergeStatus", StringType(), True),
                    StructField("mergeId", StringType(), True),
                    StructField("lastMergeSourceCommit", commit_ref_struct, True),
                    StructField("lastMergeTargetCommit", commit_ref_struct, True),
                    StructField("lastMergeCommit", commit_ref_struct, True),
                    StructField("url", StringType(), True),
                    StructField("supportsIterations", BooleanType(), True),
                    StructField("artifactId", StringType(), True),
                    StructField("organization", StringType(), False),
                    StructField("project_name", StringType(), False),
                    StructField("repository_id", StringType(), False),
                ]
            )
            return pullrequests_schema

        if table_name == "refs":
            refs_schema = StructType(
                [
                    StructField("name", StringType(), False),
                    StructField("objectId", StringType(), True),
                    StructField("creator", identity_struct, True),
                    StructField("url", StringType(), True),
                    StructField("peeledObjectId", StringType(), True),
                    StructField("organization", StringType(), False),
                    StructField("project_name", StringType(), False),
                    StructField("repository_id", StringType(), False),
                ]
            )
            return refs_schema

        if table_name == "pushes":
            pushes_schema = StructType(
                [
                    StructField("pushId", LongType(), False),
                    StructField("date", StringType(), True),
                    StructField("pushedBy", identity_struct, True),
                    StructField("url", StringType(), True),
                    StructField("refUpdates", ArrayType(ref_update_struct), True),
                    StructField("organization", StringType(), False),
                    StructField("project_name", StringType(), False),
                    StructField("repository_id", StringType(), False),
                ]
            )
            return pushes_schema

        if table_name == "users":
            users_schema = StructType(
                [
                    StructField("descriptor", StringType(), False),
                    StructField("displayName", StringType(), True),
                    StructField("mailAddress", StringType(), True),
                    StructField("principalName", StringType(), True),
                    StructField("origin", StringType(), True),
                    StructField("originId", StringType(), True),
                    StructField("subjectKind", StringType(), True),
                    StructField("domain", StringType(), True),
                    StructField("directoryAlias", StringType(), True),
                    StructField("url", StringType(), True),
                    StructField("_links", links_struct, True),
                    StructField("organization", StringType(), False),
                ]
            )
            return users_schema

        raise ValueError(f"Unsupported table: {table_name!r}")

    def read_table_metadata(
        self, table_name: str, table_options: dict[str, str]
    ) -> dict:
        """
        Fetch metadata for the given table.

        Metadata includes:
        - primary_keys: List of column names that uniquely identify a record
        - ingestion_type: One of 'snapshot', 'append', or 'cdc'
        - cursor_field: (optional) Field used for incremental ingestion
        """
        if table_name not in self.list_tables():
            raise ValueError(f"Unsupported table: {table_name!r}")

        if table_name == "projects":
            return {
                "primary_keys": ["id"],
                "ingestion_type": "snapshot",
            }

        if table_name == "repositories":
            return {
                "primary_keys": ["id"],
                "ingestion_type": "snapshot",
            }

        if table_name == "commits":
            return {
                "primary_keys": ["commitId", "repository_id"],
                "ingestion_type": "append",
            }

        if table_name == "pullrequests":
            return {
                "primary_keys": ["pullRequestId", "repository_id"],
                "cursor_field": "closedDate",
                "ingestion_type": "cdc",
            }

        if table_name == "refs":
            return {
                "primary_keys": ["name", "repository_id"],
                "ingestion_type": "snapshot",
            }

        if table_name == "pushes":
            return {
                "primary_keys": ["pushId", "repository_id"],
                "ingestion_type": "append",
            }

        if table_name == "users":
            return {
                "primary_keys": ["descriptor"],
                "ingestion_type": "snapshot",
            }

        raise ValueError(f"Unsupported table: {table_name!r}")

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read records from a table and return raw JSON-like dictionaries.

        Table-specific requirements:
        - repositories: No table_options required
        - commits, pullrequests, refs, pushes: Require repository_id in table_options

        Returns:
        - Iterator of record dictionaries
        - Updated offset dictionary for pagination/incremental reads
        """
        if table_name not in self.list_tables():
            raise ValueError(f"Unsupported table: {table_name!r}")

        if table_name == "projects":
            return self._read_projects(start_offset, table_options)

        if table_name == "repositories":
            return self._read_repositories(start_offset, table_options)

        if table_name == "commits":
            return self._read_commits(start_offset, table_options)

        if table_name == "pullrequests":
            return self._read_pullrequests(start_offset, table_options)

        if table_name == "refs":
            return self._read_refs(start_offset, table_options)

        if table_name == "pushes":
            return self._read_pushes(start_offset, table_options)

        if table_name == "users":
            return self._read_users(start_offset, table_options)

        raise ValueError(f"Unsupported table: {table_name!r}")

    def _read_projects(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read the `projects` snapshot table.

        This implementation lists all projects in the configured organization
        using the Azure DevOps REST API:

            GET /{organization}/_apis/projects?api-version=7.1

        The returned JSON objects are enriched with connector-derived fields:
            - organization: The organization name from connection config.
        """
        url = f"{self.base_url}/_apis/projects"
        params = {
            "api-version": "7.1",
        }

        response = self._session.get(url, params=params, timeout=30)
        if response.status_code != 200:
            raise RuntimeError(
                f"Azure DevOps API error for projects: {response.status_code} {response.text}"
            )

        response_json = response.json()
        if not isinstance(response_json, dict):
            raise ValueError(
                f"Unexpected response format for projects: {type(response_json).__name__}"
            )

        projects = response_json.get("value", [])
        if not isinstance(projects, list):
            raise ValueError(
                f"Unexpected 'value' format in projects response: {type(projects).__name__}"
            )

        records: list[dict[str, Any]] = []
        for project_obj in projects:
            # Shallow-copy the raw JSON and add connector-derived fields
            record: dict[str, Any] = dict(project_obj)
            record["organization"] = self.organization

            records.append(record)

        # Snapshot ingestion: return empty offset
        return iter(records), {}

    def _read_repositories(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read the `repositories` snapshot table.

        This implementation lists all Git repositories using the Azure DevOps REST API:

            GET /{organization}/{project}/_apis/git/repositories?api-version=7.1

        Project resolution:
        - If `project` is in table_options, use that project
        - Else if self.project is set (connection-level), use that
        - Else fetch from all projects in the organization (auto-discovery)

        Note: API version 7.1 is used instead of 7.2 to avoid preview version
        requirements. Version 7.2 requires the -preview flag as of this implementation.

        The returned JSON objects are enriched with connector-derived fields:
            - organization: The organization name from connection config.
            - project_name: The project name from the fetched data.
        """
        # Determine which project to use
        project = table_options.get("project") or self.project
        
        # If no project specified, fetch from all projects
        if not project:
            return self._read_repositories_all_projects(start_offset, table_options)
        
        url = f"{self.base_url}/{project}/_apis/git/repositories"
        params = {
            "api-version": "7.1",
            "includeLinks": "true",
            "includeAllUrls": "true",
        }

        response = self._session.get(url, params=params, timeout=30)
        if response.status_code != 200:
            raise RuntimeError(
                f"Azure DevOps API error for repositories: {response.status_code} {response.text}"
            )

        response_json = response.json()
        if not isinstance(response_json, dict):
            raise ValueError(
                f"Unexpected response format for repositories: {type(response_json).__name__}"
            )

        repos = response_json.get("value", [])
        if not isinstance(repos, list):
            raise ValueError(
                f"Unexpected 'value' format in repositories response: {type(repos).__name__}"
            )

        records: list[dict[str, Any]] = []
        for repo_obj in repos:
            # Shallow-copy the raw JSON and add connector-derived fields
            record: dict[str, Any] = dict(repo_obj)
            record["organization"] = self.organization
            record["project_name"] = project

            # Ensure nested structs that are absent are represented as None, not {}
            if "parentRepository" not in record or record["parentRepository"] == {}:
                record["parentRepository"] = None
            if "project" not in record or record["project"] == {}:
                record["project"] = None
            if "_links" not in record or record["_links"] == {}:
                record["_links"] = None

            records.append(record)

        # Snapshot ingestion: return empty offset
        return iter(records), {}

    def _read_repositories_all_projects(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Helper method to fetch repositories from ALL projects in the organization.
        
        This is called when no project is specified at connection or table level.
        """
        # First, get all projects
        projects_iterator, _ = self._read_projects({}, {})
        projects = list(projects_iterator)
        
        all_records: list[dict[str, Any]] = []
        
        for project_obj in projects:
            project_name = project_obj.get("name")
            if not project_name:
                continue
                
            # Fetch repositories for this project
            try:
                # Create a temporary table_options with this project
                temp_options = dict(table_options)
                temp_options["project"] = project_name
                
                # Recursively call _read_repositories with this specific project
                repos_iterator, _ = self._read_repositories({}, temp_options)
                all_records.extend(list(repos_iterator))
            except Exception:
                # Skip projects that fail (might not have Git enabled)
                continue
        
        return iter(all_records), {}

    def _read_commits(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read the `commits` append table.

        Table options:
        - repository_id (optional): Repository ID or name to fetch commits from.
          If not provided, fetches commits from ALL repositories in the project.

        Supports pagination using $skip offset from start_offset.
        """
        repository_id = table_options.get("repository_id")
        
        # If repository_id not provided, fetch from all repositories
        if not repository_id:
            return self._read_commits_all_repos(start_offset, table_options)

        # Determine which project to use
        project = table_options.get("project") or self.project
        if not project:
            raise ValueError("Project must be specified either at connection level or in table_options when repository_id is provided")

        url = f"{self.base_url}/{project}/_apis/git/repositories/{repository_id}/commits"
        
        # Get pagination offset from start_offset
        skip = start_offset.get("skip", 0)
        top = 1000  # Fetch 1000 commits per page
        
        params = {
            "api-version": "7.1",
            "$top": str(top),
            "$skip": str(skip),
        }

        response = self._session.get(url, params=params, timeout=30)
        if response.status_code != 200:
            raise RuntimeError(
                f"Azure DevOps API error for commits: {response.status_code} {response.text}"
            )

        response_json = response.json()
        if not isinstance(response_json, dict):
            raise ValueError(
                f"Unexpected response format for commits: {type(response_json).__name__}"
            )

        commits = response_json.get("value", [])
        if not isinstance(commits, list):
            raise ValueError(
                f"Unexpected 'value' format in commits response: {type(commits).__name__}"
            )

        records: list[dict[str, Any]] = []
        for commit_obj in commits:
            record: dict[str, Any] = dict(commit_obj)
            record["organization"] = self.organization
            record["project_name"] = project
            record["repository_id"] = repository_id

            # Ensure nested structs are None if absent
            if "author" not in record or record["author"] == {}:
                record["author"] = None
            if "committer" not in record or record["committer"] == {}:
                record["committer"] = None
            if "changeCounts" not in record or record["changeCounts"] == {}:
                record["changeCounts"] = None

            records.append(record)

        # Update offset for next page
        new_offset = {}
        if len(commits) == top:
            # More data likely available
            new_offset["skip"] = skip + top
        
        return iter(records), new_offset

    def _read_commits_all_repos(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read commits from ALL repositories in the project.
        
        This method:
        1. Fetches all repositories
        2. For each repository, fetches commits
        3. Combines all commits into a single result set
        """
        # First, get all repositories
        repos_iterator, _ = self._read_repositories({}, {})
        repositories = list(repos_iterator)
        
        if not repositories:
            # No repositories found, return empty
            return iter([]), {}
        
        all_commits = []
        
        # For each repository, fetch commits
        for repo in repositories:
            repo_id = repo.get("id")
            project_name = repo.get("project_name")  # Get project from repo
            if not repo_id or not project_name:
                continue
                
            # Create modified table_options with this repository_id and project
            repo_table_options = dict(table_options)
            repo_table_options["repository_id"] = repo_id
            repo_table_options["project"] = project_name  # Add project from repo
            
            # Fetch commits for this repository
            try:
                commits_iterator, _ = self._read_commits(start_offset, repo_table_options)
                commits = list(commits_iterator)
                all_commits.extend(commits)
            except Exception:
                # If a repository fails, continue with others
                # (e.g., empty repo, permissions issue)
                continue
        
        # Return all commits combined
        return iter(all_commits), {}

    def _read_pullrequests(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read the `pullrequests` CDC table.

        Table options:
        - repository_id (optional): Repository ID or name to fetch pull requests from.
          If not provided, fetches pull requests from ALL repositories in the project.
        - status_filter (optional): Filter by status (active, completed, abandoned, all). Default: all.
        """
        repository_id = table_options.get("repository_id")
        
        # If repository_id not provided, fetch from all repositories
        if not repository_id:
            return self._read_pullrequests_all_repos(start_offset, table_options)

        # Determine which project to use
        project = table_options.get("project") or self.project
        if not project:
            raise ValueError("Project must be specified either at connection level or in table_options when repository_id is provided")

        url = f"{self.base_url}/{project}/_apis/git/repositories/{repository_id}/pullrequests"
        
        status_filter = table_options.get("status_filter", "all")
        
        params = {
            "api-version": "7.1",
            "searchCriteria.status": status_filter,
        }

        response = self._session.get(url, params=params, timeout=30)
        if response.status_code != 200:
            raise RuntimeError(
                f"Azure DevOps API error for pullrequests: {response.status_code} {response.text}"
            )

        response_json = response.json()
        if not isinstance(response_json, dict):
            raise ValueError(
                f"Unexpected response format for pullrequests: {type(response_json).__name__}"
            )

        prs = response_json.get("value", [])
        if not isinstance(prs, list):
            raise ValueError(
                f"Unexpected 'value' format in pullrequests response: {type(prs).__name__}"
            )

        records: list[dict[str, Any]] = []
        for pr_obj in prs:
            record: dict[str, Any] = dict(pr_obj)
            record["organization"] = self.organization
            record["project_name"] = project
            record["repository_id"] = repository_id

            # Ensure nested structs are None if absent
            if "createdBy" not in record or record["createdBy"] == {}:
                record["createdBy"] = None
            if "lastMergeSourceCommit" not in record or record["lastMergeSourceCommit"] == {}:
                record["lastMergeSourceCommit"] = None
            if "lastMergeTargetCommit" not in record or record["lastMergeTargetCommit"] == {}:
                record["lastMergeTargetCommit"] = None
            if "lastMergeCommit" not in record or record["lastMergeCommit"] == {}:
                record["lastMergeCommit"] = None

            records.append(record)

        # CDC ingestion: return empty offset (full fetch each time)
        return iter(records), {}

    def _read_pullrequests_all_repos(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read pull requests from ALL repositories in the project.
        
        This method:
        1. Fetches all repositories
        2. For each repository, fetches pull requests
        3. Combines all PRs into a single result set
        """
        # First, get all repositories
        repos_iterator, _ = self._read_repositories({}, {})
        repositories = list(repos_iterator)
        
        if not repositories:
            # No repositories found, return empty
            return iter([]), {}
        
        all_prs = []
        
        # For each repository, fetch pull requests
        for repo in repositories:
            repo_id = repo.get("id")
            project_name = repo.get("project_name")  # Get project from repo
            if not repo_id or not project_name:
                continue
                
            # Create modified table_options with this repository_id and project
            repo_table_options = dict(table_options)
            repo_table_options["repository_id"] = repo_id
            repo_table_options["project"] = project_name  # Add project from repo
            
            # Fetch PRs for this repository
            try:
                prs_iterator, _ = self._read_pullrequests(start_offset, repo_table_options)
                prs = list(prs_iterator)
                all_prs.extend(prs)
            except Exception:
                # If a repository fails, continue with others
                continue
        
        # Return all PRs combined
        return iter(all_prs), {}

    def _read_refs(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read the `refs` snapshot table.

        Table options:
        - repository_id (optional): Repository ID or name to fetch refs from.
          If not provided, fetches refs from ALL repositories in the project.
        - filter (optional): Ref name prefix filter (e.g., 'heads/' for branches, 'tags/' for tags).
        """
        repository_id = table_options.get("repository_id")
        
        # If repository_id not provided, fetch from all repositories
        if not repository_id:
            return self._read_refs_all_repos(start_offset, table_options)

        # Determine which project to use
        project = table_options.get("project") or self.project
        if not project:
            raise ValueError("Project must be specified either at connection level or in table_options when repository_id is provided")

        url = f"{self.base_url}/{project}/_apis/git/repositories/{repository_id}/refs"
        
        params = {
            "api-version": "7.1",
        }
        
        # Add optional filter parameter
        ref_filter = table_options.get("filter")
        if ref_filter:
            params["filter"] = ref_filter

        response = self._session.get(url, params=params, timeout=30)
        if response.status_code != 200:
            raise RuntimeError(
                f"Azure DevOps API error for refs: {response.status_code} {response.text}"
            )

        response_json = response.json()
        if not isinstance(response_json, dict):
            raise ValueError(
                f"Unexpected response format for refs: {type(response_json).__name__}"
            )

        refs = response_json.get("value", [])
        if not isinstance(refs, list):
            raise ValueError(
                f"Unexpected 'value' format in refs response: {type(refs).__name__}"
            )

        records: list[dict[str, Any]] = []
        for ref_obj in refs:
            record: dict[str, Any] = dict(ref_obj)
            record["organization"] = self.organization
            record["project_name"] = project
            record["repository_id"] = repository_id

            # Ensure nested structs are None if absent
            if "creator" not in record or record["creator"] == {}:
                record["creator"] = None

            records.append(record)

        # Snapshot ingestion: return empty offset
        return iter(records), {}

    def _read_refs_all_repos(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read refs from ALL repositories in the project.
        
        This method:
        1. Fetches all repositories
        2. For each repository, fetches refs
        3. Combines all refs into a single result set
        """
        # First, get all repositories
        repos_iterator, _ = self._read_repositories({}, {})
        repositories = list(repos_iterator)
        
        if not repositories:
            # No repositories found, return empty
            return iter([]), {}
        
        all_refs = []
        
        # For each repository, fetch refs
        for repo in repositories:
            repo_id = repo.get("id")
            project_name = repo.get("project_name")  # Get project from repo
            if not repo_id or not project_name:
                continue
                
            # Create modified table_options with this repository_id and project
            repo_table_options = dict(table_options)
            repo_table_options["repository_id"] = repo_id
            repo_table_options["project"] = project_name  # Add project from repo
            
            # Fetch refs for this repository
            try:
                refs_iterator, _ = self._read_refs(start_offset, repo_table_options)
                refs = list(refs_iterator)
                all_refs.extend(refs)
            except Exception:
                # If a repository fails, continue with others
                continue
        
        # Return all refs combined
        return iter(all_refs), {}

    def _read_pushes(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read the `pushes` append table.

        Table options:
        - repository_id (optional): Repository ID or name to fetch pushes from.
          If not provided, fetches pushes from ALL repositories in the project.

        Supports pagination using $skip offset from start_offset.
        """
        repository_id = table_options.get("repository_id")
        
        # If repository_id not provided, fetch from all repositories
        if not repository_id:
            return self._read_pushes_all_repos(start_offset, table_options)

        # Determine which project to use
        project = table_options.get("project") or self.project
        if not project:
            raise ValueError("Project must be specified either at connection level or in table_options when repository_id is provided")

        url = f"{self.base_url}/{project}/_apis/git/repositories/{repository_id}/pushes"
        
        # Get pagination offset from start_offset
        skip = start_offset.get("skip", 0)
        top = 1000  # Fetch 1000 pushes per page
        
        params = {
            "api-version": "7.1",
            "$top": str(top),
            "$skip": str(skip),
        }

        response = self._session.get(url, params=params, timeout=30)
        if response.status_code != 200:
            raise RuntimeError(
                f"Azure DevOps API error for pushes: {response.status_code} {response.text}"
            )

        response_json = response.json()
        if not isinstance(response_json, dict):
            raise ValueError(
                f"Unexpected response format for pushes: {type(response_json).__name__}"
            )

        pushes = response_json.get("value", [])
        if not isinstance(pushes, list):
            raise ValueError(
                f"Unexpected 'value' format in pushes response: {type(pushes).__name__}"
            )

        records: list[dict[str, Any]] = []
        for push_obj in pushes:
            record: dict[str, Any] = dict(push_obj)
            record["organization"] = self.organization
            record["project_name"] = project
            record["repository_id"] = repository_id

            # Ensure nested structs are None if absent
            if "pushedBy" not in record or record["pushedBy"] == {}:
                record["pushedBy"] = None

            records.append(record)

        # Update offset for next page
        new_offset = {}
        if len(pushes) == top:
            # More data likely available
            new_offset["skip"] = skip + top
        
        return iter(records), new_offset

    def _read_pushes_all_repos(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read pushes from ALL repositories in the project.
        
        This method:
        1. Fetches all repositories
        2. For each repository, fetches pushes
        3. Combines all pushes into a single result set
        """
        # First, get all repositories
        repos_iterator, _ = self._read_repositories({}, {})
        repositories = list(repos_iterator)
        
        if not repositories:
            # No repositories found, return empty
            return iter([]), {}
        
        all_pushes = []
        
        # For each repository, fetch pushes
        for repo in repositories:
            repo_id = repo.get("id")
            project_name = repo.get("project_name")  # Get project from repo
            if not repo_id or not project_name:
                continue
                
            # Create modified table_options with this repository_id and project
            repo_table_options = dict(table_options)
            repo_table_options["repository_id"] = repo_id
            repo_table_options["project"] = project_name  # Add project from repo
            
            # Fetch pushes for this repository
            try:
                pushes_iterator, _ = self._read_pushes(start_offset, repo_table_options)
                pushes = list(pushes_iterator)
                all_pushes.extend(pushes)
            except Exception:
                # If a repository fails, continue with others
                continue
        
        # Return all pushes combined
        return iter(all_pushes), {}

    def _read_users(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read the `users` snapshot table.

        This implementation lists all user identities in the Azure DevOps organization
        using the Graph API:

            GET https://vssps.dev.azure.com/{organization}/_apis/graph/users?api-version=7.1-preview.1

        Note: This API uses a different base URL (vssps.dev.azure.com) and operates at
        organization level (does not require project parameter).

        The API uses continuation token-based pagination via response headers.

        The returned JSON objects are enriched with connector-derived fields:
            - organization: The organization name from connection config.
        """
        url = f"{self.vssps_base_url}/_apis/graph/users"
        params = {
            "api-version": "7.1-preview.1",
        }

        # Add continuation token from start_offset if present
        continuation_token = (start_offset or {}).get("continuationToken")
        if continuation_token:
            params["continuationToken"] = continuation_token

        response = self._session.get(url, params=params, timeout=30)
        if response.status_code != 200:
            raise RuntimeError(
                f"Azure DevOps API error for users: {response.status_code} {response.text}"
            )

        response_json = response.json()
        if not isinstance(response_json, dict):
            raise ValueError(
                f"Unexpected response format for users: {type(response_json).__name__}"
            )

        users = response_json.get("value", [])
        if not isinstance(users, list):
            raise ValueError(
                f"Unexpected 'value' format in users response: {type(users).__name__}"
            )

        records: list[dict[str, Any]] = []
        for user_obj in users:
            # Shallow-copy the raw JSON and add connector-derived fields
            record: dict[str, Any] = dict(user_obj)
            record["organization"] = self.organization

            # Ensure nested structs that are absent are represented as None, not {}
            if "_links" not in record or record["_links"] == {}:
                record["_links"] = None

            records.append(record)

        # Check for continuation token in response headers
        new_offset = {}
        next_token = response.headers.get("X-MS-ContinuationToken")
        if next_token:
            # More data available, return continuation token
            new_offset["continuationToken"] = next_token

        # Snapshot ingestion: return empty offset when no more pages
        return iter(records), new_offset

