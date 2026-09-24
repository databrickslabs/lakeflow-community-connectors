# pylint: disable=too-many-lines
from datetime import datetime, timedelta, timezone
from typing import Iterator, Any

import requests
from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.github.github_schemas import (
    TABLE_SCHEMAS,
    TABLE_METADATA,
    SUPPORTED_TABLES,
)
from databricks.labs.community_connector.sources.github.github_utils import (
    PaginationOptions,
    parse_pagination_options,
    extract_next_link,
    apply_lookback,
    compute_next_cursor,
    get_cursor_from_offset,
    require_owner_repo,
    parse_file_filter,
    should_include_file,
    decode_blob_content,
)


class GithubLakeflowConnect(LakeflowConnect):
    def __init__(self, options: dict[str, str]) -> None:
        """
        Initialize the GitHub connector with connection-level options.

        Expected options:
            - token: Personal access token used for GitHub REST API authentication.
            - base_url (optional): Override for GitHub API base URL.
              Defaults to https://api.github.com.
        """
        token = options.get("token")
        if not token:
            raise ValueError("GitHub connector requires 'token' in options")

        self.base_url = options.get("base_url", "https://api.github.com").rstrip("/")
        self._init_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        # Configure a session with proper headers for GitHub REST API v3
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
            }
        )

    def list_tables(self) -> list[str]:
        """
        List names of all tables supported by this connector.

        """
        return SUPPORTED_TABLES.copy()

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        """
        Fetch the schema of a table.

        The schema is static and derived from the GitHub REST API documentation
        and connector design for the `issues` object.
        """
        if table_name not in TABLE_SCHEMAS:
            raise ValueError(f"Unsupported table: {table_name!r}")
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        """
        Fetch metadata for the given table.

        For `issues`:
            - ingestion_type: cdc
            - primary_keys: ["id"]
            - cursor_field: updated_at
        """
        if table_name not in TABLE_METADATA:
            raise ValueError(f"Unsupported table: {table_name!r}")
        return TABLE_METADATA[table_name]

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read records from a table and return raw JSON-like dictionaries.

        For the `issues` table this method:
            - Uses `/repos/{owner}/{repo}/issues` endpoint.
            - Supports incremental reads via the `since` query parameter mapped to `updated_at`.
            - Paginates using GitHub's `Link` header until all pages are read or
              a batch limit (if provided via table_options) is reached.

        Other tables follow similar patterns based on their documented endpoints.

        Required table_options for `issues`:
            - owner: Repository owner (user or organization).
            - repo: Repository name.

        Optional table_options:
            - state: Issue state filter (default: "all").
            - per_page: Page size (max 100, default 100).
            - start_date: Initial ISO 8601 timestamp for first run if no start_offset is provided.
            - lookback_seconds: Lookback window applied to the cursor at read time (default: 300).
            - max_records_per_batch: Optional cap on the number of records returned per
              read_table call for incremental tables (cdc, append). For CDC tables,
              records are truncated to this limit. For append-only tables using a
              sliding window (commits), this is best-effort. Does not apply to
              snapshot tables.
        """
        reader_map = {
            "issues": self._read_issues,
            "repositories": self._read_repositories,
            "pull_requests": self._read_pull_requests,
            "comments": self._read_comments,
            "commits": self._read_commits,
            "assignees": self._read_assignees,
            "branches": self._read_branches,
            "collaborators": self._read_collaborators,
            "organizations": self._read_organizations,
            "teams": self._read_teams,
            "users": self._read_users,
            "reviews": self._read_reviews,
            "repository_files": self._read_repository_files,
        }

        if table_name not in reader_map:
            raise ValueError(f"Unsupported table: {table_name!r}")
        return reader_map[table_name](start_offset, table_options)

    @staticmethod
    def _parse_ts(ts: str | None) -> datetime | None:
        """Parse an ISO 8601 timestamp to an aware datetime, or None on failure.

        Uses datetime comparison rather than lexical string compare so that
        timestamps with differing fractional-second precision (e.g.
        ``...:00Z`` vs ``...:00.123Z``) compare by wall-clock time.
        """
        if not ts:
            return None
        try:
            return datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            return None

    def _compute_next_offset(
        self,
        next_cursor: str | None,
        current_cursor: str | None,
        start_offset: dict | None,
        records: list,
    ) -> dict:
        """
        Decide the offset to return from a CDC read.

        The cursor is capped at ``_init_time`` (set once in ``__init__``) so
        that a single trigger run only drains data that existed when the
        connector was instantiated.  Any records arriving after that point
        are still emitted (the API has no upper-bound filter) but the
        checkpoint will not advance past ``_init_time``, guaranteeing
        termination.  The next trigger creates a fresh connector with a
        new ``_init_time``.

        Also guards against backward movement — when ``apply_lookback``
        widens the ``since`` filter, the API may return only records
        older than ``current_cursor``, producing a ``next_cursor`` that
        regresses.  In that case the offset is held at ``current_cursor``.

        Returns start_offset (signalling "no more data") when either:
        - No records were produced, or
        - The (capped, guarded) cursor has not advanced beyond current_cursor.
        """
        if not records and start_offset:
            return start_offset

        if not next_cursor:
            return start_offset if start_offset else {}

        next_dt = self._parse_ts(next_cursor)
        init_dt = self._parse_ts(self._init_time)
        current_dt = self._parse_ts(current_cursor)

        if next_dt is not None and init_dt is not None and next_dt > init_dt:
            next_cursor = self._init_time
            next_dt = init_dt

        if current_dt is not None and next_dt is not None and next_dt < current_dt:
            next_cursor = current_cursor
            next_dt = current_dt

        if next_cursor == current_cursor:
            return start_offset if start_offset else {"cursor": next_cursor}

        return {"cursor": next_cursor}

    def _paginated_fetch(
        self,
        url: str,
        params: dict,
        pagination: PaginationOptions,
        entity_name: str,
    ) -> list[dict]:
        """
        Generic paginated fetch from GitHub API.

        Follows ``rel="next"`` Link headers until all pages are consumed.

        Args:
            url: The API endpoint URL.
            params: Query parameters for the request.
            pagination: Pagination configuration (used for per_page).
            entity_name: Name of the entity being fetched (for error messages).

        Returns:
            List of raw JSON objects from all fetched pages.
        """
        results: list[dict] = []
        next_url: str | None = url
        next_params: dict | None = params

        while next_url:
            response = self._session.get(next_url, params=next_params, timeout=30)
            if response.status_code != 200:
                raise RuntimeError(
                    f"GitHub API error for {entity_name}: {response.status_code} {response.text}"
                )

            data = response.json() or []
            if not isinstance(data, list):
                raise ValueError(
                    f"Unexpected response format for {entity_name}: {type(data).__name__}"
                )

            results.extend(data)

            link_header = response.headers.get("Link", "")
            next_url = extract_next_link(link_header)
            next_params = None

        return results

    def _read_issues(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """Internal implementation for reading the `issues` table."""
        owner, repo = require_owner_repo(table_options, "issues")
        pagination = parse_pagination_options(table_options)
        state = table_options.get("state", "all")
        cursor = get_cursor_from_offset(start_offset, table_options)

        url = f"{self.base_url}/repos/{owner}/{repo}/issues"
        params = {
            "state": state,
            "per_page": pagination.per_page,
            "sort": "updated",
            "direction": "asc",
        }
        since = apply_lookback(cursor, pagination.lookback_seconds)
        if since:
            params["since"] = since

        raw_issues = self._paginated_fetch(url, params, pagination, "issues")

        max_records = pagination.max_records_per_batch
        records: list[dict[str, Any]] = []
        max_updated_at: str | None = None

        for issue in raw_issues:
            updated_at = issue.get("updated_at")

            # Skip rows at or below the stored cursor so a no-new-rows batch is
            # empty and the offset advances.
            if cursor and isinstance(updated_at, str) and updated_at <= cursor:
                continue

            record: dict[str, Any] = dict(issue)
            record["repository_owner"] = owner
            record["repository_name"] = repo
            records.append(record)

            if isinstance(updated_at, str):
                if max_updated_at is None or updated_at > max_updated_at:
                    max_updated_at = updated_at

            if max_records is not None and len(records) >= max_records:
                break

        next_cursor = compute_next_cursor(max_updated_at, cursor)
        next_offset = self._compute_next_offset(
            next_cursor, cursor, start_offset, records
        )

        return iter(records), next_offset

    def _read_repositories(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read the `repositories` snapshot table.

        This implementation lists repositories for a given user or organization
        using the GitHub REST API:

            - GET /users/{username}/repos
            - GET /orgs/{org}/repos

        The returned JSON objects already have the full repository shape described
        in the connector schema. We add connector-derived fields:
            - repository_owner: owner.login
            - repository_name: name

        Required table_options:
            - Either:
              - owner: GitHub username (for /users/{username}/repos)
              - or org: GitHub organization login (for /orgs/{org}/repos)

        Optional table_options:
            - per_page: Page size (max 100, default 100).
        """
        owner = table_options.get("owner")
        org = table_options.get("org")

        if owner and org:
            raise ValueError(
                "table_configuration for 'repositories' must not include both "
                "'owner' and 'org'; specify only one."
            )
        if not owner and not org:
            raise ValueError(
                "table_configuration for 'repositories' must include either "
                "'owner' (username) or 'org' (organization login)"
            )

        pagination = parse_pagination_options(table_options)

        if org:
            url = f"{self.base_url}/orgs/{org}/repos"
        else:
            url = f"{self.base_url}/users/{owner}/repos"

        params = {"per_page": pagination.per_page}

        raw_repos = self._paginated_fetch(url, params, pagination, "repositories")

        records: list[dict[str, Any]] = []
        for repo_obj in raw_repos:
            record: dict[str, Any] = dict(repo_obj)
            owner_obj = repo_obj.get("owner") or {}
            record["repository_owner"] = owner_obj.get("login")
            record["repository_name"] = repo_obj.get("name")
            records.append(record)

        return iter(records), {}

    def _read_pull_requests(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read the `pull_requests` cdc table using:
            GET /repos/{owner}/{repo}/pulls

        Incremental behaviour mirrors issues using updated_at as a cursor,
        but for now this implementation always performs a forward read
        from the provided (optional) cursor.
        """
        owner, repo = require_owner_repo(table_options, "pull_requests")
        pagination = parse_pagination_options(table_options)
        state = table_options.get("state", "all")
        cursor = get_cursor_from_offset(start_offset, table_options)

        url = f"{self.base_url}/repos/{owner}/{repo}/pulls"
        params = {
            "state": state,
            "per_page": pagination.per_page,
            "sort": "updated",
            "direction": "asc",
        }
        since = apply_lookback(cursor, pagination.lookback_seconds)
        if since:
            params["since"] = since

        raw_prs = self._paginated_fetch(url, params, pagination, "pull_requests")

        max_records = pagination.max_records_per_batch
        records: list[dict[str, Any]] = []
        max_updated_at: str | None = None

        for pr in raw_prs:
            updated_at = pr.get("updated_at")

            # GET /pulls ignores `since`, so filter client-side: skip rows at or
            # below the stored cursor so a no-new-rows batch is empty and the
            # offset advances.
            if cursor and isinstance(updated_at, str) and updated_at <= cursor:
                continue

            record: dict[str, Any] = dict(pr)
            record["repository_owner"] = owner
            record["repository_name"] = repo
            records.append(record)

            if isinstance(updated_at, str):
                if max_updated_at is None or updated_at > max_updated_at:
                    max_updated_at = updated_at

            if max_records is not None and len(records) >= max_records:
                break

        next_cursor = compute_next_cursor(max_updated_at, cursor)
        next_offset = self._compute_next_offset(
            next_cursor, cursor, start_offset, records
        )

        return iter(records), next_offset

    def _read_comments(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read the `comments` cdc table using:
            GET /repos/{owner}/{repo}/issues/comments
        """
        owner, repo = require_owner_repo(table_options, "comments")
        pagination = parse_pagination_options(table_options)
        cursor = get_cursor_from_offset(start_offset, table_options)

        url = f"{self.base_url}/repos/{owner}/{repo}/issues/comments"
        params = {
            "per_page": pagination.per_page,
            "sort": "updated",
            "direction": "asc",
        }
        since = apply_lookback(cursor, pagination.lookback_seconds)
        if since:
            params["since"] = since

        raw_comments = self._paginated_fetch(url, params, pagination, "comments")

        max_records = pagination.max_records_per_batch
        records: list[dict[str, Any]] = []
        max_updated_at: str | None = None

        for comment in raw_comments:
            updated_at = comment.get("updated_at")

            # Skip rows at or below the stored cursor so a no-new-rows batch is
            # empty and the offset advances.
            if cursor and isinstance(updated_at, str) and updated_at <= cursor:
                continue

            record: dict[str, Any] = dict(comment)
            record["repository_owner"] = owner
            record["repository_name"] = repo
            records.append(record)

            if isinstance(updated_at, str):
                if max_updated_at is None or updated_at > max_updated_at:
                    max_updated_at = updated_at

            if max_records is not None and len(records) >= max_records:
                break

        next_cursor = compute_next_cursor(max_updated_at, cursor)
        next_offset = self._compute_next_offset(
            next_cursor, cursor, start_offset, records
        )

        return iter(records), next_offset

    def _find_oldest_commit_date(self, owner: str, repo: str) -> str | None:
        """Discover the committer date of the oldest commit in the repo.

        Uses two lightweight API calls: one ``per_page=1`` request to read the
        ``rel="last"`` Link header, then one request to fetch that last page.
        """
        url = f"{self.base_url}/repos/{owner}/{repo}/commits"
        resp = self._session.get(url, params={"per_page": 1}, timeout=30)
        if resp.status_code != 200:
            return None

        link_header = resp.headers.get("Link", "")
        last_url = None
        for part in link_header.split(","):
            section = part.strip()
            if 'rel="last"' in section:
                start = section.find("<")
                end = section.find(">", start + 1)
                if start != -1 and end != -1:
                    last_url = section[start + 1 : end]

        if not last_url:
            # Only one page — the single commit in the initial response is the oldest.
            data = resp.json()
            if data:
                committer = (data[-1].get("commit") or {}).get("committer") or {}
                return committer.get("date")
            return None

        last_resp = self._session.get(last_url, timeout=30)
        if last_resp.status_code != 200:
            return None
        data = last_resp.json()
        if data:
            committer = (data[-1].get("commit") or {}).get("committer") or {}
            return committer.get("date")
        return None

    def _read_commits(  # pylint: disable=too-many-locals
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read the ``commits`` append-only table using:
            GET /repos/{owner}/{repo}/commits

        The commits API returns results newest-first with no sort parameter.
        ``since``/``until`` filter on **committer date**.  This method uses a
        sliding time-window to scope each API query, but compacts multiple
        consecutive windows into a single batch until ``max_records_per_batch``
        is reached, the cursor hits ``_init_time``, or a window returns empty.

        When no ``start_date`` or prior offset is available the connector
        auto-discovers the oldest commit date via two lightweight API calls.
        """
        owner, repo = require_owner_repo(table_options, "commits")
        pagination = parse_pagination_options(table_options)
        cursor = get_cursor_from_offset(start_offset, table_options)

        if not cursor:
            cursor = self._find_oldest_commit_date(owner, repo)
        if not cursor:
            return iter([]), start_offset if start_offset else {}

        if cursor >= self._init_time:
            return iter([]), start_offset if start_offset else {}

        seven_days = 7 * 24 * 60 * 60
        try:
            window_seconds = int(table_options.get("window_seconds", str(seven_days)))
        except (TypeError, ValueError):
            window_seconds = seven_days

        max_records = pagination.max_records_per_batch
        ts_fmt = "%Y-%m-%dT%H:%M:%SZ"
        url = f"{self.base_url}/repos/{owner}/{repo}/commits"

        records: list[dict[str, Any]] = []
        window_cursor = cursor

        while window_cursor < self._init_time:
            window_dt = self._parse_ts(window_cursor)
            if window_dt is None:
                raise ValueError(
                    f"start_date / cursor {window_cursor!r} is not a valid "
                    "ISO 8601 timestamp (e.g. '2024-01-01' or "
                    "'2024-01-01T00:00:00Z')."
                )
            window_end_dt = window_dt + timedelta(seconds=window_seconds)
            window_end = min(window_end_dt.strftime(ts_fmt), self._init_time)

            params: dict[str, Any] = {
                "per_page": pagination.per_page,
                "since": window_cursor,
                "until": window_end,
            }

            raw_commits = self._paginated_fetch(url, params, pagination, "commits")

            if not raw_commits:
                window_cursor = window_end
                continue

            for commit_obj in raw_commits:
                commit_info = commit_obj.get("commit", {}) or {}
                commit_author = commit_info.get("author", {}) or {}
                commit_committer = commit_info.get("committer", {}) or {}

                record: dict[str, Any] = {
                    "sha": commit_obj.get("sha"),
                    "node_id": commit_obj.get("node_id"),
                    "repository_owner": owner,
                    "repository_name": repo,
                    "commit_message": commit_info.get("message"),
                    "commit_author_name": commit_author.get("name"),
                    "commit_author_email": commit_author.get("email"),
                    "commit_author_date": commit_author.get("date"),
                    "commit_committer_name": commit_committer.get("name"),
                    "commit_committer_email": commit_committer.get("email"),
                    "commit_committer_date": commit_committer.get("date"),
                    "html_url": commit_obj.get("html_url"),
                    "url": commit_obj.get("url"),
                    "author": commit_obj.get("author"),
                    "committer": commit_obj.get("committer"),
                }
                records.append(record)

            window_cursor = window_end

            if max_records is not None and len(records) >= max_records:
                break

        if not records:
            return iter([]), start_offset if start_offset else {"cursor": cursor}

        end_offset: dict[str, Any] = {"cursor": window_cursor}
        if start_offset and start_offset == end_offset:
            return iter([]), start_offset
        return iter(records), end_offset

    def _read_assignees(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read the `assignees` snapshot table using:
            GET /repos/{owner}/{repo}/assignees
        """
        owner, repo = require_owner_repo(table_options, "assignees")
        pagination = parse_pagination_options(table_options)

        url = f"{self.base_url}/repos/{owner}/{repo}/assignees"
        params = {"per_page": pagination.per_page}

        raw_assignees = self._paginated_fetch(url, params, pagination, "assignees")

        records: list[dict[str, Any]] = []
        for assignee in raw_assignees:
            record: dict[str, Any] = {
                "repository_owner": owner,
                "repository_name": repo,
                "login": assignee.get("login"),
                "id": assignee.get("id"),
                "node_id": assignee.get("node_id"),
                "type": assignee.get("type"),
                "site_admin": assignee.get("site_admin"),
            }
            records.append(record)

        return iter(records), {}

    def _read_branches(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read the `branches` snapshot table using:
            GET /repos/{owner}/{repo}/branches
        """
        owner, repo = require_owner_repo(table_options, "branches")
        pagination = parse_pagination_options(table_options)

        url = f"{self.base_url}/repos/{owner}/{repo}/branches"
        params = {"per_page": pagination.per_page}

        raw_branches = self._paginated_fetch(url, params, pagination, "branches")

        records: list[dict[str, Any]] = []
        for branch in raw_branches:
            record: dict[str, Any] = {
                "repository_owner": owner,
                "repository_name": repo,
                "name": branch.get("name"),
                "commit": branch.get("commit"),
                "protected": branch.get("protected"),
                "protection_url": branch.get("protection_url"),
            }
            records.append(record)

        return iter(records), {}

    def _read_collaborators(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read the `collaborators` snapshot table using:
            GET /repos/{owner}/{repo}/collaborators
        """
        owner, repo = require_owner_repo(table_options, "collaborators")
        pagination = parse_pagination_options(table_options)

        url = f"{self.base_url}/repos/{owner}/{repo}/collaborators"
        params = {"per_page": pagination.per_page}

        raw_collaborators = self._paginated_fetch(url, params, pagination, "collaborators")

        records: list[dict[str, Any]] = []
        for collaborator in raw_collaborators:
            record: dict[str, Any] = {
                "repository_owner": owner,
                "repository_name": repo,
                "login": collaborator.get("login"),
                "id": collaborator.get("id"),
                "node_id": collaborator.get("node_id"),
                "type": collaborator.get("type"),
                "site_admin": collaborator.get("site_admin"),
                "permissions": collaborator.get("permissions"),
            }
            records.append(record)

        return iter(records), {}

    def _read_organizations(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read the `organizations` snapshot table.

        Instead of requiring an explicit `org` option, this method discovers
        organizations for the authenticated user using:

            - GET /user/orgs                  (list orgs the token can see)

        It intentionally does **not** expand each organization via
        `GET /orgs/{org}` to avoid additional permission requirements on
        the detail endpoint. The table therefore exposes the summary
        metadata returned directly by `GET /user/orgs`.
        """
        pagination = parse_pagination_options(table_options)

        url = f"{self.base_url}/user/orgs"
        params = {"per_page": pagination.per_page}

        raw_orgs = self._paginated_fetch(url, params, pagination, "organizations")

        records: list[dict[str, Any]] = []
        for org_summary in raw_orgs:
            if not isinstance(org_summary, dict):
                continue

            record: dict[str, Any] = {
                "id": org_summary.get("id"),
                "login": org_summary.get("login"),
                "node_id": org_summary.get("node_id"),
                "url": org_summary.get("url"),
                "repos_url": org_summary.get("repos_url"),
                "events_url": org_summary.get("events_url"),
                "hooks_url": org_summary.get("hooks_url"),
                "issues_url": org_summary.get("issues_url"),
                "members_url": org_summary.get("members_url"),
                "public_members_url": org_summary.get("public_members_url"),
                "avatar_url": org_summary.get("avatar_url"),
                "description": org_summary.get("description"),
            }
            records.append(record)

        return iter(records), {}

    def _read_teams(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read the `teams` snapshot table.

        Instead of requiring an explicit `org` option, this method discovers
        teams for the authenticated user using:

            - GET /user/teams                          (list teams user can see)
            - GET /orgs/{org}/teams/{team_slug}        (expand each team)

        The connector also adds `organization_login` to each record to match
        the declared schema.
        """
        pagination = parse_pagination_options(table_options)

        url = f"{self.base_url}/user/teams"
        params = {"per_page": pagination.per_page}

        raw_teams = self._paginated_fetch(url, params, pagination, "teams")

        records: list[dict[str, Any]] = []
        for team_summary in raw_teams:
            org_obj = team_summary.get("organization") or {}
            org_login = org_obj.get("login")
            team_slug = team_summary.get("slug")
            if not org_login or not team_slug:
                continue

            detail_url = f"{self.base_url}/orgs/{org_login}/teams/{team_slug}"
            detail_resp = self._session.get(detail_url, timeout=30)
            if detail_resp.status_code != 200:
                raise RuntimeError(
                    f"GitHub API error for team {org_login!r}/{team_slug!r}: "
                    f"{detail_resp.status_code} {detail_resp.text}"
                )

            team_obj = detail_resp.json() or {}
            if not isinstance(team_obj, dict):
                raise ValueError(
                    f"Unexpected response format for team detail: {type(team_obj).__name__}"
                )

            record: dict[str, Any] = dict(team_obj)
            record["organization_login"] = org_login
            records.append(record)

        return iter(records), {}

    def _read_users(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read the `users` snapshot table using:
            GET /user

        The connector now resolves the user from the authenticated context and
        no longer requires a `username` option. This returns metadata for the
        current authenticated user.
        """
        url = f"{self.base_url}/user"
        response = self._session.get(url, timeout=30)
        if response.status_code != 200:
            raise RuntimeError(
                f"GitHub API error for users: {response.status_code} {response.text}"
            )

        user_obj = response.json() or {}
        if not isinstance(user_obj, dict):
            raise ValueError(
                f"Unexpected response format for user: {type(user_obj).__name__}"
            )

        record: dict[str, Any] = dict(user_obj)
        return iter([record]), {}

    def _read_reviews(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read the `reviews` append-only table.

        Primary child API:
            - GET /repos/{owner}/{repo}/pulls/{pull_number}/reviews

        Parent listing when pull_number is not provided (see Step 3 guidance in
        the connector coding instructions about parent/child relationships):
            - GET /repos/{owner}/{repo}/pulls
              Then for each pull request, call the reviews API above and
              combine all reviews into a single logical table.
        """
        owner, repo = require_owner_repo(table_options, "reviews")
        pagination = parse_pagination_options(table_options)
        pull_number_opt = table_options.get("pull_number")

        max_records = pagination.max_records_per_batch
        records: list[dict[str, Any]] = []

        # Append table with no per-record cursor: emit the full set once at a
        # sentinel offset, then return empty so the stream terminates without
        # re-emitting.
        if start_offset and start_offset.get("cursor"):
            return iter([]), start_offset

        def _fetch_reviews_for_pull(pull_number: int) -> None:
            """Fetch reviews for a single pull request and append to records."""
            url = f"{self.base_url}/repos/{owner}/{repo}/pulls/{pull_number}/reviews"
            params = {"per_page": pagination.per_page}

            raw_reviews = self._paginated_fetch(
                url, params, pagination, f"reviews for PR #{pull_number}"
            )

            for review in raw_reviews:
                record: dict[str, Any] = dict(review)
                record["repository_owner"] = owner
                record["repository_name"] = repo
                record["pull_number"] = int(pull_number)
                records.append(record)

        if pull_number_opt is not None:
            try:
                pull_number_int = int(pull_number_opt)
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"table_options['pull_number'] must be an int-compatible value, "
                    f"got {pull_number_opt!r}"
                ) from exc

            _fetch_reviews_for_pull(pull_number_int)
            next_offset = {"cursor": self._init_time} if records else (start_offset or {})
            return iter(records), next_offset

        pr_state = table_options.get("state", "all")
        url = f"{self.base_url}/repos/{owner}/{repo}/pulls"
        params = {"state": pr_state, "per_page": pagination.per_page}

        raw_prs = self._paginated_fetch(url, params, pagination, "pull_requests")

        for pr in raw_prs:
            if max_records is not None and len(records) >= max_records:
                break
            number = pr.get("number")
            if isinstance(number, int):
                _fetch_reviews_for_pull(number)

        next_offset = {"cursor": self._init_time} if records else (start_offset or {})
        return iter(records), next_offset

    # ------------------------------------------------------------------
    # repository_files (source code contents)
    # ------------------------------------------------------------------

    def _get_default_branch(self, owner: str, repo: str) -> str:
        """Return the repository's default branch name (falls back to 'main')."""
        url = f"{self.base_url}/repos/{owner}/{repo}"
        resp = self._session.get(url, timeout=30)
        if resp.status_code != 200:
            raise RuntimeError(
                f"GitHub API error resolving default branch for "
                f"{owner}/{repo}: {resp.status_code} {resp.text}"
            )
        return (resp.json() or {}).get("default_branch") or "main"

    def _resolve_commit_tree(
        self, owner: str, repo: str, ref: str
    ) -> tuple[str | None, str]:
        """Resolve a ref (branch, tag, or sha) to its commit sha and tree sha.

        Uses ``GET /repos/{owner}/{repo}/commits/{ref}`` (Contents: Read).
        """
        url = f"{self.base_url}/repos/{owner}/{repo}/commits/{ref}"
        resp = self._session.get(url, timeout=30)
        if resp.status_code != 200:
            raise RuntimeError(
                f"GitHub API error resolving ref {ref!r} for "
                f"{owner}/{repo}: {resp.status_code} {resp.text}"
            )
        data = resp.json() or {}
        commit_sha = data.get("sha")
        tree_sha = ((data.get("commit") or {}).get("tree") or {}).get("sha")
        if not tree_sha:
            raise ValueError(
                f"Could not resolve tree sha for ref {ref!r} in {owner}/{repo}"
            )
        return commit_sha, tree_sha

    def _walk_tree(
        self, owner: str, repo: str, tree_sha: str, prefix: str = ""
    ) -> list[dict]:
        """Recursively walk a git tree one level at a time.

        Fallback used when the recursive tree response is truncated (very large
        repos). Builds full paths from nested, single-level tree responses.
        """
        url = f"{self.base_url}/repos/{owner}/{repo}/git/trees/{tree_sha}"
        resp = self._session.get(url, timeout=30)
        if resp.status_code != 200:
            raise RuntimeError(
                f"GitHub API error walking tree {tree_sha!r} for "
                f"{owner}/{repo}: {resp.status_code} {resp.text}"
            )
        data = resp.json() or {}
        blobs: list[dict] = []
        for entry in data.get("tree", []) or []:
            full_path = f"{prefix}{entry.get('path')}"
            entry_type = entry.get("type")
            if entry_type == "blob":
                blob = dict(entry)
                blob["path"] = full_path
                blobs.append(blob)
            elif entry_type == "tree" and entry.get("sha"):
                blobs.extend(
                    self._walk_tree(owner, repo, entry["sha"], prefix=f"{full_path}/")
                )
        return blobs

    def _list_tree_blobs(self, owner: str, repo: str, tree_sha: str) -> list[dict]:
        """List all blob entries under a tree.

        Tries the single-call recursive tree endpoint first; if GitHub reports
        the response as truncated, falls back to a per-directory walk so no
        files are silently dropped.
        """
        url = f"{self.base_url}/repos/{owner}/{repo}/git/trees/{tree_sha}"
        resp = self._session.get(url, params={"recursive": "1"}, timeout=30)
        if resp.status_code != 200:
            raise RuntimeError(
                f"GitHub API error listing tree {tree_sha!r} for "
                f"{owner}/{repo}: {resp.status_code} {resp.text}"
            )
        data = resp.json() or {}
        if data.get("truncated"):
            return self._walk_tree(owner, repo, tree_sha)
        return [
            entry
            for entry in (data.get("tree", []) or [])
            if entry.get("type") == "blob"
        ]

    def _fetch_blob_content(
        self, owner: str, repo: str, blob_sha: str, include_binary: bool
    ) -> tuple[str | None, str | None, str | None, bool]:
        """Fetch and decode a single blob's content.

        Returns ``(content, content_base64, encoding, is_binary)``. Uses
        ``GET /repos/{owner}/{repo}/git/blobs/{sha}`` (Contents: Read).
        """
        url = f"{self.base_url}/repos/{owner}/{repo}/git/blobs/{blob_sha}"
        resp = self._session.get(url, timeout=30)
        if resp.status_code != 200:
            raise RuntimeError(
                f"GitHub API error fetching blob {blob_sha!r} for "
                f"{owner}/{repo}: {resp.status_code} {resp.text}"
            )
        data = resp.json() or {}
        return decode_blob_content(
            data.get("content"), data.get("encoding"), include_binary
        )

    def _read_repository_files(  # pylint: disable=too-many-locals
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read the ``repository_files`` snapshot table: source code contents at a ref.

        Read-only. Enumerates the git tree at a resolved commit and fetches each
        matching blob. All endpoints are GET, so a fine-grained PAT with only
        ``Contents: Read`` (+ mandatory ``Metadata: Read``), scoped to the
        selected repositories, is sufficient. Nothing here can write.

        Required table_options:
            - owner, repo.

        Optional table_options:
            - ref: Branch, tag, or commit sha. Defaults to the repo's default branch.
            - include_extensions: Comma-separated, e.g. "py,java,ts".
            - include_globs / exclude_globs: fnmatch patterns; excludes win.
            - max_file_bytes: Skip files larger than this (default 1_000_000).
              Skipped files still emit a metadata row with content = null.
            - include_binary: "true" to keep binary files as base64 (default false).
            - max_records_per_batch: Page size; when set, the read is split into
              deterministic index-based batches.
        """
        owner, repo = require_owner_repo(table_options, "repository_files")
        pagination = parse_pagination_options(table_options)
        file_filter = parse_file_filter(table_options)

        include_binary = str(
            table_options.get("include_binary", "false")
        ).strip().lower() in ("true", "1", "yes")
        try:
            max_file_bytes = int(table_options.get("max_file_bytes", 1_000_000))
        except (TypeError, ValueError):
            max_file_bytes = 1_000_000

        ref = table_options.get("ref") or self._get_default_branch(owner, repo)
        commit_sha, tree_sha = self._resolve_commit_tree(owner, repo, ref)

        blobs = [
            blob
            for blob in self._list_tree_blobs(owner, repo, tree_sha)
            if should_include_file(blob.get("path", ""), file_filter)
        ]
        blobs.sort(key=lambda blob: blob.get("path", ""))
        total = len(blobs)

        start_index = 0
        if start_offset and isinstance(start_offset, dict):
            try:
                start_index = int(start_offset.get("index", 0) or 0)
            except (TypeError, ValueError):
                start_index = 0

        max_records = pagination.max_records_per_batch
        end_index = (
            min(start_index + max_records, total)
            if max_records is not None
            else total
        )

        if start_index >= total:
            return iter([]), start_offset if start_offset else {"index": total}

        ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        host = "https://github.com" if self.base_url == "https://api.github.com" else None

        records: list[dict[str, Any]] = []
        for blob in blobs[start_index:end_index]:
            path = blob.get("path", "")
            blob_sha = blob.get("sha")
            size = blob.get("size") or 0

            content: str | None = None
            content_base64: str | None = None
            encoding: str | None = None
            is_binary: bool | None = None
            skipped_reason: str | None = None

            if size and size > max_file_bytes:
                skipped_reason = "size_exceeds_max_file_bytes"
            elif blob_sha:
                content, content_base64, encoding, is_binary = self._fetch_blob_content(
                    owner, repo, blob_sha, include_binary
                )
                if is_binary and not include_binary:
                    skipped_reason = "binary_excluded"

            records.append(
                {
                    "repository_owner": owner,
                    "repository_name": repo,
                    "path": path,
                    "ref": ref,
                    "commit_sha": commit_sha,
                    "blob_sha": blob_sha,
                    "size": size,
                    "mode": blob.get("mode"),
                    "encoding": encoding,
                    "is_binary": is_binary,
                    "skipped_reason": skipped_reason,
                    "content": content,
                    "content_base64": content_base64,
                    "html_url": (
                        f"{host}/{owner}/{repo}/blob/{commit_sha}/{path}"
                        if host and commit_sha
                        else None
                    ),
                    "ingested_at": ingested_at,
                }
            )

        if max_records is None:
            return iter(records), {}

        next_offset = {"index": end_index}
        if start_offset and start_offset == next_offset:
            return iter(records), start_offset
        return iter(records), next_offset
