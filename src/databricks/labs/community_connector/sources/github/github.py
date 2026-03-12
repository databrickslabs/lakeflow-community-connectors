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
        }

        if table_name not in reader_map:
            raise ValueError(f"Unsupported table: {table_name!r}")
        return reader_map[table_name](start_offset, table_options)

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

        Returns start_offset (signalling "no more data") when either:
        - No records were produced, or
        - The (capped) cursor has not advanced beyond current_cursor.
        """
        if not records and start_offset:
            return start_offset

        if not next_cursor:
            return start_offset if start_offset else {}

        next_cursor = min(next_cursor, self._init_time)

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
            record: dict[str, Any] = dict(issue)
            record["repository_owner"] = owner
            record["repository_name"] = repo
            records.append(record)

            updated_at = record.get("updated_at")
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
            record: dict[str, Any] = dict(pr)
            record["repository_owner"] = owner
            record["repository_name"] = repo
            records.append(record)

            updated_at = record.get("updated_at")
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
            record: dict[str, Any] = dict(comment)
            record["repository_owner"] = owner
            record["repository_name"] = repo
            records.append(record)

            updated_at = record.get("updated_at")
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
            window_end_dt = datetime.strptime(window_cursor, ts_fmt) + timedelta(
                seconds=window_seconds
            )
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
            return iter(records), {}

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

        return iter(records), {}
