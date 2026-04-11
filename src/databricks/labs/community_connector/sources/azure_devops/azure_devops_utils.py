"""Utility functions for the Azure DevOps connector.

This module contains helper functions for API interaction, project/repo
resolution, and the PR auto-discovery pattern used across multiple tables.
"""

from typing import Any, Callable

import requests


def api_get(
    session: requests.Session,
    url: str,
    params: dict[str, str],
    label: str,
) -> dict:
    """Make a GET request and return the parsed JSON response.

    Raises RuntimeError on non-200 status codes with a descriptive
    message that includes *label* (e.g. ``"commits"``).
    """
    response = session.get(url, params=params, timeout=30)
    if response.status_code != 200:
        raise RuntimeError(
            f"Azure DevOps API error for {label}: "
            f"{response.status_code} {response.text}"
        )
    return response.json()


def api_get_list(
    session: requests.Session,
    url: str,
    params: dict[str, str],
    label: str,
    key: str = "value",
) -> list[dict]:
    """GET a list endpoint and return the ``value`` array."""
    data = api_get(session, url, params, label)
    return data.get(key, [])


def resolve_projects(
    session: requests.Session,
    base_url: str,
    project: str | None,
) -> list[str]:
    """Return a list of project names to iterate over.

    If *project* is given, return it as a single-element list.
    Otherwise fetch all projects from the organization.
    """
    if project:
        return [project]
    items = api_get_list(
        session,
        f"{base_url}/_apis/projects",
        {"api-version": "7.1"},
        "projects",
    )
    return [p["name"] for p in items if p.get("name")]


def fetch_repos(
    session: requests.Session,
    base_url: str,
    project: str,
) -> list[dict]:
    """Fetch all repositories for a single project."""
    return api_get_list(
        session,
        f"{base_url}/{project}/_apis/git/repositories",
        {"api-version": "7.1"},
        "repositories",
    )


def fetch_prs(
    session: requests.Session,
    base_url: str,
    project: str,
    repo_id: str,
) -> list[dict]:
    """Fetch all pull requests (all statuses) for a repo."""
    url = (
        f"{base_url}/{project}/_apis/git/repositories"
        f"/{repo_id}/pullrequests"
    )
    return api_get_list(
        session,
        url,
        {"api-version": "7.1", "searchCriteria.status": "all"},
        "pullrequests",
    )


def nullify_empty(record: dict, *keys: str) -> None:
    """Replace missing or empty-dict nested structs with ``None``."""
    for key in keys:
        if key not in record or record[key] == {}:
            record[key] = None


# --------------------------------------------------------------------------- #
# PR sub-resource auto-discovery helper
# --------------------------------------------------------------------------- #

def for_each_pr(
    session: requests.Session,
    base_url: str,
    projects: list[str],
    table_options: dict[str, str],
    callback: Callable[
        [str, str, int], list[dict[str, Any]]
    ],
) -> list[dict[str, Any]]:
    """Iterate over pull requests and collect records via *callback*.

    The three-tier auto-discovery pattern used by threads, work-items,
    commits, and reviewers tables:

    1. If ``repository_id`` **and** ``pullrequest_id`` are in
       *table_options* → call *callback* once.
    2. If only ``repository_id`` → iterate all PRs in that repo.
    3. If neither → iterate all repos in each project, then all PRs.

    *callback(project, repo_id, pr_id)* must return a list of enriched
    record dicts for that single PR.
    """
    repo_id = table_options.get("repository_id")
    pr_id = table_options.get("pullrequest_id")

    all_records: list[dict[str, Any]] = []

    for project in projects:
        if repo_id and pr_id:
            all_records.extend(
                callback(project, repo_id, int(pr_id))
            )
        else:
            target_ids = (
                [repo_id] if repo_id
                else [r["id"] for r in
                      fetch_repos(session, base_url, project)
                      if r.get("id")]
            )
            for rid in target_ids:
                try:
                    prs = fetch_prs(
                        session, base_url, project, rid
                    )
                except RuntimeError:
                    continue
                for pr in prs:
                    pid = pr.get("pullRequestId")
                    if pid is None:
                        continue
                    try:
                        all_records.extend(
                            callback(project, rid, pid)
                        )
                    except RuntimeError:
                        continue

    return all_records
