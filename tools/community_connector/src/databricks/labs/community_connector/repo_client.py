"""
Repository client for Databricks workspace repos.

This module provides a client for creating and managing Git repos
in a Databricks workspace using the Databricks SDK.

API Reference: https://docs.databricks.com/api/workspace/repos/create
"""

from typing import Optional
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import RepoInfo, SparseCheckout

from databricks.labs.community_connector.config import RepoConfig


class RepoClient:
    """
    Client for managing Databricks workspace repos.

    This client wraps the Databricks SDK repos API to provide
    a simplified interface for creating and managing repos.
    """

    def __init__(self, workspace_client: Optional[WorkspaceClient] = None):
        """
        Initialize the RepoClient.

        Args:
            workspace_client: Optional WorkspaceClient instance. If not provided,
                            a new client will be created using default authentication.
        """
        self._client = workspace_client or WorkspaceClient()

    @property
    def client(self) -> WorkspaceClient:
        """Get the underlying WorkspaceClient."""
        return self._client

    def create(self, config: RepoConfig) -> RepoInfo:
        """
        Create a new repo in the Databricks workspace.

        If a branch is specified in config, the repo will be updated to
        checkout that branch after creation.

        Args:
            config: RepoConfig containing the repository configuration.

        Returns:
            RepoInfo object containing information about the created repo.

        API Reference: https://docs.databricks.com/api/workspace/repos/create
        """
        create_kwargs = self._build_create_payload(config)
        repo_info = self._client.repos.create(**create_kwargs)

        if not repo_info:
            raise RuntimeError("Failed to create repo: no response from API")

        # Store the repo_id for later use
        repo_id = repo_info.id

        # If a specific branch is requested, update the repo to that branch
        # (The create API doesn't accept branch, so we need a separate update call)
        if config.branch and repo_id:
            self.update(repo_id=repo_id, branch=config.branch)
            # Fetch the updated repo info to ensure we have complete data
            repo_info = self.get(repo_id=repo_id)

        return repo_info

    def _build_create_payload(self, config: RepoConfig) -> dict:
        """
        Build the payload for the repos.create API call.

        Args:
            config: RepoConfig containing the repository configuration.

        Returns:
            Dictionary of keyword arguments for the create API call.
        """
        payload = {
            "url": config.url,
            "provider": config.provider,
        }

        if config.path:
            payload["path"] = config.path

        # Add sparse checkout configuration if provided
        if config.sparse_checkout:
            patterns = config.sparse_checkout.get("patterns", [])
            if patterns:
                payload["sparse_checkout"] = SparseCheckout(patterns=patterns)

        return payload

    def get(self, repo_id: int) -> RepoInfo:
        """
        Get information about a repo.

        Args:
            repo_id: The ID of the repo.

        Returns:
            RepoInfo object containing information about the repo.
        """
        return self._client.repos.get(repo_id=repo_id)

    def update(self, repo_id: int, branch: Optional[str] = None, tag: Optional[str] = None) -> RepoInfo:
        """
        Update a repo to a different branch or tag.

        Args:
            repo_id: The ID of the repo to update.
            branch: Branch to checkout.
            tag: Tag to checkout.

        Returns:
            RepoInfo object containing updated information about the repo.
        """
        return self._client.repos.update(repo_id=repo_id, branch=branch, tag=tag)

    def list(self, path_prefix: Optional[str] = None, next_page_token: Optional[str] = None):
        """
        List repos in the workspace.

        Args:
            path_prefix: Optional path prefix to filter repos.
            next_page_token: Token for pagination.

        Returns:
            Iterator of RepoInfo objects.
        """
        return self._client.repos.list(path_prefix=path_prefix, next_page_token=next_page_token)

    def get_repo_path(self, repo_info: RepoInfo) -> Optional[str]:
        """
        Get the workspace path of a repo.

        Args:
            repo_info: RepoInfo object from create or get operations.

        Returns:
            The workspace path of the repo, or None if not available.
        """
        if repo_info is None:
            return None
        return getattr(repo_info, 'path', None)

