"""
Unit tests for the community connector clients.

These tests use mocking to test the client logic without making
actual API calls to Databricks.
"""

from unittest.mock import MagicMock, create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import RepoInfo
from databricks.sdk.service.pipelines import CreatePipelineResponse, StartUpdateResponse

from databricks.labs.community_connector_cli.config import RepoConfig, PipelineConfig
from databricks.labs.community_connector_cli.repo_client import RepoClient
from databricks.labs.community_connector_cli.pipeline_client import PipelineClient


class TestRepoClient:
    """Tests for RepoClient."""

    def test_create_repo_basic(self):
        """Test creating a repo with basic configuration."""
        mock_workspace_client = create_autospec(WorkspaceClient)

        mock_repo_info = MagicMock(spec=RepoInfo)
        mock_repo_info.id = 12345
        mock_repo_info.path = "/Repos/user@example.com/test-repo"
        mock_workspace_client.repos.create.return_value = mock_repo_info

        client = RepoClient(mock_workspace_client)

        config = RepoConfig(
            url="https://github.com/example/repo.git",
            provider="gitHub",
            path="/Repos/user@example.com/test-repo",
        )

        result = client.create(config)

        assert result.id == 12345
        assert result.path == "/Repos/user@example.com/test-repo"

        mock_workspace_client.repos.create.assert_called_once()
        call_kwargs = mock_workspace_client.repos.create.call_args.kwargs
        assert call_kwargs["url"] == "https://github.com/example/repo.git"
        assert call_kwargs["provider"] == "gitHub"

    def test_get_repo_path(self):
        """Test extracting the repo path from RepoInfo."""
        mock_workspace_client = create_autospec(WorkspaceClient)
        client = RepoClient(mock_workspace_client)

        mock_repo_info = MagicMock(spec=RepoInfo)
        mock_repo_info.path = "/Repos/test/path"

        assert client.get_repo_path(mock_repo_info) == "/Repos/test/path"


class TestPipelineClient:
    """Tests for PipelineClient."""

    def test_create_pipeline_basic(self):
        """Test creating a pipeline with basic configuration."""
        mock_workspace_client = create_autospec(WorkspaceClient)

        mock_response = MagicMock(spec=CreatePipelineResponse)
        mock_response.pipeline_id = "abc-123-def"
        mock_workspace_client.pipelines.create.return_value = mock_response

        client = PipelineClient(mock_workspace_client)

        config = PipelineConfig(
            name="Test Pipeline",
            catalog="main",
            target="test_schema",
            development=True,
        )

        result = client.create(config)

        assert result.pipeline_id == "abc-123-def"
        mock_workspace_client.pipelines.create.assert_called_once()

    def test_create_pipeline_with_source_name(self):
        """Test creating a pipeline with source_name parameter."""
        mock_workspace_client = create_autospec(WorkspaceClient)

        mock_response = MagicMock(spec=CreatePipelineResponse)
        mock_response.pipeline_id = "xyz-789"
        mock_workspace_client.pipelines.create.return_value = mock_response

        client = PipelineClient(mock_workspace_client)

        config = PipelineConfig(name="GitHub Connector")

        result = client.create(
            config,
            repo_path="/Repos/user/repo",
            source_name="github",
        )

        assert result.pipeline_id == "xyz-789"

    def test_start_pipeline(self):
        """Test starting a pipeline."""
        mock_workspace_client = create_autospec(WorkspaceClient)

        mock_response = MagicMock(spec=StartUpdateResponse)
        mock_response.update_id = "update-123"
        mock_workspace_client.pipelines.start_update.return_value = mock_response

        client = PipelineClient(mock_workspace_client)

        result = client.start("pipeline-id")

        assert result.update_id == "update-123"
        mock_workspace_client.pipelines.start_update.assert_called_once_with(
            pipeline_id="pipeline-id",
            full_refresh=False,
        )

    def test_start_pipeline_full_refresh(self):
        """Test starting a pipeline with full refresh."""
        mock_workspace_client = create_autospec(WorkspaceClient)

        mock_response = MagicMock(spec=StartUpdateResponse)
        mock_response.update_id = "update-456"
        mock_workspace_client.pipelines.start_update.return_value = mock_response

        client = PipelineClient(mock_workspace_client)

        result = client.start("pipeline-id", full_refresh=True)

        assert result.update_id == "update-456"
        mock_workspace_client.pipelines.start_update.assert_called_once_with(
            pipeline_id="pipeline-id",
            full_refresh=True,
        )


class TestConfigDefaults:
    """Tests for configuration default values."""

    def test_repo_config_defaults(self):
        """Test RepoConfig default values."""
        config = RepoConfig(url="https://github.com/test/repo.git")

        assert config.url == "https://github.com/test/repo.git"
        assert config.provider == "gitHub"
        assert config.path is None
        assert config.branch is None
        assert config.sparse_checkout is None
        assert config.exclude_root_files is None

    def test_pipeline_config_defaults(self):
        """Test PipelineConfig default values.

        Note: Most defaults are None because actual defaults come from
        default_config.yaml, not hardcoded in the dataclass.
        """
        config = PipelineConfig(name="Test")

        assert config.name == "Test"
        assert config.target is None
        assert config.catalog is None
        assert config.root_path is None
        assert config.channel is None
        assert config.continuous is None
        assert config.development is None
        assert config.serverless is None
        assert not config.libraries
        assert not config.clusters
        assert not config.configuration
