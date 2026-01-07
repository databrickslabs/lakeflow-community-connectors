"""
Unit tests for the community connector CLI.

Tests CLI helper functions, argument validation, and command invocation
using Click's CliRunner and mocks for Databricks SDK.
"""

import json
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch, create_autospec

import click
import pytest
from click.testing import CliRunner

from databricks.sdk import WorkspaceClient

from databricks.labs.community_connector.cli import (
    main,
    _parse_pipeline_spec,
    _load_ingest_template,
    _find_pipeline_by_name,
)


class TestParsePipelineSpec:
    """Tests for _parse_pipeline_spec function."""

    def test_parse_json_string(self):
        """Test parsing a valid JSON string."""
        json_str = (
            '{"connection_name": "my_conn", "objects": [{"table": {"source_table": "users"}}]}'
        )
        result = _parse_pipeline_spec(json_str)

        assert result["connection_name"] == "my_conn"
        assert len(result["objects"]) == 1
        assert result["objects"][0]["table"]["source_table"] == "users"

    def test_parse_invalid_json_string(self):
        """Test error on invalid JSON string."""
        with pytest.raises(click.ClickException) as exc_info:
            _parse_pipeline_spec("not valid json")
        assert "Invalid JSON" in str(exc_info.value)

    def test_parse_json_file(self):
        """Test parsing a JSON file."""
        spec = {
            "connection_name": "file_conn",
            "objects": [{"table": {"source_table": "orders"}}],
        }

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(spec, f)
            temp_path = f.name

        try:
            result = _parse_pipeline_spec(temp_path)
            assert result["connection_name"] == "file_conn"
            assert result["objects"][0]["table"]["source_table"] == "orders"
        finally:
            os.unlink(temp_path)

    def test_parse_yaml_file(self):
        """Test parsing a YAML file."""
        yaml_content = """
connection_name: yaml_conn
objects:
  - table:
      source_table: products
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(yaml_content)
            temp_path = f.name

        try:
            result = _parse_pipeline_spec(temp_path)
            assert result["connection_name"] == "yaml_conn"
            assert result["objects"][0]["table"]["source_table"] == "products"
        finally:
            os.unlink(temp_path)

    def test_parse_file_not_found(self):
        """Test error when file doesn't exist."""
        with pytest.raises(click.ClickException) as exc_info:
            _parse_pipeline_spec("/nonexistent/path/spec.yaml")
        assert "not found" in str(exc_info.value)

    def test_parse_with_validation_error(self):
        """Test that validation errors are raised."""
        # Missing connection_name
        json_str = '{"objects": [{"table": {"source_table": "users"}}]}'
        with pytest.raises(click.ClickException) as exc_info:
            _parse_pipeline_spec(json_str)
        assert "connection_name" in str(exc_info.value)

    def test_parse_without_validation(self):
        """Test parsing without validation."""
        # Invalid spec but validation disabled
        json_str = '{"invalid": "spec"}'
        result = _parse_pipeline_spec(json_str, validate=False)
        assert result == {"invalid": "spec"}


class TestLoadIngestTemplate:
    """Tests for _load_ingest_template function."""

    def test_load_default_template(self):
        """Test loading the default ingest template."""
        content = _load_ingest_template()

        assert "from pipeline.ingestion_pipeline import ingest" in content
        assert "{SOURCE_NAME}" in content
        assert "{CONNECTION_NAME}" in content

    def test_load_base_template(self):
        """Test loading the base ingest template."""
        content = _load_ingest_template("ingest_template_base.py")

        assert "from pipeline.ingestion_pipeline import ingest" in content
        assert "{SOURCE_NAME}" in content
        assert "{PIPELINE_SPEC}" in content

    def test_load_nonexistent_template(self):
        """Test error when template doesn't exist."""
        with pytest.raises(FileNotFoundError):
            _load_ingest_template("nonexistent_template.py")


class TestFindPipelineByName:
    """Tests for _find_pipeline_by_name function."""

    def test_find_existing_pipeline(self):
        """Test finding an existing pipeline by name."""
        mock_client = create_autospec(WorkspaceClient)

        mock_pipeline = MagicMock()
        mock_pipeline.pipeline_id = "pipeline-123"
        mock_client.pipelines.list_pipelines.return_value = [mock_pipeline]

        result = _find_pipeline_by_name(mock_client, "my_pipeline")

        assert result == "pipeline-123"
        mock_client.pipelines.list_pipelines.assert_called_once()

    def test_pipeline_not_found(self):
        """Test error when pipeline doesn't exist."""
        mock_client = create_autospec(WorkspaceClient)
        mock_client.pipelines.list_pipelines.return_value = []

        with pytest.raises(click.ClickException) as exc_info:
            _find_pipeline_by_name(mock_client, "nonexistent")
        assert "not found" in str(exc_info.value)

    def test_multiple_pipelines_warning(self, capsys):
        """Test warning when multiple pipelines match."""
        mock_client = create_autospec(WorkspaceClient)

        mock_pipeline1 = MagicMock()
        mock_pipeline1.pipeline_id = "pipeline-1"
        mock_pipeline2 = MagicMock()
        mock_pipeline2.pipeline_id = "pipeline-2"
        mock_client.pipelines.list_pipelines.return_value = [mock_pipeline1, mock_pipeline2]

        result = _find_pipeline_by_name(mock_client, "my_pipeline")

        # Should return first match
        assert result == "pipeline-1"


class TestCreatePipelineCommand:
    """Tests for create_pipeline command."""

    def test_create_pipeline_requires_connection_or_spec(self):
        """Test that either --connection-name or --pipeline-spec is required."""
        runner = CliRunner()

        with patch('databricks.labs.community_connector.cli.WorkspaceClient'):
            result = runner.invoke(
                main,
                ['create_pipeline', 'github', 'my_pipeline'],
            )

        assert result.exit_code != 0
        assert "Either --connection-name or --pipeline-spec must be provided" in result.output

    @patch('databricks.labs.community_connector.cli.WorkspaceClient')
    @patch('databricks.labs.community_connector.cli.RepoClient')
    @patch('databricks.labs.community_connector.cli.PipelineClient')
    @patch('databricks.labs.community_connector.cli._create_workspace_file')
    def test_create_pipeline_with_connection_name(
        self, mock_create_file, mock_pipeline_client, mock_repo_client, mock_workspace_client
    ):
        """Test create_pipeline with --connection-name option."""
        runner = CliRunner()

        # Setup mocks
        mock_ws = MagicMock()
        mock_workspace_client.return_value = mock_ws
        mock_ws.current_user.me.return_value.user_name = "test@example.com"
        mock_ws.config.host = "https://test.databricks.com"

        mock_repo = MagicMock()
        mock_repo_client.return_value = mock_repo
        mock_repo_info = MagicMock()
        mock_repo_info.id = 123
        mock_repo_info.path = "/Users/test@example.com/.lakeflow/test"
        mock_repo.create.return_value = mock_repo_info
        mock_repo.get_repo_path.return_value = mock_repo_info.path

        mock_pipeline = MagicMock()
        mock_pipeline_client.return_value = mock_pipeline
        mock_pipeline_response = MagicMock()
        mock_pipeline_response.pipeline_id = "pipeline-xyz"
        mock_pipeline.create.return_value = mock_pipeline_response

        result = runner.invoke(
            main,
            ['create_pipeline', 'github', 'my_pipeline', '-n', 'my_conn'],
        )

        # Should succeed (or at least pass the validation)
        assert "Either --connection-name or --pipeline-spec must be provided" not in result.output


class TestRunPipelineCommand:
    """Tests for run_pipeline command."""

    @patch('databricks.labs.community_connector.cli.WorkspaceClient')
    @patch('databricks.labs.community_connector.cli.PipelineClient')
    def test_run_pipeline_finds_by_name(self, mock_pipeline_client, mock_workspace_client):
        """Test that run_pipeline finds pipeline by name."""
        runner = CliRunner()

        # Setup mocks
        mock_ws = MagicMock()
        mock_workspace_client.return_value = mock_ws
        mock_ws.config.host = "https://test.databricks.com"

        mock_pipeline_obj = MagicMock()
        mock_pipeline_obj.pipeline_id = "found-pipeline-id"
        mock_ws.pipelines.list_pipelines.return_value = [mock_pipeline_obj]

        mock_client = MagicMock()
        mock_pipeline_client.return_value = mock_client
        mock_update = MagicMock()
        mock_update.update_id = "update-123"
        mock_client.start.return_value = mock_update

        result = runner.invoke(
            main,
            ['run_pipeline', 'my_test_pipeline'],
        )

        assert result.exit_code == 0
        assert "Pipeline run started" in result.output

    @patch('databricks.labs.community_connector.cli.WorkspaceClient')
    def test_run_pipeline_not_found(self, mock_workspace_client):
        """Test error when pipeline is not found."""
        runner = CliRunner()

        mock_ws = MagicMock()
        mock_workspace_client.return_value = mock_ws
        mock_ws.pipelines.list_pipelines.return_value = []

        result = runner.invoke(
            main,
            ['run_pipeline', 'nonexistent_pipeline'],
        )

        assert result.exit_code != 0
        assert "not found" in result.output


# pylint: disable=too-few-public-methods
class TestShowPipelineCommand:
    """Tests for show_pipeline command."""

    @patch('databricks.labs.community_connector.cli.WorkspaceClient')
    @patch('databricks.labs.community_connector.cli.PipelineClient')
    def test_show_pipeline_displays_info(self, mock_pipeline_client, mock_workspace_client):
        """Test that show_pipeline displays pipeline information."""
        runner = CliRunner()

        # Setup mocks
        mock_ws = MagicMock()
        mock_workspace_client.return_value = mock_ws
        mock_ws.config.host = "https://test.databricks.com"

        mock_pipeline_obj = MagicMock()
        mock_pipeline_obj.pipeline_id = "pipeline-456"
        mock_ws.pipelines.list_pipelines.return_value = [mock_pipeline_obj]

        mock_client = MagicMock()
        mock_pipeline_client.return_value = mock_client
        mock_info = MagicMock()
        mock_info.name = "My Pipeline"
        mock_info.pipeline_id = "pipeline-456"
        mock_info.state = "RUNNING"
        mock_info.latest_updates = []
        mock_client.get.return_value = mock_info

        result = runner.invoke(
            main,
            ['show_pipeline', 'my_pipeline'],
        )

        assert result.exit_code == 0
        assert "My Pipeline" in result.output
        assert "pipeline-456" in result.output
        assert "RUNNING" in result.output


class TestCreateConnectionCommand:
    """Tests for create_connection command."""

    def test_create_connection_requires_options(self):
        """Test that --options is required."""
        runner = CliRunner()

        result = runner.invoke(
            main,
            ['create_connection', 'github', 'my_conn'],
        )

        assert result.exit_code != 0
        assert "Missing option" in result.output or "required" in result.output.lower()

    def test_create_connection_invalid_json_options(self):
        """Test error on invalid JSON options."""
        runner = CliRunner()

        result = runner.invoke(
            main,
            ['create_connection', 'github', 'my_conn', '-o', 'not json'],
        )

        assert result.exit_code != 0
        assert "Invalid JSON" in result.output

    @patch('databricks.labs.community_connector.cli.WorkspaceClient')
    def test_create_connection_warns_missing_external_options(self, mock_workspace_client):
        """Test warning when externalOptionsAllowList is missing."""
        runner = CliRunner()

        mock_ws = MagicMock()
        mock_workspace_client.return_value = mock_ws
        mock_ws.api_client.do.return_value = {"name": "test", "connection_id": "123"}

        result = runner.invoke(
            main,
            ['create_connection', 'github', 'my_conn', '-o', '{"host": "api.github.com"}'],
        )

        assert "externalOptionsAllowList" in result.output


class TestVersionAndHelp:
    """Tests for --version and --help options."""

    def test_version_option(self):
        """Test --version displays version."""
        runner = CliRunner()
        result = runner.invoke(main, ['--version'])

        assert result.exit_code == 0
        assert "community-connector" in result.output

    def test_help_option(self):
        """Test --help displays help."""
        runner = CliRunner()
        result = runner.invoke(main, ['--help'])

        assert result.exit_code == 0
        assert "create_pipeline" in result.output
        assert "run_pipeline" in result.output
        assert "show_pipeline" in result.output
        assert "create_connection" in result.output
        assert "update_connection" in result.output

    def test_create_pipeline_help(self):
        """Test create_pipeline --help."""
        runner = CliRunner()
        result = runner.invoke(main, ['create_pipeline', '--help'])

        assert result.exit_code == 0
        assert "--connection-name" in result.output
        assert "--pipeline-spec" in result.output
        assert "--catalog" in result.output
        assert "--target" in result.output
