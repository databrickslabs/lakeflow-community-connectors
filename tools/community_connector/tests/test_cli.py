"""
Unit tests for the community connector CLI.

Tests CLI helper functions, argument validation, and command invocation
using Click's CliRunner and mocks for Databricks SDK.
"""
# pylint: disable=too-many-lines

import base64
import json
import os
import tempfile
from unittest.mock import MagicMock, patch, create_autospec

import click
import pytest
from click.testing import CliRunner

from databricks.sdk import WorkspaceClient

from databricks.labs.community_connector_cli.cli import (
    main,
    _parse_pipeline_spec,
    _load_ingest_template,
    _find_pipeline_by_name,
    _load_connector_spec,
    _validate_connection_options,
    _validate_connection_options_with_spec,
    _convert_github_url_to_raw,
    _get_default_repo_raw_url,
    _get_constant_external_options_allowlist,
    _merge_external_options_allowlist,
    _get_ingest_path_from_pipeline,
    _extract_source_name_from_ingest,
)
from databricks.labs.community_connector_cli.connector_spec import (
    ParsedConnectorSpec,
    AuthMethod,
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

        assert "from databricks.labs.community_connector.pipeline import ingest" in content
        assert "{SOURCE_NAME}" in content
        assert "{CONNECTION_NAME}" in content

    def test_load_base_template(self):
        """Test loading the base ingest template."""
        content = _load_ingest_template("ingest_template_base.py")

        assert "from databricks.labs.community_connector.pipeline import ingest" in content
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

        with patch("databricks.labs.community_connector_cli.cli.WorkspaceClient"):
            result = runner.invoke(
                main,
                ['create_pipeline', 'github', 'my_pipeline'],
            )

        assert result.exit_code != 0
        assert "Either --connection-name or --pipeline-spec must be provided" in result.output

    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    @patch("databricks.labs.community_connector_cli.cli.RepoClient")
    @patch("databricks.labs.community_connector_cli.cli.PipelineClient")
    @patch("databricks.labs.community_connector_cli.cli._create_workspace_file")
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

    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    @patch("databricks.labs.community_connector_cli.cli.PipelineClient")
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

    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
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

    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    @patch("databricks.labs.community_connector_cli.cli.PipelineClient")
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

    @patch("databricks.labs.community_connector_cli.cli._load_connector_spec")
    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    def test_create_connection_warns_missing_external_options_no_spec(
        self, mock_workspace_client, mock_load_spec
    ):
        """Test warning when externalOptionsAllowList is missing and spec is unavailable."""
        runner = CliRunner()

        # Simulate spec not found
        mock_load_spec.return_value = None

        mock_ws = MagicMock()
        mock_workspace_client.return_value = mock_ws
        mock_ws.api_client.do.return_value = {"name": "test", "connection_id": "123"}

        result = runner.invoke(
            main,
            ['create_connection', 'github', 'my_conn', '-o', '{"host": "api.github.com"}'],
        )

        assert "externalOptionsAllowList" in result.output

    @patch("databricks.labs.community_connector_cli.cli._load_connector_spec")
    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    def test_create_connection_validates_required_params(
        self, mock_workspace_client, mock_load_spec
    ):
        """Test error when required connection parameters are missing."""
        runner = CliRunner()

        # Simulate a spec with required params
        mock_load_spec.return_value = {
            "connection": {
                "parameters": [
                    {"name": "token", "type": "string", "required": True},
                    {"name": "base_url", "type": "string", "required": False},
                ]
            },
            "external_options_allowlist": "owner,repo",
        }

        result = runner.invoke(
            main,
            [
                "create_connection",
                "github",
                "my_conn",
                "-o",
                '{"base_url": "https://api.github.com"}',
            ],
        )

        assert result.exit_code != 0
        assert "Missing required connection parameters" in result.output
        assert "token" in result.output

    @patch("databricks.labs.community_connector_cli.cli._load_connector_spec")
    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    def test_create_connection_fails_unknown_params(self, mock_workspace_client, mock_load_spec):
        """Test error when unknown connection parameters are provided."""
        runner = CliRunner()

        # Simulate a spec with specific params
        mock_load_spec.return_value = {
            "connection": {
                "parameters": [
                    {"name": "token", "type": "string", "required": True},
                ]
            },
            "external_options_allowlist": "",
        }

        mock_ws = MagicMock()
        mock_workspace_client.return_value = mock_ws
        mock_ws.api_client.do.return_value = {"name": "test", "connection_id": "123"}

        result = runner.invoke(
            main,
            [
                "create_connection",
                "github",
                "my_conn",
                "-o",
                '{"token": "ghp_xxx", "unknown_param": "value"}',
            ],
        )

        # Should fail with error about unknown parameters
        assert result.exit_code != 0
        assert "Unknown connection parameters" in result.output
        assert "unknown_param" in result.output

    @patch("databricks.labs.community_connector_cli.cli._load_connector_spec")
    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    def test_create_connection_auto_adds_external_options_allowlist(
        self, mock_workspace_client, mock_load_spec
    ):
        """Test that externalOptionsAllowList is auto-added from spec merged with constant allowlist."""
        runner = CliRunner()

        mock_load_spec.return_value = {
            "connection": {
                "parameters": [
                    {"name": "token", "type": "string", "required": True},
                ]
            },
            "external_options_allowlist": "owner,repo,state",
        }

        mock_ws = MagicMock()
        mock_workspace_client.return_value = mock_ws
        mock_ws.api_client.do.return_value = {"name": "test", "connection_id": "123"}

        result = runner.invoke(
            main,
            ["create_connection", "github", "my_conn", "-o", '{"token": "ghp_xxx"}'],
        )

        assert result.exit_code == 0
        assert "Auto-added externalOptionsAllowList" in result.output

        # Verify the API was called with externalOptionsAllowList (merged: source + constant)
        call_args = mock_ws.api_client.do.call_args
        assert call_args is not None
        body = call_args.kwargs.get("body", call_args[1].get("body", {}))
        allowlist = body.get("options", {}).get("externalOptionsAllowList", "")
        # Should contain source options
        assert "owner" in allowlist
        assert "repo" in allowlist
        assert "state" in allowlist
        # Should contain constant options from config
        assert "tableName" in allowlist
        assert "tableNameList" in allowlist
        assert "tableConfigs" in allowlist
        assert "isDeleteFlow" in allowlist


class TestHelpOptions:
    """Tests for --help options."""

    def test_help_option(self):
        """Test --help displays help."""
        runner = CliRunner()
        result = runner.invoke(main, ['--help'])

        assert result.exit_code == 0
        assert "create_pipeline" in result.output
        assert "update_pipeline" in result.output
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


class TestValidateConnectionOptions:
    """Tests for _validate_connection_options function."""

    def test_validate_all_required_present(self):
        """Test validation passes when all required params are present."""
        options = {"token": "abc123", "api_key": "xyz"}
        required = {"token", "api_key"}
        optional = {"timeout"}

        errors = _validate_connection_options("test_source", options, required, optional)

        assert errors == []

    def test_validate_missing_required(self):
        """Test validation fails when required params are missing."""
        options = {"token": "abc123"}
        required = {"token", "api_key", "secret"}
        optional = set()

        errors = _validate_connection_options("test_source", options, required, optional)

        assert len(errors) == 1
        assert "Missing required connection parameters" in errors[0]
        assert "api_key" in errors[0]
        assert "secret" in errors[0]

    def test_validate_unknown_params_error(self):
        """Test that unknown params generate an error."""
        options = {"token": "abc123", "unknown_option": "value"}
        required = {"token"}
        optional = set()

        errors = _validate_connection_options("test_source", options, required, optional)

        assert len(errors) == 1
        assert "Unknown connection parameters" in errors[0]
        assert "unknown_option" in errors[0]

    def test_validate_always_allowed_params(self):
        """Test that sourceName and externalOptionsAllowList are always allowed."""
        options = {
            "token": "abc123",
            "sourceName": "github",
            "externalOptionsAllowList": "owner,repo",
        }
        required = {"token"}
        optional = set()

        errors = _validate_connection_options("test_source", options, required, optional)

        assert errors == []


class TestValidateConnectionOptionsWithSpec:
    """Tests for _validate_connection_options_with_spec function with auth_methods."""

    def test_validate_flat_params_all_required_present(self):
        """Test validation passes with flat parameters (Option A)."""
        spec = ParsedConnectorSpec(
            required_params={"token", "api_key"},
            optional_params={"timeout"},
        )
        options = {"token": "abc123", "api_key": "xyz"}

        errors = _validate_connection_options_with_spec("test_source", options, spec)

        assert errors == []

    def test_validate_flat_params_missing_required(self):
        """Test validation fails when required params are missing (Option A)."""
        spec = ParsedConnectorSpec(
            required_params={"token", "api_key"},
            optional_params=set(),
        )
        options = {"token": "abc123"}

        errors = _validate_connection_options_with_spec("test_source", options, spec)

        assert len(errors) == 1
        assert "Missing required connection parameters" in errors[0]
        assert "api_key" in errors[0]

    def test_validate_auth_methods_service_account_valid(self, capsys):
        """Test validation passes when all service_account params are provided."""
        spec = ParsedConnectorSpec(
            auth_methods=[
                AuthMethod(
                    name="service_account",
                    description="Use service account.",
                    required_params={"username", "secret"},
                    optional_params=set(),
                ),
                AuthMethod(
                    name="api_secret",
                    description="Use API secret.",
                    required_params={"api_secret"},
                    optional_params=set(),
                ),
            ],
            common_required_params=set(),
            common_optional_params={"region"},
        )
        options = {"username": "user", "secret": "pass", "region": "US"}

        errors = _validate_connection_options_with_spec("mixpanel", options, spec)

        assert errors == []
        captured = capsys.readouterr()
        assert "Detected auth method: service_account" in captured.out

    def test_validate_auth_methods_api_secret_valid(self, capsys):
        """Test validation passes when api_secret params are provided."""
        spec = ParsedConnectorSpec(
            auth_methods=[
                AuthMethod(
                    name="service_account",
                    description="Use service account.",
                    required_params={"username", "secret"},
                    optional_params=set(),
                ),
                AuthMethod(
                    name="api_secret",
                    description="Use API secret.",
                    required_params={"api_secret"},
                    optional_params=set(),
                ),
            ],
            common_required_params=set(),
            common_optional_params={"region"},
        )
        options = {"api_secret": "my_secret"}

        errors = _validate_connection_options_with_spec("mixpanel", options, spec)

        assert errors == []
        captured = capsys.readouterr()
        assert "Detected auth method: api_secret" in captured.out

    def test_validate_auth_methods_no_valid_method(self):
        """Test validation fails when no auth method is satisfied."""
        spec = ParsedConnectorSpec(
            auth_methods=[
                AuthMethod(
                    name="service_account",
                    description="Use service account.",
                    required_params={"username", "secret"},
                    optional_params=set(),
                ),
                AuthMethod(
                    name="api_secret",
                    description="Use API secret.",
                    required_params={"api_secret"},
                    optional_params=set(),
                ),
            ],
            common_required_params=set(),
            common_optional_params=set(),
        )
        options = {"username": "user"}  # Missing 'secret' for service_account

        errors = _validate_connection_options_with_spec("mixpanel", options, spec)

        assert len(errors) == 1
        assert "No valid authentication method detected" in errors[0]
        assert "service_account" in errors[0]
        assert "api_secret" in errors[0]

    def test_validate_auth_methods_missing_common_required(self):
        """Test validation fails when common required params are missing."""
        spec = ParsedConnectorSpec(
            auth_methods=[
                AuthMethod(
                    name="api_token",
                    description="Use API token.",
                    required_params={"email", "api_token"},
                    optional_params=set(),
                ),
            ],
            common_required_params={"subdomain"},
            common_optional_params=set(),
        )
        options = {"email": "user@example.com", "api_token": "token123"}  # Missing subdomain

        errors = _validate_connection_options_with_spec("zendesk", options, spec)

        assert len(errors) == 1
        assert "Missing required common parameters" in errors[0]
        assert "subdomain" in errors[0]

    def test_validate_auth_methods_unknown_params_error(self):
        """Test that unknown params generate an error."""
        spec = ParsedConnectorSpec(
            auth_methods=[
                AuthMethod(
                    name="api_secret",
                    description="Use API secret.",
                    required_params={"api_secret"},
                    optional_params=set(),
                ),
            ],
            common_required_params=set(),
            common_optional_params=set(),
        )
        options = {"api_secret": "secret", "unknown_param": "value"}

        errors = _validate_connection_options_with_spec("test_source", options, spec)

        assert len(errors) == 1
        assert "Unknown connection parameters" in errors[0]
        assert "unknown_param" in errors[0]


class TestLoadConnectorSpec:
    """Tests for _load_connector_spec function."""

    def test_load_spec_for_existing_source(self):
        """Test loading spec for a source that exists in the repo."""
        # This test will work if run from within the repo
        spec = _load_connector_spec("github")

        # May be None if not in repo and can't fetch from GitHub
        if spec is not None:
            assert "connection" in spec
            assert "external_options_allowlist" in spec

    def test_load_spec_for_nonexistent_source(self):
        """Test loading spec for a source that doesn't exist."""
        spec = _load_connector_spec("nonexistent_source_xyz123")

        # Should return None for non-existent source
        assert spec is None


class TestConvertGithubUrlToRaw:
    """Tests for _convert_github_url_to_raw function."""

    def test_convert_https_github_url(self):
        """Test converting a standard HTTPS GitHub URL."""
        url = "https://github.com/databrickslabs/lakeflow-community-connectors"
        result = _convert_github_url_to_raw(url)
        expected = (
            "https://raw.githubusercontent.com/databrickslabs/lakeflow-community-connectors/master"
        )
        assert result == expected

    def test_convert_https_github_url_with_git_suffix(self):
        """Test converting a GitHub URL with .git suffix."""
        url = "https://github.com/databrickslabs/lakeflow-community-connectors.git"
        result = _convert_github_url_to_raw(url)
        expected = (
            "https://raw.githubusercontent.com/databrickslabs/lakeflow-community-connectors/master"
        )
        assert result == expected

    def test_convert_https_github_url_with_trailing_slash(self):
        """Test converting a GitHub URL with trailing slash."""
        url = "https://github.com/databrickslabs/lakeflow-community-connectors/"
        result = _convert_github_url_to_raw(url)
        expected = (
            "https://raw.githubusercontent.com/databrickslabs/lakeflow-community-connectors/master"
        )
        assert result == expected

    def test_convert_with_custom_branch(self):
        """Test converting with a custom branch."""
        url = "https://github.com/myorg/myrepo"
        result = _convert_github_url_to_raw(url, branch="main")
        assert result == "https://raw.githubusercontent.com/myorg/myrepo/main"

    def test_already_raw_url_unchanged(self):
        """Test that already raw URLs are returned unchanged."""
        url = "https://raw.githubusercontent.com/myorg/myrepo/main"
        result = _convert_github_url_to_raw(url)
        assert result == url

    def test_http_github_url(self):
        """Test converting an HTTP GitHub URL."""
        url = "http://github.com/myorg/myrepo"
        result = _convert_github_url_to_raw(url)
        assert result == "https://raw.githubusercontent.com/myorg/myrepo/master"

    def test_git_ssh_url(self):
        """Test converting a git SSH URL."""
        url = "git@github.com:myorg/myrepo"
        result = _convert_github_url_to_raw(url)
        assert result == "https://raw.githubusercontent.com/myorg/myrepo/master"


class TestGetDefaultRepoRawUrl:
    """Tests for _get_default_repo_raw_url function."""

    def test_returns_raw_url(self):
        """Test that it returns a raw.githubusercontent.com URL."""
        url = _get_default_repo_raw_url()
        assert "raw.githubusercontent.com" in url
        assert "lakeflow-community-connectors" in url


class TestMergeExternalOptionsAllowlist:
    """Tests for _merge_external_options_allowlist function."""

    def test_merge_both_non_empty(self):
        """Test merging two non-empty allowlists."""
        source = "owner,repo,state"
        constant = "scd_type,primary_keys"
        result = _merge_external_options_allowlist(source, constant)
        # Result should be sorted and contain all items
        assert result == "owner,primary_keys,repo,scd_type,state"

    def test_merge_with_duplicates(self):
        """Test that duplicates are removed."""
        source = "owner,repo,scd_type"
        constant = "scd_type,primary_keys"
        result = _merge_external_options_allowlist(source, constant)
        # scd_type should only appear once
        assert result.count("scd_type") == 1
        assert "owner" in result
        assert "primary_keys" in result

    def test_merge_empty_source(self):
        """Test merging with empty source allowlist."""
        source = ""
        constant = "scd_type,primary_keys"
        result = _merge_external_options_allowlist(source, constant)
        assert result == "primary_keys,scd_type"

    def test_merge_empty_constant(self):
        """Test merging with empty constant allowlist."""
        source = "owner,repo"
        constant = ""
        result = _merge_external_options_allowlist(source, constant)
        assert result == "owner,repo"

    def test_merge_both_empty(self):
        """Test merging two empty allowlists."""
        source = ""
        constant = ""
        result = _merge_external_options_allowlist(source, constant)
        assert result == ""

    def test_merge_with_whitespace(self):
        """Test that whitespace is handled correctly."""
        source = "owner , repo , state"
        constant = " scd_type , primary_keys "
        result = _merge_external_options_allowlist(source, constant)
        assert "owner" in result
        assert "scd_type" in result
        # No extra whitespace in result
        assert "  " not in result


class TestGetConstantExternalOptionsAllowlist:
    """Tests for _get_constant_external_options_allowlist function."""

    def test_returns_string(self):
        """Test that it returns a string."""
        result = _get_constant_external_options_allowlist()
        assert isinstance(result, str)

    def test_contains_expected_options(self):
        """Test that it contains the expected constant options."""
        result = _get_constant_external_options_allowlist()
        # These are the options defined in default_config.yaml
        assert "tableName" in result
        assert "tableNameList" in result
        assert "tableConfigs" in result
        assert "isDeleteFlow" in result


class TestGetIngestPathFromPipeline:
    """Tests for _get_ingest_path_from_pipeline function."""

    def test_get_path_from_file_library(self):
        """Test extracting ingest.py path from file library."""
        mock_pipeline_info = MagicMock()
        mock_pipeline_info.spec.libraries = [MagicMock()]
        mock_pipeline_info.spec.libraries[0].file.path = "/Users/test/workspace/ingest.py"
        mock_pipeline_info.spec.libraries[0].notebook = None

        result = _get_ingest_path_from_pipeline(mock_pipeline_info)

        assert result == "/Users/test/workspace/ingest.py"

    def test_get_path_from_root_path_fallback(self):
        """Test falling back to root_path when no ingest.py in libraries."""
        mock_pipeline_info = MagicMock()
        mock_pipeline_info.spec.libraries = []
        mock_pipeline_info.spec.root_path = "/Users/test/workspace"

        result = _get_ingest_path_from_pipeline(mock_pipeline_info)

        assert result == "/Users/test/workspace/ingest.py"

    def test_get_path_no_spec(self):
        """Test returning None when pipeline has no spec."""
        mock_pipeline_info = MagicMock()
        mock_pipeline_info.spec = None

        result = _get_ingest_path_from_pipeline(mock_pipeline_info)

        assert result is None

    def test_get_path_empty_libraries_no_root_path(self):
        """Test returning None when no libraries and no root_path."""
        mock_pipeline_info = MagicMock()
        mock_pipeline_info.spec.libraries = []
        mock_pipeline_info.spec.root_path = None

        result = _get_ingest_path_from_pipeline(mock_pipeline_info)

        assert result is None

    def test_get_path_from_notebook_library(self):
        """Test extracting ingest path from notebook library."""
        mock_pipeline_info = MagicMock()
        mock_lib = MagicMock()
        mock_lib.file = None
        mock_lib.notebook.path = "/Users/test/workspace/ingest"
        mock_pipeline_info.spec.libraries = [mock_lib]
        mock_pipeline_info.spec.root_path = None

        result = _get_ingest_path_from_pipeline(mock_pipeline_info)

        assert result == "/Users/test/workspace/ingest.py"


class TestExtractSourceNameFromIngest:
    """Tests for _extract_source_name_from_ingest function."""

    def test_extract_double_quoted_source_name(self):
        """Test extracting source_name with double quotes."""
        content = """
from pipeline.ingestion_pipeline import ingest
source_name = "github"
"""
        result = _extract_source_name_from_ingest(content)

        assert result == "github"

    def test_extract_single_quoted_source_name(self):
        """Test extracting source_name with single quotes."""
        content = """
from pipeline.ingestion_pipeline import ingest
source_name = 'stripe'
"""
        result = _extract_source_name_from_ingest(content)

        assert result == "stripe"

    def test_extract_source_name_with_spaces(self):
        """Test extracting source_name with spaces around equals."""
        content = """
source_name   =   "hubspot"
"""
        result = _extract_source_name_from_ingest(content)

        assert result == "hubspot"

    def test_extract_source_name_not_found(self):
        """Test returning None when source_name is not found."""
        content = """
# Some other content
pipeline_spec = {}
"""
        result = _extract_source_name_from_ingest(content)

        assert result is None


class TestUpdatePipelineCommand:
    """Tests for update_pipeline command."""

    def test_update_pipeline_requires_pipeline_spec(self):
        """Test that --pipeline-spec is required."""
        runner = CliRunner()

        result = runner.invoke(
            main,
            ["update_pipeline", "my_pipeline"],
        )

        assert result.exit_code != 0
        assert "Missing option" in result.output or "required" in result.output.lower()

    def test_update_pipeline_help(self):
        """Test update_pipeline --help."""
        runner = CliRunner()
        result = runner.invoke(main, ["update_pipeline", "--help"])

        assert result.exit_code == 0
        assert "--pipeline-spec" in result.output
        assert "PIPELINE_NAME" in result.output

    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    def test_update_pipeline_not_found(self, mock_workspace_client):
        """Test error when pipeline is not found."""
        runner = CliRunner()

        mock_ws = MagicMock()
        mock_workspace_client.return_value = mock_ws
        mock_ws.pipelines.list_pipelines.return_value = []

        result = runner.invoke(
            main,
            [
                "update_pipeline",
                "nonexistent_pipeline",
                "-ps",
                '{"connection_name": "conn", "objects": []}',
            ],
        )

        assert result.exit_code != 0
        assert "not found" in result.output

    @patch("databricks.labs.community_connector_cli.cli._create_workspace_file")
    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    @patch("databricks.labs.community_connector_cli.cli.PipelineClient")
    def test_update_pipeline_success(
        self, mock_pipeline_client, mock_workspace_client, mock_create_file
    ):
        """Test successful pipeline update."""
        runner = CliRunner()

        # Setup workspace client mock
        mock_ws = MagicMock()
        mock_workspace_client.return_value = mock_ws
        mock_ws.config.host = "https://test.databricks.com"

        # Setup pipeline list mock
        mock_pipeline_obj = MagicMock()
        mock_pipeline_obj.pipeline_id = "pipeline-123"
        mock_ws.pipelines.list_pipelines.return_value = [mock_pipeline_obj]

        # Setup pipeline client mock
        mock_client = MagicMock()
        mock_pipeline_client.return_value = mock_client
        mock_pipeline_info = MagicMock()
        mock_pipeline_info.spec.root_path = "/Users/test/workspace"
        mock_pipeline_info.spec.libraries = []
        mock_client.get.return_value = mock_pipeline_info

        # Setup workspace export mock (for reading existing ingest.py)
        mock_export_response = MagicMock()
        existing_content = 'source_name = "github"\npipeline_spec = {}'
        mock_export_response.content = base64.b64encode(existing_content.encode()).decode()
        mock_ws.workspace.export.return_value = mock_export_response

        result = runner.invoke(
            main,
            [
                "update_pipeline",
                "my_pipeline",
                "-ps",
                '{"connection_name": "my_conn", "objects": [{"table": {"source_table": "users"}}]}',
            ],
        )

        assert result.exit_code == 0
        assert "Pipeline updated successfully" in result.output
        assert "pipeline-123" in result.output

        # Verify the workspace file was created
        mock_create_file.assert_called_once()
        call_args = mock_create_file.call_args
        assert "ingest.py" in call_args[0][1]  # path contains ingest.py

    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    @patch("databricks.labs.community_connector_cli.cli.PipelineClient")
    def test_update_pipeline_cannot_determine_ingest_path(
        self, mock_pipeline_client, mock_workspace_client
    ):
        """Test error when ingest.py path cannot be determined."""
        runner = CliRunner()

        mock_ws = MagicMock()
        mock_workspace_client.return_value = mock_ws

        mock_pipeline_obj = MagicMock()
        mock_pipeline_obj.pipeline_id = "pipeline-123"
        mock_ws.pipelines.list_pipelines.return_value = [mock_pipeline_obj]

        mock_client = MagicMock()
        mock_pipeline_client.return_value = mock_client
        mock_pipeline_info = MagicMock()
        mock_pipeline_info.spec = None  # No spec available
        mock_client.get.return_value = mock_pipeline_info

        result = runner.invoke(
            main,
            [
                "update_pipeline",
                "my_pipeline",
                "-ps",
                '{"connection_name": "conn", "objects": []}',
            ],
        )

        assert result.exit_code != 0
        assert "Could not determine ingest.py path" in result.output

    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    @patch("databricks.labs.community_connector_cli.cli.PipelineClient")
    def test_update_pipeline_cannot_extract_source_name(
        self, mock_pipeline_client, mock_workspace_client
    ):
        """Test error when source_name cannot be extracted from ingest.py."""
        runner = CliRunner()

        mock_ws = MagicMock()
        mock_workspace_client.return_value = mock_ws

        mock_pipeline_obj = MagicMock()
        mock_pipeline_obj.pipeline_id = "pipeline-123"
        mock_ws.pipelines.list_pipelines.return_value = [mock_pipeline_obj]

        mock_client = MagicMock()
        mock_pipeline_client.return_value = mock_client
        mock_pipeline_info = MagicMock()
        mock_pipeline_info.spec.root_path = "/Users/test/workspace"
        mock_pipeline_info.spec.libraries = []
        mock_client.get.return_value = mock_pipeline_info

        # Return content without source_name
        mock_export_response = MagicMock()
        mock_export_response.content = base64.b64encode(b"# no source name here").decode()
        mock_ws.workspace.export.return_value = mock_export_response

        result = runner.invoke(
            main,
            [
                "update_pipeline",
                "my_pipeline",
                "-ps",
                '{"connection_name": "conn", "objects": []}',
            ],
        )

        assert result.exit_code != 0
        assert "Could not extract source_name" in result.output

    def test_update_pipeline_with_yaml_file(self):
        """Test update_pipeline with a YAML spec file."""
        runner = CliRunner()

        yaml_content = """
connection_name: yaml_conn
objects:
  - table:
      source_table: products
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            temp_path = f.name

        try:
            with patch(
                "databricks.labs.community_connector_cli.cli.WorkspaceClient"
            ) as mock_ws_client:
                mock_ws = MagicMock()
                mock_ws_client.return_value = mock_ws
                mock_ws.pipelines.list_pipelines.return_value = []

                result = runner.invoke(
                    main,
                    ["update_pipeline", "my_pipeline", "-ps", temp_path],
                )

                # Will fail at "pipeline not found" but validates YAML parsing works
                assert "not found" in result.output
        finally:
            os.unlink(temp_path)
