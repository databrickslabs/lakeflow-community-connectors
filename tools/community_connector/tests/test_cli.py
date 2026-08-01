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
from pathlib import Path
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
    _find_local_source_path,
    _upload_source_files,
    _load_connector_spec,
    _validate_connection_options,
    _validate_connection_options_with_spec,
    _convert_github_url_to_raw,
    _get_default_repo_raw_url,
    _get_constant_external_options_allowlist,
    _merge_external_options_allowlist,
    _get_ingest_path_from_pipeline,
    _extract_source_name_from_ingest,
    _parse_volume_path,
    _ensure_volume_directory,
    _validate_wheel_layout,
    _validate_framework_wheel,
    _find_repo_root,
    _interpolate_oauth_placeholders,
    _resolve_display_name,
    _build_community_connector_manifest,
    _write_community_connector_manifest,
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
                ['create_pipeline', 'github', 'my_pipeline', '--use-workspace-pipeline'],
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
            ['create_pipeline', 'github', 'my_pipeline', '-n', 'my_conn',
             '--use-workspace-pipeline'],
        )

        # Should succeed (or at least pass the validation)
        assert "Either --connection-name or --pipeline-spec must be provided" not in result.output

    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    @patch("databricks.labs.community_connector_cli.cli.RepoClient")
    @patch("databricks.labs.community_connector_cli.cli.PipelineClient")
    @patch("databricks.labs.community_connector_cli.cli._create_workspace_file")
    def test_create_pipeline_with_package(
        self, mock_create_file, mock_pipeline_client, mock_repo_client, mock_workspace_client
    ):
        """Test create_pipeline with --package skips repo clone and creates directory."""
        runner = CliRunner()

        mock_ws = MagicMock()
        mock_workspace_client.return_value = mock_ws
        mock_ws.current_user.me.return_value.user_name = "test@example.com"
        mock_ws.config.host = "https://test.databricks.com"

        mock_pipeline = MagicMock()
        mock_pipeline_client.return_value = mock_pipeline
        mock_pipeline_response = MagicMock()
        mock_pipeline_response.pipeline_id = "pipeline-xyz"
        mock_pipeline.create.return_value = mock_pipeline_response

        mock_pipeline_info = MagicMock()
        mock_pipeline_info.spec.catalog = "main"
        mock_pipeline_info.spec.schema = "default"
        mock_pipeline_info.spec.environment = None
        mock_ws.pipelines.get.return_value = mock_pipeline_info

        with tempfile.NamedTemporaryFile(suffix=".whl") as temp_pkg:
            temp_pkg.write(b"dummy wheel content")
            temp_pkg.flush()

            result = runner.invoke(
                main,
                [
                    'create_pipeline', 'github', 'my_pipeline',
                    '-n', 'my_conn', '--package', temp_pkg.name,
                    '--use-workspace-pipeline',
                ],
            )

            assert result.exit_code == 0, f"Exit code: {result.exit_code}\nOutput: {result.output}"
            assert "Using local connector packages" in result.output
            assert "Package uploaded successfully" in result.output
            assert "Creating workspace directory" in result.output

            # Repo should NOT be cloned
            mock_repo_client.return_value.create.assert_not_called()

            # Workspace directory should be created via mkdirs
            mock_ws.workspace.mkdirs.assert_called()

            mock_ws.files.upload.assert_called_once()
            mock_ws.pipelines.update.assert_called_once()

    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    @patch("databricks.labs.community_connector_cli.cli.RepoClient")
    @patch("databricks.labs.community_connector_cli.cli.PipelineClient")
    @patch("databricks.labs.community_connector_cli.cli._create_workspace_file")
    def test_create_pipeline_with_multiple_packages(
        self, mock_create_file, mock_pipeline_client, mock_repo_client, mock_workspace_client
    ):
        """Test create_pipeline with multiple --package options skips repo clone."""
        runner = CliRunner()

        mock_ws = MagicMock()
        mock_workspace_client.return_value = mock_ws
        mock_ws.current_user.me.return_value.user_name = "test@example.com"
        mock_ws.config.host = "https://test.databricks.com"

        mock_pipeline = MagicMock()
        mock_pipeline_client.return_value = mock_pipeline
        mock_pipeline_response = MagicMock()
        mock_pipeline_response.pipeline_id = "pipeline-xyz"
        mock_pipeline.create.return_value = mock_pipeline_response

        mock_pipeline_info = MagicMock()
        mock_pipeline_info.spec.catalog = "main"
        mock_pipeline_info.spec.schema = "default"
        mock_pipeline_info.spec.environment = None
        mock_ws.pipelines.get.return_value = mock_pipeline_info

        with tempfile.NamedTemporaryFile(suffix=".whl") as pkg1, \
             tempfile.NamedTemporaryFile(suffix=".whl") as pkg2:
            pkg1.write(b"wheel1")
            pkg1.flush()
            pkg2.write(b"wheel2")
            pkg2.flush()

            result = runner.invoke(
                main,
                [
                    'create_pipeline', 'github', 'my_pipeline', '-n', 'my_conn',
                    '-p', pkg1.name, '-p', pkg2.name, '--use-workspace-pipeline',
                ],
            )

            assert result.exit_code == 0, f"Exit code: {result.exit_code}\nOutput: {result.output}"
            mock_repo_client.return_value.create.assert_not_called()
            assert mock_ws.files.upload.call_count == 2
            mock_ws.pipelines.update.assert_called_once()

    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    @patch("databricks.labs.community_connector_cli.cli.RepoClient")
    @patch("databricks.labs.community_connector_cli.cli.PipelineClient")
    @patch("databricks.labs.community_connector_cli.cli._create_workspace_file")
    def test_create_pipeline_prints_url_at_end(
        self, mock_create_file, mock_pipeline_client, mock_repo_client, mock_workspace_client
    ):
        """Test that pipeline URL/ID is printed after all steps including package upload."""
        runner = CliRunner()

        mock_ws = MagicMock()
        mock_workspace_client.return_value = mock_ws
        mock_ws.current_user.me.return_value.user_name = "test@example.com"
        mock_ws.config.host = "https://test.databricks.com"

        mock_pipeline = MagicMock()
        mock_pipeline_client.return_value = mock_pipeline
        mock_pipeline_response = MagicMock()
        mock_pipeline_response.pipeline_id = "pipeline-xyz"
        mock_pipeline.create.return_value = mock_pipeline_response

        mock_pipeline_info = MagicMock()
        mock_pipeline_info.spec.catalog = "main"
        mock_pipeline_info.spec.schema = "default"
        mock_pipeline_info.spec.environment = None
        mock_ws.pipelines.get.return_value = mock_pipeline_info

        with tempfile.NamedTemporaryFile(suffix=".whl") as temp_pkg:
            temp_pkg.write(b"dummy wheel content")
            temp_pkg.flush()

            result = runner.invoke(
                main,
                ['create_pipeline', 'github', 'my_pipeline', '-n', 'my_conn', '-p', temp_pkg.name,
                 '--use-workspace-pipeline'],
            )

            assert result.exit_code == 0, f"Exit code: {result.exit_code}\nOutput: {result.output}"

            output = result.output
            pkg_uploaded_pos = output.index("Package uploaded successfully")
            pipeline_url_pos = output.index("Pipeline URL:")
            assert pipeline_url_pos > pkg_uploaded_pos

class TestFindLocalSourcePath:
    """Tests for _find_local_source_path function."""

    def test_finds_source_from_cwd(self, tmp_path):
        """Test finding source directory when cwd is the repo root."""
        source_dir = (
            tmp_path / "src" / "databricks" / "labs"
            / "community_connector" / "sources" / "github"
        )
        source_dir.mkdir(parents=True)

        with patch.object(Path, "cwd", return_value=tmp_path):
            result = _find_local_source_path("github")

        assert result is not None
        assert result == source_dir.resolve()

    def test_returns_none_when_not_found(self, tmp_path):
        """Test returning None when source directory doesn't exist."""
        with patch.object(Path, "cwd", return_value=tmp_path):
            result = _find_local_source_path("nonexistent_source")

        assert result is None


class TestUploadSourceFiles:
    """Tests for _upload_source_files function."""

    def test_uploads_py_readme_and_spec(self, tmp_path):
        """Test that *.py, README.md, and connector_spec.yaml are uploaded."""
        source_dir = (
            tmp_path / "src" / "databricks" / "labs"
            / "community_connector" / "sources" / "mysource"
        )
        source_dir.mkdir(parents=True)
        (source_dir / "mysource.py").write_text("# connector code")
        (source_dir / "__init__.py").write_text("")
        (source_dir / "README.md").write_text("# My Source")
        (source_dir / "connector_spec.yaml").write_text("connection: {}")
        (source_dir / "icon.svg").write_text("<svg/>")  # should be skipped

        mock_ws = MagicMock()

        with patch(
            "databricks.labs.community_connector_cli.cli._find_local_source_path",
            return_value=source_dir,
        ):
            _upload_source_files(mock_ws, "mysource", "/workspace/path", debug=False)

        assert mock_ws.workspace.mkdirs.call_count == 1
        assert mock_ws.workspace.import_.call_count == 4  # 2 .py + README.md + connector_spec.yaml

    def test_raises_when_source_not_found(self):
        """Test that ClickException is raised when source directory is not found."""
        mock_ws = MagicMock()

        with patch(
            "databricks.labs.community_connector_cli.cli._find_local_source_path",
            return_value=None,
        ):
            with pytest.raises(click.ClickException, match="Could not find local source directory"):
                _upload_source_files(mock_ws, "nosource", "/workspace/path", debug=False)

    def test_uploads_only_py_when_no_readme_or_spec(self, tmp_path):
        """Test upload when only .py files exist (no README.md or connector_spec.yaml)."""
        source_dir = (
            tmp_path / "src" / "databricks" / "labs"
            / "community_connector" / "sources" / "mysource"
        )
        source_dir.mkdir(parents=True)
        (source_dir / "mysource.py").write_text("# code")

        mock_ws = MagicMock()

        with patch(
            "databricks.labs.community_connector_cli.cli._find_local_source_path",
            return_value=source_dir,
        ):
            _upload_source_files(mock_ws, "mysource", "/workspace/path", debug=False)

        assert mock_ws.workspace.import_.call_count == 1


# pylint: disable=too-many-arguments,too-many-positional-arguments
class TestCreatePipelineUseLocalSource:
    """Tests for create_pipeline command with --use-local-source flag."""

    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    @patch("databricks.labs.community_connector_cli.cli.RepoClient")
    @patch("databricks.labs.community_connector_cli.cli.PipelineClient")
    @patch("databricks.labs.community_connector_cli.cli._create_workspace_file")
    @patch("databricks.labs.community_connector_cli.cli._upload_source_files")
    def test_create_pipeline_with_use_local_source(
        self, mock_upload_src, mock_create_file, mock_pipeline_client,
        mock_repo_client, mock_workspace_client
    ):
        """Test create_pipeline with --use-local-source uploads source files."""
        runner = CliRunner()

        mock_ws = MagicMock()
        mock_workspace_client.return_value = mock_ws
        mock_ws.current_user.me.return_value.user_name = "test@example.com"
        mock_ws.config.host = "https://test.databricks.com"

        mock_repo = MagicMock()
        mock_repo_client.return_value = mock_repo
        mock_repo_info = MagicMock()
        mock_repo_info.path = "/Repos/test@example.com/repo"
        mock_repo.create.return_value = mock_repo_info
        mock_repo.get_repo_path.return_value = mock_repo_info.path

        mock_pipeline = MagicMock()
        mock_pipeline_client.return_value = mock_pipeline
        mock_pipeline_response = MagicMock()
        mock_pipeline_response.pipeline_id = "pipeline-xyz"
        mock_pipeline.create.return_value = mock_pipeline_response

        result = runner.invoke(
            main,
            ['create_pipeline', 'github', 'my_pipeline', '-n', 'my_conn', '--use-local-source',
             '--use-workspace-pipeline'],
        )

        assert result.exit_code == 0, f"Exit code: {result.exit_code}\nOutput: {result.output}"
        mock_upload_src.assert_called_once()
        call_args = mock_upload_src.call_args
        assert call_args[0][1] == "github"  # source_name

    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    @patch("databricks.labs.community_connector_cli.cli.RepoClient")
    @patch("databricks.labs.community_connector_cli.cli.PipelineClient")
    @patch("databricks.labs.community_connector_cli.cli._create_workspace_file")
    @patch("databricks.labs.community_connector_cli.cli._upload_source_files")
    def test_create_pipeline_without_use_local_source(
        self, mock_upload_src, mock_create_file, mock_pipeline_client,
        mock_repo_client, mock_workspace_client
    ):
        """Test create_pipeline without --use-local-source does NOT upload source files."""
        runner = CliRunner()

        mock_ws = MagicMock()
        mock_workspace_client.return_value = mock_ws
        mock_ws.current_user.me.return_value.user_name = "test@example.com"
        mock_ws.config.host = "https://test.databricks.com"

        mock_repo = MagicMock()
        mock_repo_client.return_value = mock_repo
        mock_repo_info = MagicMock()
        mock_repo_info.path = "/Repos/test@example.com/repo"
        mock_repo.create.return_value = mock_repo_info
        mock_repo.get_repo_path.return_value = mock_repo_info.path

        mock_pipeline = MagicMock()
        mock_pipeline_client.return_value = mock_pipeline
        mock_pipeline_response = MagicMock()
        mock_pipeline_response.pipeline_id = "pipeline-xyz"
        mock_pipeline.create.return_value = mock_pipeline_response

        result = runner.invoke(
            main,
            ['create_pipeline', 'github', 'my_pipeline', '-n', 'my_conn',
             '--use-workspace-pipeline'],
        )

        assert result.exit_code == 0, f"Exit code: {result.exit_code}\nOutput: {result.output}"
        mock_upload_src.assert_not_called()

    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    @patch("databricks.labs.community_connector_cli.cli.RepoClient")
    @patch("databricks.labs.community_connector_cli.cli.PipelineClient")
    @patch("databricks.labs.community_connector_cli.cli._create_workspace_file")
    @patch("databricks.labs.community_connector_cli.cli._upload_source_files")
    def test_create_pipeline_use_local_source_with_package(
        self, mock_upload_src, mock_create_file, mock_pipeline_client,
        mock_repo_client, mock_workspace_client
    ):
        """Test create_pipeline with both --use-local-source and --package."""
        runner = CliRunner()

        mock_ws = MagicMock()
        mock_workspace_client.return_value = mock_ws
        mock_ws.current_user.me.return_value.user_name = "test@example.com"
        mock_ws.config.host = "https://test.databricks.com"

        mock_pipeline = MagicMock()
        mock_pipeline_client.return_value = mock_pipeline
        mock_pipeline_response = MagicMock()
        mock_pipeline_response.pipeline_id = "pipeline-xyz"
        mock_pipeline.create.return_value = mock_pipeline_response

        mock_pipeline_info = MagicMock()
        mock_pipeline_info.spec.catalog = "main"
        mock_pipeline_info.spec.schema = "default"
        mock_pipeline_info.spec.environment = None
        mock_ws.pipelines.get.return_value = mock_pipeline_info

        with tempfile.NamedTemporaryFile(suffix=".whl") as temp_pkg:
            temp_pkg.write(b"dummy wheel content")
            temp_pkg.flush()

            result = runner.invoke(
                main,
                [
                    'create_pipeline', 'github', 'my_pipeline', '-n', 'my_conn',
                    '-p', temp_pkg.name, '--use-local-source', '--use-workspace-pipeline',
                ],
            )

        assert result.exit_code == 0, f"Exit code: {result.exit_code}\nOutput: {result.output}"
        mock_upload_src.assert_called_once()
        mock_ws.files.upload.assert_called_once()


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
        """Test externalOptionsAllowList is auto-added from merged spec."""
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


class TestCreateConnectionConnectionType:
    """Tests for the COMMUNITY connection type and auth_type plumbing."""

    @patch("databricks.labs.community_connector_cli.cli._load_connector_spec")
    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    def test_create_connection_uses_community_type(self, mock_workspace_client, mock_load_spec):
        """create_connection POSTs connection_type=COMMUNITY."""
        runner = CliRunner()

        mock_load_spec.return_value = {
            "connection": {
                "parameters": [{"name": "token", "type": "string", "required": True}],
            },
            "external_options_allowlist": "",
        }
        mock_ws = MagicMock()
        mock_workspace_client.return_value = mock_ws
        mock_ws.api_client.do.return_value = {"name": "test", "connection_id": "123"}

        result = runner.invoke(
            main,
            ["create_connection", "github", "my_conn", "-o", '{"token": "ghp_xxx"}'],
        )

        assert result.exit_code == 0, result.output
        body = mock_ws.api_client.do.call_args.kwargs["body"]
        assert body["connection_type"] == "COMMUNITY"
        # Static mode must NOT stamp community_oauth_flow on the options.
        assert "community_oauth_flow" not in body["options"]

    def test_create_connection_static_rejects_oauth_flow_in_options(self):
        """Static mode rejects sneaking community_oauth_flow in via --options."""
        runner = CliRunner()

        result = runner.invoke(
            main,
            [
                "create_connection",
                "github",
                "my_conn",
                "-o",
                '{"token": "ghp_xxx", "community_oauth_flow": "m2m"}',
            ],
        )

        assert result.exit_code != 0
        assert "community_oauth_flow" in result.output

    @patch("databricks.labs.community_connector_cli.cli._load_connector_spec")
    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    def test_create_connection_m2m_sets_oauth_flow(
        self, mock_workspace_client, mock_load_spec
    ):
        """--auth-type=m2m stamps community_oauth_flow=m2m and exempts OAuth keys from spec."""
        runner = CliRunner()

        # Spec lists only connector-runtime params; OAuth keys must not trigger
        # 'unknown parameter' errors.
        mock_load_spec.return_value = {
            "connection": {"parameters": []},
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
                "--auth-type",
                "m2m",
                "-o",
                json.dumps(
                    {
                        "client_id": "cid",
                        "client_secret": "csecret",
                        "token_endpoint": "https://example.com/token",
                        "oauth_scope": "repo",
                    }
                ),
            ],
        )

        assert result.exit_code == 0, result.output
        body = mock_ws.api_client.do.call_args.kwargs["body"]
        assert body["connection_type"] == "COMMUNITY"
        opts = body["options"]
        assert opts["community_oauth_flow"] == "m2m"
        assert opts["client_id"] == "cid"
        assert opts["token_endpoint"] == "https://example.com/token"

    @patch("databricks.labs.community_connector_cli.cli._load_connector_spec")
    def test_create_connection_m2m_requires_oauth_fields(self, mock_load_spec):
        """--auth-type=m2m errors out when required OAuth options are missing."""
        runner = CliRunner()
        mock_load_spec.return_value = None

        result = runner.invoke(
            main,
            [
                "create_connection",
                "github",
                "my_conn",
                "--auth-type",
                "m2m",
                "-o",
                '{"client_id": "cid"}',
            ],
        )

        assert result.exit_code != 0
        assert "--auth-type=m2m" in result.output
        assert "client_secret" in result.output
        assert "token_endpoint" in result.output

    @patch("databricks.labs.community_connector_cli.cli._load_connector_spec")
    def test_create_connection_u2m_requires_oauth_fields(self, mock_load_spec):
        """--auth-type=u2m must error on missing required OAuth options before
        kicking off the loopback flow."""
        runner = CliRunner()
        mock_load_spec.return_value = None

        result = runner.invoke(
            main,
            [
                "create_connection",
                "github",
                "my_conn",
                "--auth-type",
                "u2m",
                "-o",
                '{"client_id": "cid"}',
            ],
        )

        assert result.exit_code != 0
        assert "--auth-type=u2m" in result.output
        assert "client_secret" in result.output
        assert "authorization_endpoint" in result.output
        assert "token_endpoint" in result.output

    @patch(
        "databricks.labs.community_connector_cli.cli.run_u2m_authorization_code_flow"
    )
    @patch("databricks.labs.community_connector_cli.cli._load_connector_spec")
    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    def test_create_connection_u2m_runs_oauth_flow(
        self, mock_workspace_client, mock_load_spec, mock_oauth
    ):
        """--auth-type=u2m runs the loopback OAuth flow and injects code/verifier/redirect."""
        runner = CliRunner()

        mock_load_spec.return_value = None
        mock_ws = MagicMock()
        mock_workspace_client.return_value = mock_ws
        mock_ws.api_client.do.return_value = {"name": "test", "connection_id": "123"}
        mock_oauth.return_value = ("AUTHCODE", "VERIFIER", "http://localhost:54321/callback")

        result = runner.invoke(
            main,
            [
                "create_connection",
                "github",
                "my_conn",
                "--auth-type",
                "u2m",
                "-o",
                json.dumps(
                    {
                        "client_id": "cid",
                        "client_secret": "csecret",
                        "authorization_endpoint": "https://example.com/authorize",
                        "token_endpoint": "https://example.com/token",
                        "oauth_scope": "repo",
                    }
                ),
            ],
        )

        assert result.exit_code == 0, result.output
        mock_oauth.assert_called_once()
        kwargs = mock_oauth.call_args.kwargs
        assert kwargs["client_id"] == "cid"
        assert kwargs["authorization_endpoint"] == "https://example.com/authorize"
        assert kwargs["scope"] == "repo"

        body = mock_ws.api_client.do.call_args.kwargs["body"]
        opts = body["options"]
        assert opts["community_oauth_flow"] == "u2m"
        assert opts["authorization_code"] == "AUTHCODE"
        assert opts["pkce_verifier"] == "VERIFIER"
        assert opts["oauth_redirect_uri"] == "http://localhost:54321/callback"


class TestUpdateConnectionCommand:
    """update_connection must support the same auth modes as create_connection
    so a U2M OAuth grant can be refreshed without recreating the connection."""

    @patch(
        "databricks.labs.community_connector_cli.cli.run_u2m_authorization_code_flow"
    )
    @patch("databricks.labs.community_connector_cli.cli._load_connector_spec")
    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    def test_update_connection_u2m_refreshes_oauth_grant(
        self, mock_workspace_client, mock_load_spec, mock_oauth
    ):
        """--auth-type=u2m re-runs the loopback flow and PATCHes a fresh
        authorization code / verifier / redirect into the connection."""
        runner = CliRunner()

        mock_load_spec.return_value = None
        mock_ws = MagicMock()
        mock_workspace_client.return_value = mock_ws
        mock_ws.api_client.do.return_value = {"name": "my_conn", "connection_id": "123"}
        mock_oauth.return_value = (
            "NEWCODE",
            "NEWVERIFIER",
            "http://localhost:54321/callback",
        )

        result = runner.invoke(
            main,
            [
                "update_connection",
                "github",
                "my_conn",
                "--auth-type",
                "u2m",
                "-o",
                json.dumps(
                    {
                        "client_id": "cid",
                        "client_secret": "csecret",
                        "authorization_endpoint": "https://example.com/authorize",
                        "token_endpoint": "https://example.com/token",
                        "oauth_scope": "repo",
                    }
                ),
            ],
        )

        assert result.exit_code == 0, result.output
        mock_oauth.assert_called_once()

        method, path = mock_ws.api_client.do.call_args.args[:2]
        assert method == "PATCH"
        assert path.endswith("/connections/my_conn")

        opts = mock_ws.api_client.do.call_args.kwargs["body"]["options"]
        assert opts["community_oauth_flow"] == "u2m"
        assert opts["authorization_code"] == "NEWCODE"
        assert opts["pkce_verifier"] == "NEWVERIFIER"
        assert opts["oauth_redirect_uri"] == "http://localhost:54321/callback"

    @patch("databricks.labs.community_connector_cli.cli._load_connector_spec")
    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    def test_update_connection_defaults_to_static_no_oauth_flow(
        self, mock_workspace_client, mock_load_spec
    ):
        """Without --auth-type the update stays static and runs no OAuth flow."""
        runner = CliRunner()

        mock_load_spec.return_value = None
        mock_ws = MagicMock()
        mock_workspace_client.return_value = mock_ws
        mock_ws.api_client.do.return_value = {"name": "my_conn", "connection_id": "123"}

        result = runner.invoke(
            main,
            [
                "update_connection",
                "github",
                "my_conn",
                "-o",
                '{"token": "ghp_xxxx"}',
            ],
        )

        assert result.exit_code == 0, result.output
        opts = mock_ws.api_client.do.call_args.kwargs["body"]["options"]
        assert "community_oauth_flow" not in opts


class TestOAuthDefaultsAutoFill:
    """Tests for auto-populating OAuth options from connector_spec.yaml."""

    @patch("databricks.labs.community_connector_cli.cli._load_connector_spec")
    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    def test_m2m_fills_endpoints_and_scope_from_spec(
        self, mock_workspace_client, mock_load_spec
    ):
        """User only supplies client_id + client_secret; spec fills the rest."""
        runner = CliRunner()
        mock_load_spec.return_value = {
            "connection": {
                "parameters": [],
                "oauth": {
                    "authorization_endpoint": "https://idp/authorize",
                    "token_endpoint": "https://idp/token",
                    "oauth_scope": "read",
                },
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
                "demo",
                "my_conn",
                "--auth-type",
                "m2m",
                "-o",
                '{"client_id":"cid","client_secret":"csecret"}',
            ],
        )

        assert result.exit_code == 0, result.output
        body = mock_ws.api_client.do.call_args.kwargs["body"]
        opts = body["options"]
        assert opts["community_oauth_flow"] == "m2m"
        assert opts["token_endpoint"] == "https://idp/token"
        assert opts["authorization_endpoint"] == "https://idp/authorize"
        assert opts["oauth_scope"] == "read"

    @patch("databricks.labs.community_connector_cli.cli._load_connector_spec")
    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    def test_user_options_override_spec_defaults(
        self, mock_workspace_client, mock_load_spec
    ):
        """A value passed via --options overrides the spec default for that key."""
        runner = CliRunner()
        mock_load_spec.return_value = {
            "connection": {
                "parameters": [],
                "oauth": {
                    "authorization_endpoint": "https://idp/authorize",
                    "token_endpoint": "https://idp/token",
                    "oauth_scope": "read",
                },
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
                "demo",
                "my_conn",
                "--auth-type",
                "m2m",
                "-o",
                json.dumps(
                    {
                        "client_id": "cid",
                        "client_secret": "csecret",
                        "oauth_scope": "custom",
                    }
                ),
            ],
        )

        assert result.exit_code == 0, result.output
        opts = mock_ws.api_client.do.call_args.kwargs["body"]["options"]
        assert opts["oauth_scope"] == "custom"

    @patch("databricks.labs.community_connector_cli.cli._load_connector_spec")
    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    def test_static_mode_ignores_oauth_defaults(
        self, mock_workspace_client, mock_load_spec
    ):
        """OAuth defaults must not leak into static-credential connections."""
        runner = CliRunner()
        mock_load_spec.return_value = {
            "connection": {
                "parameters": [{"name": "token", "type": "string", "required": True}],
                "oauth": {"token_endpoint": "https://idp/token"},
            },
            "external_options_allowlist": "",
        }
        mock_ws = MagicMock()
        mock_workspace_client.return_value = mock_ws
        mock_ws.api_client.do.return_value = {"name": "test", "connection_id": "123"}

        result = runner.invoke(
            main,
            ["create_connection", "demo", "my_conn", "-o", '{"token":"abc"}'],
        )

        assert result.exit_code == 0, result.output
        opts = mock_ws.api_client.do.call_args.kwargs["body"]["options"]
        assert "token_endpoint" not in opts
        assert "community_oauth_flow" not in opts


class TestOAuthSpecBlockShape:
    """With the connector spec declaring its OAuth flow (PR #218 shape), the
    user no longer passes --auth-type: the CLI takes the auth type from
    oauth.flow and resolves the spec's human-friendly oauth keys
    (authorization_url / token_url / scopes / extra_auth_params) into the
    option names the existing flow uses."""

    _SPEC = {
        "connection": {
            "parameters": [
                {"name": "client_id", "type": "string", "required": True},
                {"name": "client_secret", "type": "string", "required": True},
                {"name": "user_id", "type": "string", "required": False},
            ],
            "oauth": {
                "flow": "u2m",
                "pkce": False,
                "scopes": "https://www.googleapis.com/auth/gmail.readonly",
                "authorization_url": "https://accounts.google.com/o/oauth2/v2/auth",
                "token_url": "https://oauth2.googleapis.com/token",
                "extra_auth_params": {"access_type": "offline", "prompt": "consent"},
            },
        },
        "external_options_allowlist": "",
    }

    @patch(
        "databricks.labs.community_connector_cli.cli.run_u2m_authorization_code_flow"
    )
    @patch("databricks.labs.community_connector_cli.cli._load_connector_spec")
    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    def test_auth_type_derived_from_spec_flow_no_flag_needed(
        self, mock_workspace_client, mock_load_spec, mock_oauth
    ):
        """No --auth-type: the u2m flow runs (from oauth.flow), spec keys map to
        the RFC names, extra_auth_params reach the flow, and flow-control keys
        are not stored on the connection."""
        runner = CliRunner()
        mock_load_spec.return_value = self._SPEC
        mock_ws = MagicMock()
        mock_workspace_client.return_value = mock_ws
        mock_ws.api_client.do.return_value = {"name": "test", "connection_id": "123"}
        mock_oauth.return_value = ("CODE", "VERIFIER", "http://127.0.0.1:5/callback")

        result = runner.invoke(
            main,
            [
                "create_connection",
                "gmail",
                "my_conn",
                "-o",
                '{"client_id":"cid","client_secret":"csecret"}',
            ],
        )

        assert result.exit_code == 0, result.output

        # The u2m flow ran even though --auth-type was not passed.
        mock_oauth.assert_called_once()
        kwargs = mock_oauth.call_args.kwargs
        assert kwargs["authorization_endpoint"] == (
            "https://accounts.google.com/o/oauth2/v2/auth"
        )
        assert kwargs["scope"] == "https://www.googleapis.com/auth/gmail.readonly"
        assert kwargs["extra_auth_params"] == {
            "access_type": "offline",
            "prompt": "consent",
        }

        opts = mock_ws.api_client.do.call_args.kwargs["body"]["options"]
        assert opts["authorization_endpoint"] == (
            "https://accounts.google.com/o/oauth2/v2/auth"
        )
        assert opts["token_endpoint"] == "https://oauth2.googleapis.com/token"
        assert opts["oauth_scope"] == "https://www.googleapis.com/auth/gmail.readonly"
        assert opts["community_oauth_flow"] == "u2m"
        # Flow-control keys must never be stored on the connection.
        for leaked in ("flow", "pkce", "extra_auth_params", "scopes",
                       "authorization_url", "token_url"):
            assert leaked not in opts

    @patch(
        "databricks.labs.community_connector_cli.cli.run_u2m_authorization_code_flow"
    )
    @patch("databricks.labs.community_connector_cli.cli._load_connector_spec")
    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    def test_oauth_mode_enforces_connector_required_params(
        self, mock_workspace_client, mock_load_spec, mock_oauth
    ):
        """A connector-specific required param missing errors out (like static
        mode) before the browser flow runs — the OAuth fields are exempt, but
        connector params are not."""
        runner = CliRunner()
        spec = {
            "connection": {
                "parameters": [
                    {"name": "client_id", "type": "string", "required": True},
                    {"name": "client_secret", "type": "string", "required": True},
                    # A connector-specific required option, not an OAuth field.
                    {"name": "region", "type": "string", "required": True},
                ],
                "oauth": {
                    "flow": "u2m",
                    "scopes": "read",
                    "authorization_url": "https://idp/authorize",
                    "token_url": "https://idp/token",
                },
            },
            "external_options_allowlist": "",
        }
        mock_load_spec.return_value = spec
        mock_ws = MagicMock()
        mock_workspace_client.return_value = mock_ws

        result = runner.invoke(
            main,
            [
                "create_connection",
                "demo",
                "my_conn",
                "-o",
                '{"client_id":"cid","client_secret":"csecret"}',
            ],
        )

        assert result.exit_code != 0
        assert "region" in result.output
        # The browser flow must not run when required options are missing.
        mock_oauth.assert_not_called()
        mock_ws.api_client.do.assert_not_called()

    @patch("databricks.labs.community_connector_cli.cli._load_connector_spec")
    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    def test_explicit_auth_type_overrides_spec_flow(
        self, mock_workspace_client, mock_load_spec
    ):
        """An explicit --auth-type wins over the spec's oauth.flow."""
        runner = CliRunner()
        mock_load_spec.return_value = self._SPEC
        mock_ws = MagicMock()
        mock_workspace_client.return_value = mock_ws
        mock_ws.api_client.do.return_value = {"name": "test", "connection_id": "123"}

        result = runner.invoke(
            main,
            [
                "create_connection",
                "gmail",
                "my_conn",
                "--auth-type",
                "static",
                "-o",
                '{"client_id":"cid","client_secret":"csecret"}',
            ],
        )

        assert result.exit_code == 0, result.output
        opts = mock_ws.api_client.do.call_args.kwargs["body"]["options"]
        # static wins: no OAuth flow stamped, oauth defaults not applied.
        assert "community_oauth_flow" not in opts
        assert "authorization_endpoint" not in opts

    @patch(
        "databricks.labs.community_connector_cli.cli.run_u2m_authorization_code_flow"
    )
    @patch("databricks.labs.community_connector_cli.cli._load_connector_spec")
    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    def test_oauth_mode_still_rejects_truly_unknown_key(
        self, mock_workspace_client, mock_load_spec, mock_oauth
    ):
        """OAuth keys are exempt, but a genuinely unknown key is still rejected
        (the exemption must not become a blanket pass-through)."""
        runner = CliRunner()
        mock_load_spec.return_value = self._SPEC
        mock_ws = MagicMock()
        mock_workspace_client.return_value = mock_ws

        result = runner.invoke(
            main,
            [
                "create_connection",
                "gmail",
                "my_conn",
                "-o",
                '{"client_id":"cid","client_secret":"csecret","bogus_param":"x"}',
            ],
        )

        assert result.exit_code != 0
        assert "bogus_param" in result.output
        mock_oauth.assert_not_called()

    @pytest.mark.parametrize("flow", ["m2m", "u2m_per_user"])
    @patch(
        "databricks.labs.community_connector_cli.cli.run_u2m_authorization_code_flow"
    )
    @patch("databricks.labs.community_connector_cli.cli._load_connector_spec")
    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    def test_non_u2m_flows_derived_from_spec_no_browser(
        self, mock_workspace_client, mock_load_spec, mock_oauth, flow
    ):
        """m2m / u2m_per_user are derived from oauth.flow and stamp
        community_oauth_flow without opening a browser."""
        runner = CliRunner()
        mock_load_spec.return_value = {
            "connection": {
                "parameters": [
                    {"name": "client_id", "type": "string", "required": True},
                    {"name": "client_secret", "type": "string", "required": True},
                ],
                "oauth": {
                    "flow": flow,
                    "scopes": "read",
                    "authorization_url": "https://idp/authorize",
                    "token_url": "https://idp/token",
                },
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
                "demo",
                "my_conn",
                "-o",
                '{"client_id":"cid","client_secret":"csecret"}',
            ],
        )

        assert result.exit_code == 0, result.output
        opts = mock_ws.api_client.do.call_args.kwargs["body"]["options"]
        assert opts["community_oauth_flow"] == flow
        mock_oauth.assert_not_called()  # only u2m opens the browser

    @patch("databricks.labs.community_connector_cli.cli._load_connector_spec")
    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    def test_invalid_spec_flow_errors(self, mock_workspace_client, mock_load_spec):
        """An unrecognized oauth.flow value is rejected with a clear error."""
        runner = CliRunner()
        mock_load_spec.return_value = {
            "connection": {"parameters": [], "oauth": {"flow": "bogus"}},
            "external_options_allowlist": "",
        }
        mock_ws = MagicMock()
        mock_workspace_client.return_value = mock_ws

        result = runner.invoke(
            main, ["create_connection", "demo", "my_conn", "-o", "{}"]
        )

        assert result.exit_code != 0
        assert "Unknown auth type" in result.output
        assert "bogus" in result.output

    @patch(
        "databricks.labs.community_connector_cli.cli.run_u2m_authorization_code_flow"
    )
    @patch("databricks.labs.community_connector_cli.cli._load_connector_spec")
    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    def test_pkce_false_skips_pkce(
        self, mock_workspace_client, mock_load_spec, mock_oauth
    ):
        """oauth.pkce: false runs the flow with use_pkce=False and stores no
        pkce_verifier."""
        runner = CliRunner()
        mock_load_spec.return_value = self._SPEC  # has pkce: False
        mock_ws = MagicMock()
        mock_workspace_client.return_value = mock_ws
        mock_ws.api_client.do.return_value = {"name": "test", "connection_id": "123"}
        # No-PKCE flow returns an empty verifier.
        mock_oauth.return_value = ("CODE", "", "http://127.0.0.1:5/callback")

        result = runner.invoke(
            main,
            [
                "create_connection",
                "gmail",
                "my_conn",
                "-o",
                '{"client_id":"cid","client_secret":"csecret"}',
            ],
        )

        assert result.exit_code == 0, result.output
        assert mock_oauth.call_args.kwargs["use_pkce"] is False
        opts = mock_ws.api_client.do.call_args.kwargs["body"]["options"]
        assert "pkce_verifier" not in opts

    @patch(
        "databricks.labs.community_connector_cli.cli.run_u2m_authorization_code_flow"
    )
    @patch("databricks.labs.community_connector_cli.cli._load_connector_spec")
    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    def test_pkce_defaults_on_when_absent(
        self, mock_workspace_client, mock_load_spec, mock_oauth
    ):
        """No pkce key in the spec -> PKCE stays on (the secure default)."""
        runner = CliRunner()
        mock_load_spec.return_value = {
            "connection": {
                "parameters": [
                    {"name": "client_id", "type": "string", "required": True},
                    {"name": "client_secret", "type": "string", "required": True},
                ],
                "oauth": {
                    "flow": "u2m",
                    "scopes": "read",
                    "authorization_url": "https://idp/authorize",
                    "token_url": "https://idp/token",
                },
            },
            "external_options_allowlist": "",
        }
        mock_ws = MagicMock()
        mock_workspace_client.return_value = mock_ws
        mock_ws.api_client.do.return_value = {"name": "test", "connection_id": "123"}
        mock_oauth.return_value = ("CODE", "VER", "http://127.0.0.1:5/callback")

        result = runner.invoke(
            main,
            [
                "create_connection",
                "demo",
                "my_conn",
                "-o",
                '{"client_id":"cid","client_secret":"csecret"}',
            ],
        )

        assert result.exit_code == 0, result.output
        assert mock_oauth.call_args.kwargs["use_pkce"] is True
        opts = mock_ws.api_client.do.call_args.kwargs["body"]["options"]
        assert opts["pkce_verifier"] == "VER"


class TestMakeWorkspaceClient:
    """Tests for the WorkspaceClient profile-resolution helper."""

    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    def test_env_var_defers_to_sdk(self, mock_workspace_client, monkeypatch):
        """When DATABRICKS_CONFIG_PROFILE is set, the SDK reads it directly."""
        monkeypatch.setenv("DATABRICKS_CONFIG_PROFILE", "my-profile")
        from databricks.labs.community_connector_cli.cli import _make_workspace_client

        _make_workspace_client()

        mock_workspace_client.assert_called_once_with()

    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    def test_falls_back_to_default_profile(self, mock_workspace_client, monkeypatch):
        """Without the env var, force the DEFAULT profile to disambiguate."""
        monkeypatch.delenv("DATABRICKS_CONFIG_PROFILE", raising=False)
        monkeypatch.delenv("DATABRICKS_HOST", raising=False)
        from databricks.labs.community_connector_cli.cli import _make_workspace_client

        _make_workspace_client()

        mock_workspace_client.assert_called_once_with(profile="DEFAULT")

    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    def test_databricks_host_env_skips_default_profile(
        self, mock_workspace_client, monkeypatch
    ):
        """DATABRICKS_HOST signals env-var auth — defer to the SDK so users whose
        ~/.databrickscfg has no [DEFAULT] section don't break on file resolution."""
        monkeypatch.delenv("DATABRICKS_CONFIG_PROFILE", raising=False)
        monkeypatch.setenv("DATABRICKS_HOST", "https://example.cloud.databricks.com")
        from databricks.labs.community_connector_cli.cli import _make_workspace_client

        _make_workspace_client()

        mock_workspace_client.assert_called_once_with()


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
        assert "--schema" in result.output


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

    def test_update_pipeline_requires_spec_or_package(self):
        """Test that at least --pipeline-spec or --package is required."""
        runner = CliRunner()

        with patch("databricks.labs.community_connector_cli.cli.WorkspaceClient"):
            result = runner.invoke(
                main,
                ["update_pipeline", "my_pipeline", "--use-workspace-pipeline"],
            )

        assert result.exit_code != 0
        assert "At least one of --pipeline-spec or --package must be provided" in result.output

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
                "--use-workspace-pipeline",
            ],
        )

        assert result.exit_code == 0
        assert "Pipeline updated successfully" in result.output
        assert "pipeline-123" in result.output

        # Verify the workspace file was created
        mock_create_file.assert_called_once()
        call_args = mock_create_file.call_args
        assert "ingest.py" in call_args[0][1]  # path contains ingest.py

    @patch("databricks.labs.community_connector_cli.cli._create_workspace_file")
    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    @patch("databricks.labs.community_connector_cli.cli.PipelineClient")
    def test_update_pipeline_with_package(
        self, mock_pipeline_client, mock_workspace_client, mock_create_file
    ):
        """Test update_pipeline with --package option alongside --pipeline-spec."""
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
        mock_pipeline_info.spec.catalog = "main"
        mock_pipeline_info.spec.schema = "default"
        mock_pipeline_info.spec.environment = None
        mock_pipeline_info.spec.as_dict.return_value = {"name": "test"}
        mock_client.get.return_value = mock_pipeline_info

        # Setup workspace export mock (for reading existing ingest.py)
        mock_export_response = MagicMock()
        existing_content = 'source_name = "github"\npipeline_spec = {}'
        mock_export_response.content = base64.b64encode(existing_content.encode()).decode()
        mock_ws.workspace.export.return_value = mock_export_response

        with tempfile.NamedTemporaryFile(suffix=".whl") as temp_pkg:
            temp_pkg.write(b"dummy wheel content")
            temp_pkg.flush()

            result = runner.invoke(
                main,
                [
                    "update_pipeline",
                    "my_pipeline",
                    "-ps",
                    '{"connection_name": "my_conn", '
                    '"objects": [{"table": {"source_table": "users"}}]}',
                    "--package",
                    temp_pkg.name,
                    "--use-workspace-pipeline",
                ],
            )

        assert result.exit_code == 0, f"Exit code: {result.exit_code}\nOutput: {result.output}"
        assert "Using local connector packages" in result.output
        assert "Package uploaded successfully" in result.output
        assert "Pipeline dependencies updated" in result.output

        # Verify Files API was used
        mock_ws.files.upload.assert_called_once()
        mock_ws.pipelines.update.assert_called_once()

    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    @patch("databricks.labs.community_connector_cli.cli.PipelineClient")
    def test_update_pipeline_package_only(self, mock_pipeline_client, mock_workspace_client):
        """Test update_pipeline with --package only (no --pipeline-spec)."""
        runner = CliRunner()

        mock_ws = MagicMock()
        mock_workspace_client.return_value = mock_ws
        mock_ws.config.host = "https://test.databricks.com"

        mock_pipeline_obj = MagicMock()
        mock_pipeline_obj.pipeline_id = "pipeline-123"
        mock_ws.pipelines.list_pipelines.return_value = [mock_pipeline_obj]

        mock_client = MagicMock()
        mock_pipeline_client.return_value = mock_client
        mock_pipeline_info = MagicMock()
        mock_pipeline_info.spec.catalog = "main"
        mock_pipeline_info.spec.schema = "default"
        mock_pipeline_info.spec.environment = None
        mock_client.get.return_value = mock_pipeline_info

        with tempfile.NamedTemporaryFile(suffix=".whl") as temp_pkg:
            temp_pkg.write(b"dummy wheel content")
            temp_pkg.flush()

            result = runner.invoke(
                main,
                ["update_pipeline", "my_pipeline", "-p", temp_pkg.name,
                 "--use-workspace-pipeline"],
            )

        assert result.exit_code == 0, f"Exit code: {result.exit_code}\nOutput: {result.output}"
        assert "Package uploaded successfully" in result.output
        assert "Pipeline dependencies updated" in result.output
        assert "Pipeline updated successfully" in result.output
        # Should NOT read or update ingest.py
        mock_ws.workspace.export.assert_not_called()

    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    @patch("databricks.labs.community_connector_cli.cli.PipelineClient")
    def test_update_pipeline_multiple_packages_only(
        self, mock_pipeline_client, mock_workspace_client
    ):
        """Test update_pipeline with multiple --package options (no --pipeline-spec)."""
        runner = CliRunner()

        mock_ws = MagicMock()
        mock_workspace_client.return_value = mock_ws
        mock_ws.config.host = "https://test.databricks.com"

        mock_pipeline_obj = MagicMock()
        mock_pipeline_obj.pipeline_id = "pipeline-123"
        mock_ws.pipelines.list_pipelines.return_value = [mock_pipeline_obj]

        mock_client = MagicMock()
        mock_pipeline_client.return_value = mock_client
        mock_pipeline_info = MagicMock()
        mock_pipeline_info.spec.catalog = "main"
        mock_pipeline_info.spec.schema = "default"
        mock_pipeline_info.spec.environment = None
        mock_client.get.return_value = mock_pipeline_info

        with tempfile.NamedTemporaryFile(suffix=".whl") as pkg1, \
             tempfile.NamedTemporaryFile(suffix=".whl") as pkg2:
            pkg1.write(b"wheel1")
            pkg1.flush()
            pkg2.write(b"wheel2")
            pkg2.flush()

            result = runner.invoke(
                main,
                ["update_pipeline", "my_pipeline", "-p", pkg1.name, "-p", pkg2.name,
                 "--use-workspace-pipeline"],
            )

        assert result.exit_code == 0, f"Exit code: {result.exit_code}\nOutput: {result.output}"
        assert mock_ws.files.upload.call_count == 2
        mock_ws.pipelines.update.assert_called_once()

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
                '{"connection_name": "conn", "objects": [{"table": {"source_table": "t"}}]}',
                "--use-workspace-pipeline",
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
                '{"connection_name": "conn", "objects": [{"table": {"source_table": "t"}}]}',
                "--use-workspace-pipeline",
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


class TestManagedCreatePipeline:
    """Tests for the default (managed ingestion) create_pipeline mode."""

    def _bare_spec(self):
        return (
            '{"connection_name": "my_conn", '
            '"objects": [{"table": {"source_table": "commits"}}]}'
        )

    @patch("databricks.labs.community_connector_cli.cli._build_and_upload_managed_wheels")
    @patch("databricks.labs.community_connector_cli.cli._resolve_managed_dest_dir")
    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    def test_managed_create_bare_spec(
        self, mock_ws_client, mock_dest_dir, mock_build_upload
    ):
        runner = CliRunner()
        mock_ws = MagicMock()
        mock_ws_client.return_value = mock_ws
        mock_ws.config.host = "https://test.databricks.com"
        mock_ws.api_client.do.return_value = {"pipeline_id": "pl-123"}
        mock_dest_dir.return_value = "/Volumes/main/default/community_connector/packages"
        mock_build_upload.return_value = [
            "/Volumes/main/default/community_connector/packages/w.whl"]

        result = runner.invoke(
            main,
            ["create_pipeline", "github", "my_pipeline", "-ps", self._bare_spec(),
             "-c", "cat", "-t", "sch"],
        )

        assert result.exit_code == 0, f"Output: {result.output}"
        assert "Pipeline created!" in result.output
        # POST to the pipelines endpoint with a managed body.
        args, kwargs = mock_ws.api_client.do.call_args
        assert args[0] == "POST"
        assert args[1] == "/api/2.0/pipelines"
        body = kwargs["body"]
        assert body["ingestion_definition"]["source_type"] == "COMMUNITY"
        assert body["ingestion_definition"]["connection_name"] == "my_conn"
        assert body["catalog"] == "cat"
        assert body["configuration"][
            "pipelines.managedIngestion.registerPythonDataSource"] == "true"
        assert body["environment"]["dependencies"]

    def test_managed_create_requires_spec_or_connection(self):
        runner = CliRunner()
        with patch("databricks.labs.community_connector_cli.cli.WorkspaceClient"):
            result = runner.invoke(
                main, ["create_pipeline", "github", "my_pipeline"]
            )
        assert result.exit_code != 0
        assert "Either --pipeline-spec or --connection-name" in result.output

    @patch("databricks.labs.community_connector_cli.cli._build_and_upload_managed_wheels")
    @patch("databricks.labs.community_connector_cli.cli._resolve_managed_dest_dir")
    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    def test_managed_create_empty_pipeline_from_connection(
        self, mock_ws_client, mock_dest_dir, mock_build_upload
    ):
        """No --pipeline-spec: create an empty pipeline (no objects) from -n."""
        runner = CliRunner()
        mock_ws = MagicMock()
        mock_ws_client.return_value = mock_ws
        mock_ws.config.host = "https://test.databricks.com"
        mock_ws.api_client.do.return_value = {"pipeline_id": "pl-empty"}
        mock_dest_dir.return_value = "/Volumes/cat/sch/community_connector/packages"
        mock_build_upload.return_value = ["/Volumes/cat/sch/community_connector/packages/w.whl"]

        result = runner.invoke(
            main,
            ["create_pipeline", "github", "my_pipeline", "-n", "my_conn",
             "-c", "cat", "-t", "sch"],
        )

        assert result.exit_code == 0, f"Output: {result.output}"
        assert "empty pipeline" in result.output
        body = mock_ws.api_client.do.call_args.kwargs["body"]
        assert body["ingestion_definition"]["connection_name"] == "my_conn"
        assert body["ingestion_definition"]["objects"] == []
        assert body["ingestion_definition"]["source_type"] == "COMMUNITY"
        assert body["catalog"] == "cat"

    @patch("databricks.labs.community_connector_cli.cli._build_and_upload_managed_wheels")
    @patch("databricks.labs.community_connector_cli.cli._resolve_managed_dest_dir")
    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    def test_managed_create_full_spec_with_environment_skips_upload(
        self, mock_ws_client, mock_dest_dir, mock_build_upload
    ):
        runner = CliRunner()
        mock_ws = MagicMock()
        mock_ws_client.return_value = mock_ws
        mock_ws.config.host = "https://test.databricks.com"
        mock_ws.api_client.do.return_value = {"pipeline_id": "pl-9"}

        full_spec = (
            '{"name": "p", "catalog": "cat", "schema": "sch", '
            '"ingestion_definition": {"connection_name": "c", '
            '"objects": [{"table": {"source_table": "t", '
            '"destination_catalog": "cat", "destination_schema": "sch"}}]}, '
            '"environment": {"dependencies": ["/Volumes/x/y/z/pre.whl"]}}'
        )
        result = runner.invoke(
            main, ["create_pipeline", "github", "my_pipeline", "-ps", full_spec]
        )

        assert result.exit_code == 0, f"Output: {result.output}"
        mock_build_upload.assert_not_called()
        mock_dest_dir.assert_not_called()
        body = mock_ws.api_client.do.call_args.kwargs["body"]
        assert body["environment"]["dependencies"] == ["/Volumes/x/y/z/pre.whl"]

    @patch("databricks.labs.community_connector_cli.cli._build_and_upload_managed_wheels")
    @patch("databricks.labs.community_connector_cli.cli._resolve_managed_dest_dir")
    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    def test_managed_create_full_spec_catalog_schema_from_spec(
        self, mock_ws_client, mock_dest_dir, mock_build_upload
    ):
        """When --catalog/--schema are omitted, the volume uses the spec's values."""
        runner = CliRunner()
        mock_ws = MagicMock()
        mock_ws_client.return_value = mock_ws
        mock_ws.config.host = "https://test.databricks.com"
        mock_ws.api_client.do.return_value = {"pipeline_id": "pl-1"}
        mock_dest_dir.return_value = "/Volumes/spec_cat/spec_sch/community_connector/packages"
        mock_build_upload.return_value = [
            "/Volumes/spec_cat/spec_sch/community_connector/packages/w.whl"]

        full_spec = (
            '{"name": "p", "catalog": "spec_cat", "schema": "spec_sch", '
            '"ingestion_definition": {"connection_name": "c", '
            '"objects": [{"table": {"source_table": "t", '
            '"destination_catalog": "spec_cat", "destination_schema": "spec_sch"}}]}}'
        )
        result = runner.invoke(
            main, ["create_pipeline", "github", "my_pipeline", "-ps", full_spec]
        )

        assert result.exit_code == 0, f"Output: {result.output}"
        # Volume resolution is called with the spec's catalog/schema, not main/default.
        # Positional args: (ws, volume_path, catalog, schema, debug)
        call = mock_dest_dir.call_args
        assert call.args[2] == "spec_cat"
        assert call.args[3] == "spec_sch"

    @staticmethod
    def _mock_get_with_spec(mock_ws, spec_dict):
        """Wire pipelines.get(...) to return a spec whose as_dict() == spec_dict."""
        mock_info = MagicMock()
        mock_info.spec.as_dict.return_value = spec_dict
        mock_ws.pipelines.get.return_value = mock_info

    @patch("databricks.labs.community_connector_cli.cli._build_and_upload_managed_wheels")
    @patch("databricks.labs.community_connector_cli.cli._resolve_managed_dest_dir")
    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    def test_managed_update_uses_put(
        self, mock_ws_client, mock_dest_dir, mock_build_upload
    ):
        runner = CliRunner()
        mock_ws = MagicMock()
        mock_ws_client.return_value = mock_ws
        mock_ws.config.host = "https://test.databricks.com"
        mock_pipeline_obj = MagicMock()
        mock_pipeline_obj.pipeline_id = "pl-77"
        mock_ws.pipelines.list_pipelines.return_value = [mock_pipeline_obj]
        self._mock_get_with_spec(mock_ws, {"name": "my_pipeline", "catalog": "cat"})
        mock_dest_dir.return_value = "/Volumes/cat/sch/community_connector/packages"
        mock_build_upload.return_value = ["/Volumes/cat/sch/community_connector/packages/w.whl"]

        result = runner.invoke(
            main,
            ["update_pipeline", "my_pipeline", "-ps", self._bare_spec(),
             "-s", "github", "-c", "cat", "-t", "sch"],
        )

        assert result.exit_code == 0, f"Output: {result.output}"
        args, kwargs = mock_ws.api_client.do.call_args
        assert args[0] == "PUT"
        assert args[1] == "/api/2.0/pipelines/pl-77"
        assert kwargs["body"]["id"] == "pl-77"

    @patch("databricks.labs.community_connector_cli.cli._build_and_upload_managed_wheels")
    @patch("databricks.labs.community_connector_cli.cli._resolve_managed_dest_dir")
    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    def test_managed_update_no_source_reuses_existing_deps(
        self, mock_ws_client, mock_dest_dir, mock_build_upload
    ):
        """Update without -s/--package must not rebuild; it reuses existing deps."""
        runner = CliRunner()
        mock_ws = MagicMock()
        mock_ws_client.return_value = mock_ws
        mock_ws.config.host = "https://test.databricks.com"
        mock_pipeline_obj = MagicMock()
        mock_pipeline_obj.pipeline_id = "pl-88"
        mock_ws.pipelines.list_pipelines.return_value = [mock_pipeline_obj]

        # Existing pipeline already points at wheels on a volume.
        existing = ["/Volumes/cat/sch/community_connector/packages/existing.whl"]
        self._mock_get_with_spec(mock_ws, {
            "catalog": "cat", "schema": "sch",
            "environment": {"dependencies": existing},
        })

        result = runner.invoke(
            main,
            ["update_pipeline", "my_pipeline", "-ps", self._bare_spec()],
        )

        assert result.exit_code == 0, f"Output: {result.output}"
        # No build/upload and no volume resolution.
        mock_build_upload.assert_not_called()
        mock_dest_dir.assert_not_called()
        # Existing dependencies are carried into the new body.
        body = mock_ws.api_client.do.call_args.kwargs["body"]
        assert body["environment"]["dependencies"] == existing

    @patch("databricks.labs.community_connector_cli.cli._build_and_upload_managed_wheels")
    @patch("databricks.labs.community_connector_cli.cli._resolve_managed_dest_dir")
    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    def test_managed_update_bare_spec_preserves_existing_settings(
        self, mock_ws_client, mock_dest_dir, mock_build_upload
    ):
        """A bare-spec update must merge onto the existing spec (full-replace PUT).

        Unmanaged fields (tags, notifications, channel, serverless) and
        catalog/schema must survive even when the CLI options omit them.
        """
        runner = CliRunner()
        mock_ws = MagicMock()
        mock_ws_client.return_value = mock_ws
        mock_ws.config.host = "https://test.databricks.com"
        mock_pipeline_obj = MagicMock()
        mock_pipeline_obj.pipeline_id = "pl-99"
        mock_ws.pipelines.list_pipelines.return_value = [mock_pipeline_obj]
        mock_dest_dir.return_value = "/Volumes/livecat/livesch/community_connector/packages"
        mock_build_upload.return_value = [
            "/Volumes/livecat/livesch/community_connector/packages/w.whl"]

        # Live pipeline has settings the CLI does not manage plus CURRENT channel.
        self._mock_get_with_spec(mock_ws, {
            "name": "my_pipeline",
            "catalog": "livecat",
            "schema": "livesch",
            "channel": "CURRENT",
            "serverless": False,
            "tags": {"team": "data"},
            "notifications": [{"email_recipients": ["a@b.com"]}],
            "budget_policy_id": "bp-1",
            "environment": {"dependencies": ["/Volumes/old/old/old/old.whl"]},
        })

        # Rebuild wheels (-s) but omit -c/-t, channel, serverless.
        result = runner.invoke(
            main,
            ["update_pipeline", "my_pipeline", "-ps", self._bare_spec(), "-s", "github"],
        )

        assert result.exit_code == 0, f"Output: {result.output}"
        body = mock_ws.api_client.do.call_args.kwargs["body"]
        # Unmanaged fields preserved.
        assert body["tags"] == {"team": "data"}
        assert body["notifications"] == [{"email_recipients": ["a@b.com"]}]
        assert body["budget_policy_id"] == "bp-1"
        # Existing channel/serverless not overridden by managed defaults.
        assert body["channel"] == "CURRENT"
        assert body["serverless"] is False
        # catalog/schema fall back to the live pipeline's values.
        assert body["catalog"] == "livecat"
        assert body["schema"] == "livesch"
        # Managed additions still applied.
        assert body["configuration"][
            "pipelines.managedIngestion.registerPythonDataSource"] == "true"
        assert body["ingestion_definition"]["source_type"] == "COMMUNITY"
        # Rebuilt wheels replace the old dependencies.
        assert body["environment"]["dependencies"] == [
            "/Volumes/livecat/livesch/community_connector/packages/w.whl"]
        # Per-table destinations backfilled from the live catalog/schema.
        table = body["ingestion_definition"]["objects"][0]["table"]
        assert table["destination_catalog"] == "livecat"
        assert table["destination_schema"] == "livesch"


def _make_fake_wheel(path: Path, source_name: str) -> Path:
    """Build a minimal in-process zip that looks like a connector wheel.

    Only used by the wheel-layout and upload-command tests — we never invoke
    the real ``python -m build`` from unit tests.
    """
    import zipfile  # local to keep top-of-file imports unchanged
    wheel = path / f"lakeflow_community_connectors_{source_name}-0.1.0-py3-none-any.whl"
    namespace = f"databricks/labs/community_connector/sources/{source_name}"
    with zipfile.ZipFile(wheel, "w") as zf:
        zf.writestr(f"{namespace}/__init__.py", "")
        zf.writestr(f"{namespace}/{source_name}.py", "# connector code")
        zf.writestr(
            f"lakeflow_community_connectors_{source_name}-0.1.0.dist-info/METADATA",
            "Metadata-Version: 2.1\nName: lakeflow-community-connectors-"
            f"{source_name}\nVersion: 0.1.0\n",
        )
    return wheel


def _make_fake_framework_wheel(path: Path) -> Path:
    """Build a minimal in-process zip that looks like the framework wheel."""
    import zipfile
    wheel = path / "lakeflow_community_connectors-0.1.0-py3-none-any.whl"
    namespace = "databricks/labs/community_connector/interface"
    with zipfile.ZipFile(wheel, "w") as zf:
        zf.writestr(f"{namespace}/__init__.py", "")
        zf.writestr(f"{namespace}/lakeflow_connect.py", "# framework code")
        zf.writestr(
            "lakeflow_community_connectors-0.1.0.dist-info/METADATA",
            "Metadata-Version: 2.1\nName: lakeflow-community-connectors\nVersion: 0.1.0\n",
        )
    return wheel


def _make_fake_repo_root(tmp_path: Path, source_name: str = "example") -> tuple:
    """Lay out a tmp directory that looks like the connectors repo.

    Returns (repo_root, source_dir). Both have ``pyproject.toml`` files: the
    root's marks itself as ``name = "lakeflow-community-connectors"`` so
    ``_find_repo_root`` can identify it.
    """
    repo_root = tmp_path / "repo"
    source_dir = (
        repo_root / "src" / "databricks" / "labs"
        / "community_connector" / "sources" / source_name
    )
    source_dir.mkdir(parents=True)
    (repo_root / "pyproject.toml").write_text(
        '[project]\nname = "lakeflow-community-connectors"\nversion = "0.1.0"\n'
    )
    (source_dir / "pyproject.toml").write_text(
        f'[project]\nname = "lakeflow-community-connectors-{source_name}"\n'
        'version = "0.1.0"\n'
    )
    return repo_root, source_dir


class TestParseVolumePath:
    """Tests for _parse_volume_path."""

    def test_volume_root(self):
        assert _parse_volume_path("/Volumes/main/default/cc") == (
            "main", "default", "cc", "",
        )

    def test_with_subpath(self):
        assert _parse_volume_path("/Volumes/main/default/cc/packages") == (
            "main", "default", "cc", "packages",
        )

    def test_trailing_slash_normalized(self):
        assert _parse_volume_path("/Volumes/main/default/cc/packages/") == (
            "main", "default", "cc", "packages",
        )

    def test_nested_subpath(self):
        assert _parse_volume_path("/Volumes/c/s/v/a/b/c") == ("c", "s", "v", "a/b/c")

    def test_invalid_raises(self):
        with pytest.raises(click.ClickException, match="Invalid volume path"):
            _parse_volume_path("/Workspace/Users/me/wheels")

    def test_missing_volume_part_raises(self):
        with pytest.raises(click.ClickException, match="Invalid volume path"):
            _parse_volume_path("/Volumes/main/default")


class TestEnsureVolumeDirectory:
    """Tests for _ensure_volume_directory — volume + subdir auto-create."""

    def test_volume_exists_no_subpath(self):
        ws = MagicMock()
        ws.volumes.read.return_value = MagicMock()  # volume exists
        result = _ensure_volume_directory(ws, "/Volumes/c/s/v", debug=False)
        assert result == "/Volumes/c/s/v"
        ws.volumes.create.assert_not_called()
        ws.files.create_directory.assert_not_called()

    def test_volume_created_when_missing(self):
        ws = MagicMock()
        ws.volumes.read.side_effect = Exception("NOT_FOUND")
        result = _ensure_volume_directory(ws, "/Volumes/c/s/v", debug=False)
        assert result == "/Volumes/c/s/v"
        ws.volumes.create.assert_called_once()
        kwargs = ws.volumes.create.call_args.kwargs
        assert kwargs["catalog_name"] == "c"
        assert kwargs["schema_name"] == "s"
        assert kwargs["name"] == "v"

    def test_subdir_created(self):
        ws = MagicMock()
        ws.volumes.read.return_value = MagicMock()
        result = _ensure_volume_directory(ws, "/Volumes/c/s/v/pkg", debug=False)
        assert result == "/Volumes/c/s/v/pkg"
        ws.files.create_directory.assert_called_once_with("/Volumes/c/s/v/pkg")

    def test_subdir_already_exists_is_swallowed(self):
        ws = MagicMock()
        ws.volumes.read.return_value = MagicMock()
        ws.files.create_directory.side_effect = Exception("RESOURCE_ALREADY_EXISTS")
        result = _ensure_volume_directory(ws, "/Volumes/c/s/v/pkg", debug=False)
        assert result == "/Volumes/c/s/v/pkg"

    def test_volume_create_already_exists_swallowed(self):
        """Race condition: read fails but create reports ALREADY_EXISTS."""
        ws = MagicMock()
        ws.volumes.read.side_effect = Exception("NOT_FOUND")
        ws.volumes.create.side_effect = Exception("ALREADY_EXISTS")
        result = _ensure_volume_directory(ws, "/Volumes/c/s/v", debug=False)
        assert result == "/Volumes/c/s/v"

    def test_volume_create_other_error_raises(self):
        ws = MagicMock()
        ws.volumes.read.side_effect = Exception("NOT_FOUND")
        ws.volumes.create.side_effect = Exception("PERMISSION_DENIED")
        with pytest.raises(click.ClickException, match="Failed to create volume"):
            _ensure_volume_directory(ws, "/Volumes/c/s/v", debug=False)

    def test_subdir_unexpected_error_raises(self):
        ws = MagicMock()
        ws.volumes.read.return_value = MagicMock()
        ws.files.create_directory.side_effect = Exception("PERMISSION_DENIED")
        with pytest.raises(click.ClickException, match="Failed to create directory"):
            _ensure_volume_directory(ws, "/Volumes/c/s/v/pkg", debug=False)


class TestValidateWheelLayout:
    """Tests for _validate_wheel_layout."""

    def test_valid_layout(self, tmp_path):
        wheel = _make_fake_wheel(tmp_path, "example")
        _validate_wheel_layout(wheel, "example")  # no exception

    def test_wrong_source_name_raises(self, tmp_path):
        wheel = _make_fake_wheel(tmp_path, "example")
        with pytest.raises(click.ClickException, match="does not contain"):
            _validate_wheel_layout(wheel, "different_source")

    def test_corrupt_wheel_raises(self, tmp_path):
        bad = tmp_path / "bogus.whl"
        bad.write_bytes(b"not a zip")
        with pytest.raises(click.ClickException, match="not a valid wheel"):
            _validate_wheel_layout(bad, "example")


class TestValidateFrameworkWheel:
    """Tests for _validate_framework_wheel."""

    def test_valid_framework_wheel(self, tmp_path):
        wheel = _make_fake_framework_wheel(tmp_path)
        _validate_framework_wheel(wheel)  # no exception

    def test_connector_wheel_rejected_as_framework(self, tmp_path):
        """A connector wheel does not contain the interface/ namespace."""
        wheel = _make_fake_wheel(tmp_path, "example")
        with pytest.raises(click.ClickException, match="does not contain"):
            _validate_framework_wheel(wheel)

    def test_corrupt_framework_wheel_raises(self, tmp_path):
        bad = tmp_path / "bogus.whl"
        bad.write_bytes(b"not a zip")
        with pytest.raises(click.ClickException, match="not a valid wheel"):
            _validate_framework_wheel(bad)


class TestFindRepoRoot:
    """Tests for _find_repo_root."""

    def test_walks_up_from_source_dir(self, tmp_path):
        repo_root, source_dir = _make_fake_repo_root(tmp_path)
        assert _find_repo_root(source_dir) == repo_root.resolve()

    def test_returns_none_when_no_framework_root(self, tmp_path):
        """No pyproject anywhere in the parent chain → None."""
        nested = tmp_path / "a" / "b" / "c"
        nested.mkdir(parents=True)
        assert _find_repo_root(nested) is None

    def test_skips_connector_pyproject(self, tmp_path):
        """A connector pyproject (different name) must not be mistaken for root."""
        _, source_dir = _make_fake_repo_root(tmp_path, "example")
        # The source_dir itself has a pyproject named
        # `lakeflow-community-connectors-example` — it should be skipped, and
        # the walk should continue until it finds the framework root above.
        result = _find_repo_root(source_dir)
        assert result is not None
        assert (result / "pyproject.toml").is_file()
        # Sanity: it's not the connector's pyproject.
        assert "sources" not in str(result)


class TestUploadCommand:
    """Tests for the `upload` Click command (two-wheel flow)."""

    def test_upload_default_builds_both_wheels(self, tmp_path):
        """Default path: framework + connector wheels are both built and uploaded."""
        repo_root, source_dir = _make_fake_repo_root(tmp_path, "example")

        def fake_build(src, outdir, debug):
            # Distinguish the two builds by the source path we receive.
            if Path(src).resolve() == source_dir.resolve():
                return _make_fake_wheel(outdir, "example")
            return _make_fake_framework_wheel(outdir)

        runner = CliRunner()
        with patch(
            "databricks.labs.community_connector_cli.cli._make_workspace_client"
        ) as mock_ws_factory, patch(
            "databricks.labs.community_connector_cli.cli._build_connector_wheel",
            side_effect=fake_build,
        ) as mock_build:
            mock_ws = MagicMock()
            mock_ws.volumes.read.return_value = MagicMock()
            mock_ws_factory.return_value = mock_ws

            result = runner.invoke(
                main,
                [
                    "upload",
                    "example",
                    "--volume-path",
                    "/Volumes/main/default/cc/packages",
                    "--source-dir",
                    str(source_dir),
                ],
            )

            assert result.exit_code == 0, result.output
            # Two builds (framework + connector), two uploads.
            assert mock_build.call_count == 2
            assert mock_ws.files.upload.call_count == 2

            # Framework must be uploaded first so that, if the cluster's pip
            # processes the deps array in order, the connector's
            # lakeflow-community-connectors dep resolves locally.
            first_dest = mock_ws.files.upload.call_args_list[0].args[0]
            second_dest = mock_ws.files.upload.call_args_list[1].args[0]
            assert "lakeflow_community_connectors-" in first_dest
            assert "lakeflow_community_connectors_example-" in second_dest

    def test_upload_skip_framework(self, tmp_path):
        """--skip-framework uploads only the connector wheel."""
        repo_root, source_dir = _make_fake_repo_root(tmp_path, "example")

        def fake_build(src, outdir, debug):
            return _make_fake_wheel(outdir, "example")

        runner = CliRunner()
        with patch(
            "databricks.labs.community_connector_cli.cli._make_workspace_client"
        ) as mock_ws_factory, patch(
            "databricks.labs.community_connector_cli.cli._build_connector_wheel",
            side_effect=fake_build,
        ) as mock_build:
            mock_ws = MagicMock()
            mock_ws.volumes.read.return_value = MagicMock()
            mock_ws_factory.return_value = mock_ws

            result = runner.invoke(
                main,
                [
                    "upload",
                    "example",
                    "--volume-path",
                    "/Volumes/main/default/cc/packages",
                    "--source-dir",
                    str(source_dir),
                    "--skip-framework",
                ],
            )

            assert result.exit_code == 0, result.output
            mock_build.assert_called_once()  # only the connector
            mock_ws.files.upload.assert_called_once()

    def test_upload_with_prebuilt_connector_wheel(self, tmp_path):
        """--wheel skips the connector build but still builds the framework."""
        repo_root, source_dir = _make_fake_repo_root(tmp_path, "example")
        prebuilt = _make_fake_wheel(tmp_path, "example")

        def fake_build(src, outdir, debug):
            return _make_fake_framework_wheel(outdir)

        runner = CliRunner()
        with patch(
            "databricks.labs.community_connector_cli.cli._make_workspace_client"
        ) as mock_ws_factory, patch(
            "databricks.labs.community_connector_cli.cli._build_connector_wheel",
            side_effect=fake_build,
        ) as mock_build:
            mock_ws = MagicMock()
            mock_ws.volumes.read.return_value = MagicMock()
            mock_ws_factory.return_value = mock_ws

            result = runner.invoke(
                main,
                [
                    "upload",
                    "example",
                    "--volume-path",
                    "/Volumes/main/default/cc/packages",
                    "--source-dir",
                    str(source_dir),
                    "--wheel",
                    str(prebuilt),
                ],
            )

            assert result.exit_code == 0, result.output
            mock_build.assert_called_once()  # only the framework
            assert mock_ws.files.upload.call_count == 2

    def test_upload_with_prebuilt_framework_wheel(self, tmp_path):
        """--framework-wheel skips the framework build but still builds the connector."""
        repo_root, source_dir = _make_fake_repo_root(tmp_path, "example")
        fw_prebuilt = _make_fake_framework_wheel(tmp_path)

        def fake_build(src, outdir, debug):
            return _make_fake_wheel(outdir, "example")

        runner = CliRunner()
        with patch(
            "databricks.labs.community_connector_cli.cli._make_workspace_client"
        ) as mock_ws_factory, patch(
            "databricks.labs.community_connector_cli.cli._build_connector_wheel",
            side_effect=fake_build,
        ) as mock_build:
            mock_ws = MagicMock()
            mock_ws.volumes.read.return_value = MagicMock()
            mock_ws_factory.return_value = mock_ws

            result = runner.invoke(
                main,
                [
                    "upload",
                    "example",
                    "--volume-path",
                    "/Volumes/main/default/cc/packages",
                    "--source-dir",
                    str(source_dir),
                    "--framework-wheel",
                    str(fw_prebuilt),
                ],
            )

            assert result.exit_code == 0, result.output
            mock_build.assert_called_once()  # only the connector
            assert mock_ws.files.upload.call_count == 2

    def test_upload_with_both_prebuilt_wheels(self, tmp_path):
        """Pre-built connector + pre-built framework → no build at all."""
        repo_root, source_dir = _make_fake_repo_root(tmp_path, "example")
        conn_prebuilt = _make_fake_wheel(tmp_path, "example")
        fw_prebuilt = _make_fake_framework_wheel(tmp_path)

        runner = CliRunner()
        with patch(
            "databricks.labs.community_connector_cli.cli._make_workspace_client"
        ) as mock_ws_factory, patch(
            "databricks.labs.community_connector_cli.cli._build_connector_wheel"
        ) as mock_build:
            mock_ws = MagicMock()
            mock_ws.volumes.read.return_value = MagicMock()
            mock_ws_factory.return_value = mock_ws

            result = runner.invoke(
                main,
                [
                    "upload",
                    "example",
                    "--volume-path",
                    "/Volumes/main/default/cc/packages",
                    "--source-dir",
                    str(source_dir),
                    "--wheel",
                    str(conn_prebuilt),
                    "--framework-wheel",
                    str(fw_prebuilt),
                ],
            )

            assert result.exit_code == 0, result.output
            mock_build.assert_not_called()
            assert mock_ws.files.upload.call_count == 2

    def test_upload_skip_framework_and_framework_wheel_mutually_exclusive(self, tmp_path):
        """--skip-framework + --framework-wheel is contradictory and must error."""
        repo_root, source_dir = _make_fake_repo_root(tmp_path, "example")
        fw_prebuilt = _make_fake_framework_wheel(tmp_path)

        runner = CliRunner()
        with patch(
            "databricks.labs.community_connector_cli.cli._make_workspace_client"
        ) as mock_ws_factory:
            mock_ws_factory.return_value = MagicMock()
            result = runner.invoke(
                main,
                [
                    "upload",
                    "example",
                    "--volume-path",
                    "/Volumes/main/default/cc/packages",
                    "--source-dir",
                    str(source_dir),
                    "--skip-framework",
                    "--framework-wheel",
                    str(fw_prebuilt),
                ],
            )

            assert result.exit_code != 0
            assert "mutually exclusive" in result.output

    def test_upload_raises_when_repo_root_not_found(self, tmp_path):
        """No framework root in the source's parent chain → ClickException."""
        # Build a source dir that has no root pyproject anywhere above it.
        bare_source = tmp_path / "isolated_source"
        bare_source.mkdir()
        (bare_source / "pyproject.toml").write_text(
            '[project]\nname = "lakeflow-community-connectors-example"\n'
        )

        runner = CliRunner()
        with patch(
            "databricks.labs.community_connector_cli.cli._make_workspace_client"
        ) as mock_ws_factory:
            mock_ws = MagicMock()
            mock_ws.volumes.read.return_value = MagicMock()
            mock_ws_factory.return_value = mock_ws

            result = runner.invoke(
                main,
                [
                    "upload",
                    "example",
                    "--volume-path",
                    "/Volumes/main/default/cc/packages",
                    "--source-dir",
                    str(bare_source),
                ],
            )

            assert result.exit_code != 0
            assert "Could not find the framework repo root" in result.output

    def test_upload_raises_when_source_not_found(self):
        """No --source-dir and the locator can't find anything → ClickException."""
        runner = CliRunner()
        with patch(
            "databricks.labs.community_connector_cli.cli._make_workspace_client"
        ) as mock_ws_factory, patch(
            "databricks.labs.community_connector_cli.cli._find_local_source_path",
            return_value=None,
        ):
            mock_ws = MagicMock()
            mock_ws.volumes.read.return_value = MagicMock()
            mock_ws_factory.return_value = mock_ws

            result = runner.invoke(
                main,
                [
                    "upload",
                    "ghost_source",
                    "--volume-path",
                    "/Volumes/main/default/cc/packages",
                ],
            )

            assert result.exit_code != 0
            assert "Could not find source directory" in result.output

    def test_upload_invalid_volume_path(self, tmp_path):
        """Malformed --volume-path raises before any network call."""
        repo_root, source_dir = _make_fake_repo_root(tmp_path, "example")
        wheel = _make_fake_wheel(tmp_path, "example")
        runner = CliRunner()
        with patch(
            "databricks.labs.community_connector_cli.cli._make_workspace_client"
        ) as mock_ws_factory:
            mock_ws_factory.return_value = MagicMock()
            result = runner.invoke(
                main,
                [
                    "upload",
                    "example",
                    "--volume-path",
                    "/Workspace/Users/me/wheels",
                    "--source-dir",
                    str(source_dir),
                    "--wheel",
                    str(wheel),
                    "--skip-framework",
                ],
            )
            assert result.exit_code != 0
            assert "Invalid volume path" in result.output

    def test_upload_keep_wheel_copies_only_built_artifacts(self, tmp_path):
        """--keep-wheel copies wheels we built in this run, not user-supplied ones."""
        repo_root, source_dir = _make_fake_repo_root(tmp_path, "example")
        # User supplies framework wheel; CLI builds the connector wheel.
        user_supplied_dir = tmp_path / "user_supplied"
        user_supplied_dir.mkdir()
        fw_prebuilt = _make_fake_framework_wheel(user_supplied_dir)

        def fake_build(src, outdir, debug):
            return _make_fake_wheel(outdir, "example")

        keep_dir = tmp_path / "kept"

        runner = CliRunner()
        with patch(
            "databricks.labs.community_connector_cli.cli._make_workspace_client"
        ) as mock_ws_factory, patch(
            "databricks.labs.community_connector_cli.cli._build_connector_wheel",
            side_effect=fake_build,
        ):
            mock_ws = MagicMock()
            mock_ws.volumes.read.return_value = MagicMock()
            mock_ws_factory.return_value = mock_ws

            result = runner.invoke(
                main,
                [
                    "upload",
                    "example",
                    "--volume-path",
                    "/Volumes/main/default/cc/packages",
                    "--source-dir",
                    str(source_dir),
                    "--framework-wheel",
                    str(fw_prebuilt),
                    "--keep-wheel",
                    str(keep_dir),
                ],
            )

            assert result.exit_code == 0, result.output
            kept_files = list(keep_dir.glob("*.whl"))
            # Only the connector wheel (which we built) should be copied —
            # not the user-supplied framework wheel.
            assert len(kept_files) == 1
            assert "example" in kept_files[0].name


class TestInterpolateOauthPlaceholders:
    """`{param}` interpolation in resolved OAuth connection options
    (parameter-in-a-parameter, e.g. a per-tenant token endpoint)."""

    def test_interpolates_tenant_id_into_token_endpoint(self):
        conn = {
            "token_endpoint": (
                "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
            ),
            "oauth_scope": "499b84ac/.default",
        }
        out = _interpolate_oauth_placeholders(conn, {"tenant_id": "T-123"})
        assert out["token_endpoint"] == (
            "https://login.microsoftonline.com/T-123/oauth2/v2.0/token"
        )
        # values without placeholders are untouched
        assert out["oauth_scope"] == "499b84ac/.default"

    def test_missing_referenced_param_raises(self):
        import click

        conn = {
            "token_endpoint": (
                "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
            )
        }
        with pytest.raises(click.ClickException, match="tenant_id"):
            _interpolate_oauth_placeholders(conn, {})

    def test_no_placeholders_is_noop(self):
        conn = {"token_endpoint": "https://example.com/token"}
        assert _interpolate_oauth_placeholders(conn, {}) == conn


class TestResolveDisplayName:
    """Tests for _resolve_display_name."""

    def test_explicit_wins(self):
        assert _resolve_display_name("My Name", {"display_name": "GitHub"}, "github") == "My Name"

    def test_falls_back_to_spec_display_name(self):
        assert _resolve_display_name(None, {"display_name": "GitHub"}, "github") == "GitHub"

    def test_falls_back_to_source_name(self):
        assert _resolve_display_name(None, {}, "github") == "github"
        assert _resolve_display_name(None, None, "github") == "github"

    def test_rejects_unsafe_filename(self):
        with pytest.raises(click.ClickException, match="not a valid filename"):
            _resolve_display_name("../evil", None, "github")
        with pytest.raises(click.ClickException, match="not a valid filename"):
            _resolve_display_name(None, {"display_name": "a/b"}, "github")


class TestBuildCommunityConnectorManifest:
    """Tests for _build_community_connector_manifest."""

    def test_field_derivation_and_null_logo(self):
        spec = {"connection": {"parameters": []}}
        manifest = _build_community_connector_manifest(
            "azure-devops", "Azure DevOps", spec, None
        )
        assert manifest["id"] == "AZURE_DEVOPS"
        assert manifest["sourceName"] == "azure-devops"
        assert manifest["displayName"] == "Azure DevOps"
        assert manifest["logo"] is None
        assert manifest["repositoryPath"] == ""
        assert manifest["readmeUrl"] == ""
        assert manifest["connectionSpec"] == spec
        # No dependencies key when none supplied.
        assert "dependencies" not in manifest

    def test_includes_dependencies_when_present(self):
        manifest = _build_community_connector_manifest(
            "github", "GitHub", None, ["/Volumes/main/default/community_connector/x.whl"]
        )
        assert manifest["connectionSpec"] is None
        assert manifest["dependencies"] == [
            "/Volumes/main/default/community_connector/x.whl"
        ]


class TestWriteCommunityConnectorManifest:
    """Tests for _write_community_connector_manifest."""

    def test_mkdirs_and_import_called(self):
        mock_ws = MagicMock()
        _write_community_connector_manifest(
            mock_ws,
            "/Users/me/.community-connectors",
            "/Users/me/.community-connectors/GitHub.connector.json",
            {"id": "GITHUB"},
            overwrite=True,
        )
        mock_ws.workspace.mkdirs.assert_called_once_with(
            "/Users/me/.community-connectors"
        )
        assert mock_ws.workspace.import_.call_count == 1
        _, kwargs = mock_ws.workspace.import_.call_args
        assert kwargs["path"] == "/Users/me/.community-connectors/GitHub.connector.json"
        assert kwargs["overwrite"] is True
        # Content is base64 of the JSON manifest.
        decoded = base64.b64decode(kwargs["content"]).decode("utf-8")
        assert json.loads(decoded) == {"id": "GITHUB"}

    def test_existing_dir_is_tolerated(self):
        mock_ws = MagicMock()
        mock_ws.workspace.mkdirs.side_effect = Exception("RESOURCE_ALREADY_EXISTS")
        _write_community_connector_manifest(
            mock_ws, "/Users/me/.community-connectors",
            "/Users/me/.community-connectors/x.connector.json", {"id": "X"},
            overwrite=True,
        )
        assert mock_ws.workspace.import_.call_count == 1

    def test_existing_file_without_overwrite_raises_hint(self):
        mock_ws = MagicMock()
        mock_ws.workspace.import_.side_effect = Exception("RESOURCE_ALREADY_EXISTS")
        with pytest.raises(click.ClickException, match="--overwrite"):
            _write_community_connector_manifest(
                mock_ws, "/Users/me/.community-connectors",
                "/Users/me/.community-connectors/x.connector.json", {"id": "X"},
                overwrite=False,
            )

    def test_mkdirs_error_other_than_already_exists_raises(self):
        mock_ws = MagicMock()
        mock_ws.workspace.mkdirs.side_effect = Exception("PERMISSION_DENIED")
        with pytest.raises(click.ClickException, match="Failed to create workspace directory"):
            _write_community_connector_manifest(
                mock_ws, "/Users/me/.community-connectors",
                "/Users/me/.community-connectors/x.connector.json", {"id": "X"},
                overwrite=True,
            )
        assert mock_ws.workspace.import_.call_count == 0


class TestPublishCommand:
    """Tests for the `publish` command."""

    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    @patch("databricks.labs.community_connector_cli.cli._build_and_upload_managed_wheels")
    @patch("databricks.labs.community_connector_cli.cli._resolve_managed_dest_dir")
    @patch("databricks.labs.community_connector_cli.cli._load_connector_spec")
    def test_publish_builds_wheels_and_writes_manifest(
        self, mock_load_spec, mock_dest_dir, mock_build_wheels, mock_ws_cls
    ):
        runner = CliRunner()
        mock_load_spec.return_value = {"display_name": "GitHub", "connection": {}}
        mock_dest_dir.return_value = "/Volumes/main/default/community_connector/packages"
        mock_build_wheels.return_value = [
            "/Volumes/main/default/community_connector/packages/fw.whl",
            "/Volumes/main/default/community_connector/packages/conn.whl",
        ]
        mock_ws = MagicMock()
        mock_ws_cls.return_value = mock_ws
        mock_ws.current_user.me.return_value.user_name = "me@example.com"

        result = runner.invoke(main, ["publish", "github"])

        assert result.exit_code == 0, result.output
        mock_build_wheels.assert_called_once()
        # Wrote the manifest to the user's .community-connectors dir.
        _, kwargs = mock_ws.workspace.import_.call_args
        assert kwargs["path"] == (
            "/Users/me@example.com/.community-connectors/GitHub.connector.json"
        )
        decoded = base64.b64decode(kwargs["content"]).decode("utf-8")
        manifest = json.loads(decoded)
        assert manifest["id"] == "GITHUB"
        assert manifest["displayName"] == "GitHub"
        assert manifest["logo"] is None
        assert manifest["dependencies"] == mock_build_wheels.return_value

    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    @patch("databricks.labs.community_connector_cli.cli._build_and_upload_managed_wheels")
    @patch("databricks.labs.community_connector_cli.cli._resolve_managed_dest_dir")
    @patch("databricks.labs.community_connector_cli.cli._load_connector_spec")
    def test_publish_uses_custom_display_name_in_path(
        self, mock_load_spec, mock_dest_dir, mock_build_wheels, mock_ws_cls
    ):
        runner = CliRunner()
        mock_load_spec.return_value = {"connection": {}}
        mock_dest_dir.return_value = "/Volumes/main/default/community_connector/packages"
        mock_build_wheels.return_value = []
        mock_ws = MagicMock()
        mock_ws_cls.return_value = mock_ws
        mock_ws.current_user.me.return_value.user_name = "me@example.com"

        result = runner.invoke(main, ["publish", "github", "-d", "My GitHub"])

        assert result.exit_code == 0, result.output
        _, kwargs = mock_ws.workspace.import_.call_args
        assert kwargs["path"] == (
            "/Users/me@example.com/.community-connectors/My GitHub.connector.json"
        )

    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    @patch("databricks.labs.community_connector_cli.cli._build_and_upload_managed_wheels")
    @patch("databricks.labs.community_connector_cli.cli._resolve_managed_dest_dir")
    @patch("databricks.labs.community_connector_cli.cli._load_connector_spec")
    def test_publish_with_package_bypasses_build(
        self, mock_load_spec, mock_dest_dir, mock_build_wheels, mock_ws_cls, tmp_path
    ):
        runner = CliRunner()
        mock_load_spec.return_value = {"connection": {}}
        mock_dest_dir.return_value = "/Volumes/main/default/community_connector/packages"
        wheel = tmp_path / "conn.whl"
        wheel.write_text("x")
        mock_build_wheels.return_value = [
            "/Volumes/main/default/community_connector/packages/conn.whl"
        ]
        mock_ws = MagicMock()
        mock_ws_cls.return_value = mock_ws
        mock_ws.current_user.me.return_value.user_name = "me@example.com"

        result = runner.invoke(
            main, ["publish", "github", "-p", str(wheel)]
        )

        assert result.exit_code == 0, result.output
        # Pre-built package path is forwarded to the upload helper as a tuple.
        _, kwargs = mock_build_wheels.call_args
        passed_packages = mock_build_wheels.call_args[0][3]
        assert passed_packages == (str(wheel),)

    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    @patch("databricks.labs.community_connector_cli.cli._upload_wheel")
    @patch("databricks.labs.community_connector_cli.cli._resolve_managed_dest_dir")
    @patch("databricks.labs.community_connector_cli.cli._load_connector_spec")
    def test_publish_package_bypass_uploads_without_building(
        self, mock_load_spec, mock_dest_dir, mock_upload_wheel, mock_ws_cls, tmp_path
    ):
        """--package uploads pre-built wheels as-is, exercising the real bypass
        branch of _build_and_upload_managed_wheels (no build step)."""
        runner = CliRunner()
        mock_load_spec.return_value = {"connection": {}}
        dest = "/Volumes/main/default/community_connector/packages"
        mock_dest_dir.return_value = dest
        mock_upload_wheel.side_effect = (
            lambda _ws, wheel_path, _dest: f"{dest}/{Path(wheel_path).name}"
        )
        wheel = tmp_path / "conn.whl"
        wheel.write_text("x")
        mock_ws = MagicMock()
        mock_ws_cls.return_value = mock_ws
        mock_ws.current_user.me.return_value.user_name = "me@example.com"

        result = runner.invoke(main, ["publish", "github", "-p", str(wheel)])

        assert result.exit_code == 0, result.output
        mock_upload_wheel.assert_called_once()
        manifest = json.loads(
            base64.b64decode(mock_ws.workspace.import_.call_args.kwargs["content"]).decode("utf-8")
        )
        assert manifest["dependencies"] == [f"{dest}/conn.whl"]

    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    @patch("databricks.labs.community_connector_cli.cli._build_and_upload_managed_wheels")
    @patch("databricks.labs.community_connector_cli.cli._resolve_managed_dest_dir")
    @patch("databricks.labs.community_connector_cli.cli._load_connector_spec")
    def test_publish_null_spec_writes_null_connection_spec_with_warning(
        self, mock_load_spec, mock_dest_dir, mock_build_wheels, mock_ws_cls
    ):
        runner = CliRunner()
        mock_load_spec.return_value = None
        mock_dest_dir.return_value = "/Volumes/main/default/community_connector/packages"
        mock_build_wheels.return_value = []
        mock_ws = MagicMock()
        mock_ws_cls.return_value = mock_ws
        mock_ws.current_user.me.return_value.user_name = "me@example.com"

        result = runner.invoke(main, ["publish", "github"])

        assert result.exit_code == 0, result.output
        assert "null connectionSpec" in result.output
        manifest = json.loads(
            base64.b64decode(mock_ws.workspace.import_.call_args.kwargs["content"]).decode("utf-8")
        )
        assert manifest["connectionSpec"] is None

    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    @patch("databricks.labs.community_connector_cli.cli._build_and_upload_managed_wheels")
    @patch("databricks.labs.community_connector_cli.cli._resolve_managed_dest_dir")
    @patch("databricks.labs.community_connector_cli.cli._load_connector_spec")
    def test_publish_forwards_overwrite_flag(
        self, mock_load_spec, mock_dest_dir, mock_build_wheels, mock_ws_cls
    ):
        runner = CliRunner()
        mock_load_spec.return_value = {"connection": {}}
        mock_dest_dir.return_value = "/Volumes/main/default/community_connector/packages"
        mock_build_wheels.return_value = []
        mock_ws = MagicMock()
        mock_ws_cls.return_value = mock_ws
        mock_ws.current_user.me.return_value.user_name = "me@example.com"

        result = runner.invoke(main, ["publish", "github", "--overwrite"])

        assert result.exit_code == 0, result.output
        assert mock_ws.workspace.import_.call_args.kwargs["overwrite"] is True


class TestUnpublishCommand:
    """Tests for the `unpublish` command."""

    @staticmethod
    def _existing_ws(user="me@example.com"):
        """A WorkspaceClient mock whose get_status succeeds (object exists)."""
        mock_ws = MagicMock()
        mock_ws.current_user.me.return_value.user_name = user
        return mock_ws

    @staticmethod
    def _missing_ws(user="me@example.com"):
        """A WorkspaceClient mock whose get_status raises RESOURCE_DOES_NOT_EXIST."""
        mock_ws = MagicMock()
        mock_ws.current_user.me.return_value.user_name = user
        mock_ws.workspace.get_status.side_effect = Exception("RESOURCE_DOES_NOT_EXIST")
        return mock_ws

    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    @patch("databricks.labs.community_connector_cli.cli._load_connector_spec")
    def test_unpublish_deletes_after_confirmation(self, mock_load_spec, mock_ws_cls):
        runner = CliRunner()
        mock_load_spec.return_value = {"display_name": "GitHub", "connection": {}}
        mock_ws = self._existing_ws()
        mock_ws_cls.return_value = mock_ws

        # Confirm the prompt with "y".
        result = runner.invoke(main, ["unpublish", "github"], input="y\n")

        assert result.exit_code == 0, result.output
        mock_ws.workspace.delete.assert_called_once_with(
            path="/Users/me@example.com/.community-connectors/GitHub.connector.json"
        )

    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    @patch("databricks.labs.community_connector_cli.cli._load_connector_spec")
    def test_unpublish_yes_skips_prompt(self, mock_load_spec, mock_ws_cls):
        runner = CliRunner()
        mock_load_spec.return_value = {"connection": {}}
        mock_ws = self._existing_ws()
        mock_ws_cls.return_value = mock_ws

        result = runner.invoke(main, ["unpublish", "github", "--yes"])

        assert result.exit_code == 0, result.output
        mock_ws.workspace.delete.assert_called_once_with(
            path="/Users/me@example.com/.community-connectors/github.connector.json"
        )

    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    @patch("databricks.labs.community_connector_cli.cli._load_connector_spec")
    def test_unpublish_uses_display_name_without_loading_spec(self, mock_load_spec, mock_ws_cls):
        runner = CliRunner()
        mock_ws = self._existing_ws()
        mock_ws_cls.return_value = mock_ws

        result = runner.invoke(main, ["unpublish", "github", "-d", "My GitHub", "--yes"])

        assert result.exit_code == 0, result.output
        # --display-name provided → the spec is not loaded at all.
        mock_load_spec.assert_not_called()
        mock_ws.workspace.delete.assert_called_once_with(
            path="/Users/me@example.com/.community-connectors/My GitHub.connector.json"
        )

    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    @patch("databricks.labs.community_connector_cli.cli._load_connector_spec")
    def test_unpublish_errors_when_not_found(self, mock_load_spec, mock_ws_cls):
        runner = CliRunner()
        mock_load_spec.return_value = {"connection": {}}
        mock_ws = self._missing_ws()
        mock_ws_cls.return_value = mock_ws

        result = runner.invoke(main, ["unpublish", "github", "--yes"])

        assert result.exit_code != 0
        assert "No published connector found" in result.output
        mock_ws.workspace.delete.assert_not_called()

    @patch("databricks.labs.community_connector_cli.cli.WorkspaceClient")
    @patch("databricks.labs.community_connector_cli.cli._load_connector_spec")
    def test_unpublish_aborts_when_declined(self, mock_load_spec, mock_ws_cls):
        runner = CliRunner()
        mock_load_spec.return_value = {"connection": {}}
        mock_ws = self._existing_ws()
        mock_ws_cls.return_value = mock_ws

        # Decline the confirmation prompt with "n".
        result = runner.invoke(main, ["unpublish", "github"], input="n\n")

        assert result.exit_code != 0
        mock_ws.workspace.delete.assert_not_called()
