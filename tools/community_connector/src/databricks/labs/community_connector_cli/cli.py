"""
Command-line interface for the Community Connector tool.

This module provides the CLI commands for setting up and running
Databricks Lakeflow community connectors.

Configuration Precedence:
    CLI arguments → --config file → default_config.yaml → code defaults
"""
# pylint: disable=too-many-lines

import base64
import json
import re
import traceback
from pathlib import Path
from typing import Optional, List, Set

import click
import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language

from databricks.labs.community_connector_cli.config import build_config, load_default_config
from databricks.labs.community_connector_cli.pipeline_client import PipelineClient
from databricks.labs.community_connector_cli.pipeline_spec_validator import (
    PipelineSpecValidationError,
    validate_pipeline_spec,
)
from databricks.labs.community_connector_cli.repo_client import RepoClient
from databricks.labs.community_connector_cli.connector_spec import (
    ParsedConnectorSpec,
    convert_github_url_to_raw,
    load_connector_spec,
    parse_connector_spec,
    parse_connector_spec_legacy,
    merge_external_options_allowlist,
    validate_connection_options,
    validate_connection_options_legacy,
)


# Re-export for backward compatibility with tests
_convert_github_url_to_raw = convert_github_url_to_raw
_parse_connector_spec = parse_connector_spec
_parse_connector_spec_legacy = parse_connector_spec_legacy
_merge_external_options_allowlist = merge_external_options_allowlist


def _get_default_repo_raw_url() -> str:
    """Get the default repository raw URL from default_config.yaml."""
    config = load_default_config()
    repo_config = config.get("repo", {})
    repo_url = repo_config.get(
        "url", "https://github.com/databrickslabs/lakeflow-community-connectors"
    )
    branch = repo_config.get("branch", "master")

    return convert_github_url_to_raw(repo_url, branch)


def _load_connector_spec(source_name: str, spec_path: Optional[str] = None) -> Optional[dict]:
    """Load connector_spec.yaml for a source. CLI wrapper with warning output."""
    return load_connector_spec(
        source_name=source_name,
        spec_path=spec_path,
        get_default_repo_url=_get_default_repo_raw_url,
        cli_file_path=__file__,
        warn_callback=lambda msg: click.echo(f"⚠️  Warning: {msg}", err=True),
    )


def _get_constant_external_options_allowlist() -> str:
    """Get constant external options allowlist from default config."""
    config = load_default_config()
    connection_config = config.get("connection", {})
    return connection_config.get("external_options_allowlist", "")


def _validate_connection_options_with_spec(
    source_name: str, options_dict: dict, parsed_spec: ParsedConnectorSpec
) -> List[str]:
    """Validate connection options against spec. Returns list of error messages."""
    result = validate_connection_options(source_name, options_dict, parsed_spec)

    # Print detected auth method
    if result.detected_auth_method:
        click.echo(f"  ✓ Detected auth method: {result.detected_auth_method}")

    # Print warnings
    for warning in result.warnings:
        click.echo(f"⚠️  Warning: {warning}", err=True)

    return result.errors


def _validate_connection_options(
    source_name: str, options_dict: dict, required_params: Set[str], optional_params: Set[str]
) -> List[str]:
    """Legacy validation function. Returns list of error messages."""
    result = validate_connection_options_legacy(
        source_name, options_dict, required_params, optional_params
    )

    # Print warnings
    for warning in result.warnings:
        click.echo(f"⚠️  Warning: {warning}", err=True)

    return result.errors


def _prepare_connection_options(
    source_name: str, options: str, spec_path: Optional[str], debug: bool
) -> dict:
    """Parse, validate, and enrich connection options. Raises ClickException on failure."""
    # Parse options JSON
    try:
        options_dict = json.loads(options)
    except json.JSONDecodeError as e:
        raise click.ClickException(f"Invalid JSON for --options: {e}")

    if not isinstance(options_dict, dict):
        raise click.ClickException("--options must be a JSON object (key-value pairs)")

    # Get constant allowlist and load spec
    constant_allowlist = _get_constant_external_options_allowlist()
    connector_spec = _load_connector_spec(source_name, spec_path)

    if connector_spec:
        parsed_spec = _parse_connector_spec(connector_spec)
        _debug_print_spec(parsed_spec, constant_allowlist, debug)

        # Validate connection options
        errors = _validate_connection_options_with_spec(source_name, options_dict, parsed_spec)
        if errors:
            raise click.ClickException("\n".join(errors))

        # Auto-add externalOptionsAllowList
        _add_external_options_allowlist(
            options_dict, parsed_spec.external_options_allowlist, constant_allowlist
        )
    else:
        click.echo(
            f"⚠️  Warning: Could not load connector spec for '{source_name}'. "
            "Skipping parameter validation.",
            err=True,
        )
        if "externalOptionsAllowList" not in options_dict and constant_allowlist:
            options_dict["externalOptionsAllowList"] = constant_allowlist
            click.echo(f"  ✓ Auto-added constant externalOptionsAllowList: {constant_allowlist}")

    options_dict["sourceName"] = source_name

    if debug:
        click.echo(f"[DEBUG] Options (with source_name): {options_dict}")

    return options_dict


def _debug_print_spec(
    parsed_spec: ParsedConnectorSpec, constant_allowlist: str, debug: bool
) -> None:
    """Print debug information about the connector spec."""
    if not debug:
        return
    if parsed_spec.has_auth_methods():
        click.echo(f"[DEBUG] Auth methods: {[m.name for m in parsed_spec.auth_methods]}")
        click.echo(f"[DEBUG] Common required: {parsed_spec.common_required_params}")
        click.echo(f"[DEBUG] Common optional: {parsed_spec.common_optional_params}")
    else:
        click.echo(f"[DEBUG] Required params: {parsed_spec.required_params}")
        click.echo(f"[DEBUG] Optional params: {parsed_spec.optional_params}")
    click.echo(f"[DEBUG] Source allowlist: {parsed_spec.external_options_allowlist}")
    click.echo(f"[DEBUG] Constant allowlist: {constant_allowlist}")


def _add_external_options_allowlist(
    options_dict: dict, source_allowlist: str, constant_allowlist: str
) -> None:
    """Add external options allowlist to options if not already present."""
    if "externalOptionsAllowList" not in options_dict:
        merged_allowlist = _merge_external_options_allowlist(source_allowlist, constant_allowlist)
        options_dict["externalOptionsAllowList"] = merged_allowlist
        if merged_allowlist:
            click.echo(f"  ✓ Auto-added externalOptionsAllowList: {merged_allowlist}")
        else:
            click.echo("  ✓ Set externalOptionsAllowList to empty (no table-specific options)")


def _handle_api_error(e: Exception, operation: str, debug: bool) -> None:
    """Handle API errors with detailed output."""
    error_msg = str(e)
    if hasattr(e, "message"):
        error_msg = e.message
    if hasattr(e, "error_code"):
        error_msg = f"[{e.error_code}] {error_msg}"
    if debug:
        click.echo(f"\n[DEBUG] Full exception: {traceback.format_exc()}", err=True)
    raise click.ClickException(f"Failed to {operation} connection: {error_msg}")


class OrderedGroup(click.Group):  # pylint: disable=too-few-public-methods
    """Custom Click group that preserves command order as defined in code."""

    def list_commands(self, ctx):
        """Return commands in the order they were added, not alphabetically."""
        return list(self.commands.keys())


def _parse_pipeline_spec(spec_input: str, validate: bool = True) -> dict:
    """Parse pipeline spec from JSON string or YAML/JSON file."""
    # Check if it's a file path
    if spec_input.endswith(('.yaml', '.yml', '.json')):
        try:
            with open(spec_input, 'r') as f:
                if spec_input.endswith('.json'):
                    spec = json.load(f)
                else:
                    spec = yaml.safe_load(f)
        except FileNotFoundError:
            raise click.ClickException(f"Pipeline spec file not found: {spec_input}")
        except Exception as e:
            raise click.ClickException(f"Failed to parse pipeline spec file: {e}")
    else:
        # Try to parse as JSON string
        try:
            spec = json.loads(spec_input)
        except json.JSONDecodeError as e:
            raise click.ClickException(f"Invalid JSON for --pipeline-spec: {e}")

    # Validate the spec (connection_name is always required in spec)
    if validate:
        try:
            warnings = validate_pipeline_spec(spec)
            for warning in warnings:
                click.echo(f"⚠️  Warning: {warning}", err=True)
        except PipelineSpecValidationError as e:
            raise click.ClickException(str(e))

    return spec


def _find_pipeline_by_name(workspace_client, pipeline_name: str) -> str:
    """Find a pipeline by name and return its ID."""
    filter_str = f"name LIKE '{pipeline_name}'"
    pipelines = list(workspace_client.pipelines.list_pipelines(filter=filter_str))

    if not pipelines:
        raise click.ClickException(f"Pipeline '{pipeline_name}' not found")

    if len(pipelines) > 1:
        click.echo(
            f"Warning: Found {len(pipelines)} pipelines matching "
            f"'{pipeline_name}', using first match"
        )

    return pipelines[0].pipeline_id


def _load_ingest_template(template_name: str = "ingest_template.py") -> str:
    """Load an ingest template from bundled templates."""
    template_path = Path(__file__).parent / "templates" / template_name
    with open(template_path, "r") as f:
        return f.read()


def _create_workspace_file(workspace_client, path: str, content: str) -> None:
    """Create a file in the Databricks workspace."""
    # Import the file to workspace using base64 encoding
    content_bytes = content.encode("utf-8")
    content_base64 = base64.b64encode(content_bytes).decode("utf-8")

    workspace_client.workspace.import_(
        path=path,
        content=content_base64,
        format=ImportFormat.SOURCE,
        language=Language.PYTHON,
        overwrite=True,
    )


def _delete_workspace_files(
    workspace_client, base_path: str, files: list, debug: bool = False
) -> None:
    """
    Delete files from the Databricks workspace.

    Args:
        workspace_client: The WorkspaceClient instance.
        base_path: Base workspace path (repo root).
        files: List of file names to delete.
        debug: Whether to print debug output.
    """
    for file_name in files:
        file_path = f"{base_path}/{file_name}"
        try:
            workspace_client.workspace.delete(path=file_path)
            if debug:
                click.echo(f"    [DEBUG] Deleted: {file_path}")
        except Exception as e:
            # RESOURCE_DOES_NOT_EXIST is fine - file doesn't exist
            if "RESOURCE_DOES_NOT_EXIST" in str(e) or "does not exist" in str(e).lower():
                if debug:
                    click.echo(f"    [DEBUG] File not found (skipped): {file_path}")
            else:
                # Log warning but don't fail the process
                click.echo(f"    Warning: Could not delete {file_path}: {e}")


def _replace_placeholder_in_value(value, placeholder: str, replacement: str):
    """
    Recursively replace a placeholder in a value (dict, list, or string).

    Args:
        value: Value to process (dict, list, or string).
        placeholder: Placeholder string to replace (e.g., "{WORKSPACE_PATH}").
        replacement: Replacement string.

    Returns:
        Value with placeholder replaced.
    """
    if isinstance(value, dict):
        return {
            k: _replace_placeholder_in_value(v, placeholder, replacement) for k, v in value.items()
        }
    elif isinstance(value, list):
        return [_replace_placeholder_in_value(item, placeholder, replacement) for item in value]
    elif isinstance(value, str):
        return value.replace(placeholder, replacement)
    else:
        return value


def _resolve_workspace_paths(
    workspace_path: str, repo_config, pipeline_config, current_user_name: str
):
    """
    Resolve placeholders in workspace paths and config objects.

    Args:
        workspace_path: The workspace path with potential {CURRENT_USER} placeholder.
        repo_config: The repo configuration object.
        pipeline_config: The pipeline configuration object.
        current_user_name: The current user's name.

    Returns:
        Resolved workspace_path string.
    """
    # Replace {CURRENT_USER} in workspace_path
    if workspace_path and "{CURRENT_USER}" in workspace_path:
        workspace_path = workspace_path.replace("{CURRENT_USER}", current_user_name)

    # Replace {WORKSPACE_PATH} in repo.path
    if repo_config.path:
        repo_config.path = repo_config.path.replace("{WORKSPACE_PATH}", workspace_path)

    # Replace {WORKSPACE_PATH} in pipeline.root_path
    if pipeline_config.root_path:
        pipeline_config.root_path = pipeline_config.root_path.replace(
            "{WORKSPACE_PATH}", workspace_path
        )

    # Replace {WORKSPACE_PATH} in libraries
    if pipeline_config.libraries:
        pipeline_config.libraries = _replace_placeholder_in_value(
            pipeline_config.libraries, "{WORKSPACE_PATH}", workspace_path
        )

    return workspace_path


def _ensure_parent_directory(workspace_client, workspace_path: str) -> None:
    """
    Ensure the parent workspace directory exists.

    Args:
        workspace_client: The WorkspaceClient instance.
        workspace_path: The full workspace path.

    Raises:
        click.ClickException: If directory creation fails.
    """
    parent_path = "/".join(workspace_path.rstrip("/").split("/")[:-1])
    if parent_path:
        click.echo(f"\nEnsuring workspace directory exists: {parent_path}")
        try:
            workspace_client.workspace.mkdirs(parent_path)
            click.echo("  ✓ Directory ready")
        except Exception as e:
            if "RESOURCE_ALREADY_EXISTS" in str(e):
                click.echo("  ✓ Directory already exists")
            else:
                raise click.ClickException(f"Failed to create workspace directory: {e}")


def _create_repo_and_cleanup(workspace_client, repo_config, debug: bool) -> str:
    """
    Create the repo and clean up excluded files.

    Args:
        workspace_client: The WorkspaceClient instance.
        repo_config: The repo configuration object.
        debug: Whether to print debug output.

    Returns:
        The repo workspace path.

    Raises:
        click.ClickException: If repo creation fails.
    """
    click.echo("\nStep 1: Creating repo...")
    repo_client = RepoClient(workspace_client)

    try:
        repo_info = repo_client.create(repo_config)
        repo_workspace_path = repo_client.get_repo_path(repo_info)

        if not repo_workspace_path:
            repo_workspace_path = repo_config.path
            click.echo(f"  ✓ Repo created (using configured path: {repo_workspace_path})")
        else:
            click.echo(f"  ✓ Repo created at: {repo_workspace_path}")

        if debug:
            click.echo(f"  [DEBUG] Repo ID: {repo_info.id if repo_info else 'N/A'}")
    except Exception as e:
        raise click.ClickException(f"Failed to create repo: {e}")

    # Clean up excluded root files (cone mode includes all root files)
    if repo_config.exclude_root_files:
        click.echo("\n  Cleaning up excluded root files...")
        _delete_workspace_files(
            workspace_client,
            repo_workspace_path,
            repo_config.exclude_root_files,
            debug=debug,
        )
        click.echo(f"  ✓ Cleaned up {len(repo_config.exclude_root_files)} excluded files")

    return repo_workspace_path


# pylint: disable=too-many-arguments,too-many-positional-arguments
def _create_ingest_file(
    workspace_client,
    workspace_path: str,
    source_name: str,
    connection_name: Optional[str],
    pipeline_spec_input: Optional[str],
    debug: bool,
) -> None:
    """
    Create the ingest.py file in the workspace.

    Args:
        workspace_client: The WorkspaceClient instance.
        workspace_path: The workspace path where ingest.py will be created.
        source_name: The connector source name.
        connection_name: The connection name (optional if pipeline_spec_input provided).
        pipeline_spec_input: The pipeline spec input (optional).
        debug: Whether to print debug output.

    Raises:
        click.ClickException: If file creation fails.
    """
    click.echo("\nStep 2: Creating ingest.py...")
    ingest_path = f"{workspace_path}/src/ingest.py"
    try:
        if pipeline_spec_input:
            pipeline_spec = _parse_pipeline_spec(pipeline_spec_input)
            if connection_name:
                pipeline_spec["connection_name"] = connection_name

            if debug:
                click.echo(f"  [DEBUG] Using provided pipeline spec: {pipeline_spec}")

            ingest_content = _load_ingest_template("ingest_template_base.py")
            ingest_content = ingest_content.replace("{SOURCE_NAME}", source_name)
            spec_json = json.dumps(pipeline_spec, indent=4)
            ingest_content = ingest_content.replace("{PIPELINE_SPEC}", spec_json)
        else:
            ingest_content = _load_ingest_template()
            ingest_content = ingest_content.replace("{SOURCE_NAME}", source_name)
            ingest_content = ingest_content.replace("{CONNECTION_NAME}", connection_name)

        _create_workspace_file(workspace_client, ingest_path, ingest_content)
        click.echo(f"  ✓ Created: {ingest_path}")
    except click.ClickException:
        raise
    except Exception as e:
        raise click.ClickException(f"Failed to create ingest.py: {e}")


def _create_and_show_pipeline(
    workspace_client,
    pipeline_config,
    repo_workspace_path: str,
    source_name: str,
    debug: bool,
) -> None:
    """
    Create the pipeline and display results.

    Args:
        workspace_client: The WorkspaceClient instance.
        pipeline_config: The pipeline configuration object.
        repo_workspace_path: The repo workspace path.
        source_name: The connector source name.
        debug: Whether to print debug output.

    Raises:
        click.ClickException: If pipeline creation fails.
    """
    click.echo(f"\nStep 3: Creating pipeline '{pipeline_config.name}'...")
    pipeline_client = PipelineClient(workspace_client)

    try:
        pipeline_response = pipeline_client.create(
            pipeline_config,
            repo_path=repo_workspace_path,
            source_name=source_name,
        )
        pipeline_id = pipeline_response.pipeline_id

        workspace_host = workspace_client.config.host
        if workspace_host and workspace_host.endswith("/"):
            workspace_host = workspace_host[:-1]
        pipeline_url = f"{workspace_host}/pipelines/{pipeline_id}"

        click.echo("  ✓ Pipeline created!")
        click.echo(f"\n{'=' * 60}")
        click.echo(f"Pipeline URL: {pipeline_url}")
        click.echo(f"Pipeline ID:  {pipeline_id}")
        click.echo(f"{'=' * 60}")

        if debug:
            click.echo(f"\n[DEBUG] Full pipeline response: {pipeline_response}")

    except Exception as e:
        raise click.ClickException(f"Failed to create pipeline: {e}")


@click.group(cls=OrderedGroup)
@click.option("--debug", is_flag=True, help="Enable debug output")
@click.pass_context
def main(ctx: click.Context, debug: bool):
    """
    Databricks Lakeflow Community Connector CLI.

    This tool helps you set up and run community connectors
    in your Databricks workspace.

    Configuration is loaded from default_config.yaml bundled with the package.
    You can override values using CLI options or a custom --config file.
    """
    ctx.ensure_object(dict)
    ctx.obj["debug"] = debug


@main.command("create_pipeline")
@click.argument("source_name")
@click.argument("pipeline_name")
@click.option(
    "--connection-name",
    "-n",
    help="Name of the UC connection to use for the connector "
    "(required if --pipeline-spec not provided)",
)
@click.option(
    "--pipeline-spec",
    "-ps",
    "pipeline_spec_input",
    help="Pipeline spec as JSON string or path to .yaml/.json file (must include connection_name)",
)
@click.option(
    "--repo-url",
    "-r",
    default=None,
    help="Git repository URL",
)
@click.option("--catalog", "-c", help="UC target catalog for the pipeline")
@click.option("--target", "-t", help="Target schema for the pipeline")
@click.option(
    "--config",
    "-f",
    "config_file",
    type=click.Path(exists=True),
    help="Path to custom config file (overrides defaults)",
)
@click.pass_context
# pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals
def create_pipeline(
    ctx: click.Context,
    source_name: str,
    pipeline_name: str,
    connection_name: Optional[str],
    pipeline_spec_input: Optional[str],
    config_file: Optional[str],
    repo_url: Optional[str],
    catalog: Optional[str],
    target: Optional[str],
):
    """
    Create a community connector pipeline.

    SOURCE_NAME is the name of the connector source (e.g., 'github', 'stripe', 'hubspot').

    PIPELINE_NAME is a unique name for this pipeline instance.

    This command creates a Git repo in your workspace and then creates
    a DLT pipeline for the specified connector source.

    Either --connection-name or --pipeline-spec must be provided.
    If using --pipeline-spec, it must include 'connection_name'.

    Configuration is loaded from bundled defaults and can be overridden
    with --config file or individual CLI options.

    \b
    Example:
        community-connector create_pipeline github my_github_pipeline -n my_conn
        community-connector create_pipeline stripe my_stripe -n stripe_conn -c main
        community-connector create_pipeline github my_pipeline -ps spec.yaml
        community-connector create_pipeline github my_pipeline -ps pipeline_spec.json
    """
    debug = ctx.obj.get("debug", False)

    # Validate: either connection_name or pipeline_spec_input must be provided
    if not connection_name and not pipeline_spec_input:
        raise click.ClickException(
            "Either --connection-name or --pipeline-spec must be provided"
        )

    # Build config with precedence: CLI args > config file > defaults
    workspace_path, repo_config, pipeline_config = build_config(
        source_name=source_name,
        pipeline_name=pipeline_name,
        repo_url=repo_url,
        catalog=catalog,
        target=target,
        config_file=config_file,
    )

    click.echo(f"Creating connector for source: {source_name}")
    click.echo(f"Pipeline name: {pipeline_name}")
    if connection_name:
        click.echo(f"Connection name: {connection_name}")
    elif pipeline_spec_input:
        click.echo("Connection name: (from pipeline spec)")
    click.echo(f"Using repo: {repo_config.url}")

    if debug:
        click.echo(f"[DEBUG] workspace_path (before resolution): {workspace_path}")
        click.echo(f"[DEBUG] Repo config: {repo_config}")
        click.echo(f"[DEBUG] Pipeline config: {pipeline_config}")

    # Create the workspace client
    workspace_client = WorkspaceClient()

    # Get current user from workspace and resolve paths
    current_user = workspace_client.current_user.me()
    workspace_path = _resolve_workspace_paths(
        workspace_path, repo_config, pipeline_config, current_user.user_name
    )

    if debug:
        click.echo(f"[DEBUG] Resolved workspace_path: {workspace_path}")
        click.echo(f"[DEBUG] Resolved repo.path: {repo_config.path}")
        click.echo(f"[DEBUG] Resolved root_path: {pipeline_config.root_path}")
        click.echo(f"[DEBUG] Resolved libraries: {pipeline_config.libraries}")

    # Ensure the parent workspace directory exists
    _ensure_parent_directory(workspace_client, workspace_path)

    # Step 1: Create the repo and clean up excluded files
    repo_workspace_path = _create_repo_and_cleanup(workspace_client, repo_config, debug)

    # Step 2: Create ingest.py in the workspace
    _create_ingest_file(
        workspace_client,
        workspace_path,
        source_name,
        connection_name,
        pipeline_spec_input,
        debug,
    )

    # Step 3: Create the pipeline
    _create_and_show_pipeline(
        workspace_client,
        pipeline_config,
        repo_workspace_path,
        source_name,
        debug,
    )


def _get_ingest_path_from_pipeline(pipeline_info) -> Optional[str]:
    """
    Extract the ingest.py path from the pipeline's library configuration or root_path.

    Args:
        pipeline_info: The GetPipelineResponse from the pipelines API.

    Returns:
        The workspace path to ingest.py, or None if not found.
    """
    # Try to get the path from spec
    if hasattr(pipeline_info, "spec") and pipeline_info.spec:
        spec = pipeline_info.spec

        # First, try to find ingest.py in the libraries
        if hasattr(spec, "libraries") and spec.libraries:
            for lib in spec.libraries:
                # Check file library
                if hasattr(lib, "file") and lib.file and hasattr(lib.file, "path"):
                    path = lib.file.path
                    if path and "ingest.py" in path:
                        return path
                # Check notebook library (less likely but possible)
                if hasattr(lib, "notebook") and lib.notebook and hasattr(lib.notebook, "path"):
                    path = lib.notebook.path
                    if path and "ingest" in path:
                        # For notebook, append .py extension assumption
                        return path + ".py" if not path.endswith(".py") else path

        # Fall back to root_path + /ingest.py
        if hasattr(spec, "root_path") and spec.root_path:
            return f"{spec.root_path}/ingest.py"

    return None


def _read_workspace_file(workspace_client, path: str) -> str:
    """
    Read a file from the Databricks workspace.

    Args:
        workspace_client: The WorkspaceClient instance.
        path: The workspace path to the file.

    Returns:
        The file contents as a string.

    Raises:
        click.ClickException: If the file cannot be read.
    """
    try:
        export_response = workspace_client.workspace.export(path=path)
        if export_response.content:
            content_bytes = base64.b64decode(export_response.content)
            return content_bytes.decode("utf-8")
        raise click.ClickException(f"File is empty: {path}")
    except Exception as e:
        if "RESOURCE_DOES_NOT_EXIST" in str(e) or "does not exist" in str(e).lower():
            raise click.ClickException(f"File not found: {path}")
        raise click.ClickException(f"Failed to read file {path}: {e}")


def _extract_source_name_from_ingest(content: str) -> Optional[str]:
    """
    Extract the source_name from an existing ingest.py file.

    Args:
        content: The content of the ingest.py file.

    Returns:
        The source_name value, or None if not found.
    """
    # Match: source_name = "github" or source_name = 'github'
    match = re.search(r'source_name\s*=\s*["\']([^"\']+)["\']', content)
    if match:
        return match.group(1)
    return None


def _generate_ingest_content(source_name: str, pipeline_spec: dict) -> str:
    """
    Generate the ingest.py content from a pipeline spec.

    Args:
        source_name: The connector source name.
        pipeline_spec: The parsed pipeline spec dictionary.

    Returns:
        The generated ingest.py content.
    """
    content = _load_ingest_template("ingest_template_base.py")
    content = content.replace("{SOURCE_NAME}", source_name)
    spec_json = json.dumps(pipeline_spec, indent=4)
    return content.replace("{PIPELINE_SPEC}", spec_json)


def _print_pipeline_success(workspace_client, pipeline_id: str) -> None:
    """Print success message with pipeline URL."""
    workspace_host = workspace_client.config.host
    if workspace_host and workspace_host.endswith("/"):
        workspace_host = workspace_host[:-1]
    pipeline_url = f"{workspace_host}/pipelines/{pipeline_id}"

    click.echo(f"\n{'=' * 60}")
    click.echo("Pipeline updated successfully!")
    click.echo(f"View pipeline: {pipeline_url}")
    click.echo(f"{'=' * 60}")
    click.echo("\nNote: Run the pipeline to apply the new configuration.")


@main.command("update_pipeline")
@click.argument("pipeline_name")
@click.option(
    "--pipeline-spec",
    "-ps",
    "pipeline_spec_input",
    required=True,
    help="Pipeline spec as JSON string or path to .yaml/.json file (must include connection_name)",
)
@click.pass_context
def update_pipeline(ctx: click.Context, pipeline_name: str, pipeline_spec_input: str):
    """
    Update the ingest.py for an existing community connector pipeline.

    PIPELINE_NAME is the name of the pipeline to update.

    This command updates the ingest.py file in the workspace with the new
    pipeline spec. Only the ingest.py file is modified; other pipeline
    settings remain unchanged.

    \b
    Example:
        community-connector update_pipeline my_pipeline -ps spec.yaml
        community-connector update_pipeline my_pipeline \\
            -ps '{"connection_name": "conn", "objects": []}'
    """
    debug = ctx.obj.get("debug", False)

    workspace_client = WorkspaceClient()
    pipeline_client = PipelineClient(workspace_client)

    try:
        # Step 1: Find pipeline by name
        click.echo(f"Finding pipeline: {pipeline_name}")
        pipeline_id = _find_pipeline_by_name(workspace_client, pipeline_name)
        click.echo(f"  ✓ Found pipeline ID: {pipeline_id}")

        # Step 2: Get pipeline info to find ingest.py path
        pipeline_info = pipeline_client.get(pipeline_id)

        if debug:
            click.echo(f"[DEBUG] Pipeline spec: {pipeline_info.spec}")

        ingest_path = _get_ingest_path_from_pipeline(pipeline_info)
        if not ingest_path:
            raise click.ClickException(
                "Could not determine ingest.py path from pipeline configuration. "
                "Please ensure the pipeline was created with community-connector CLI."
            )

        click.echo(f"  ✓ Found ingest.py at: {ingest_path}")

        # Step 3: Read existing ingest.py to get source_name
        click.echo("\nReading existing ingest.py...")
        existing_content = _read_workspace_file(workspace_client, ingest_path)
        source_name = _extract_source_name_from_ingest(existing_content)

        if not source_name:
            raise click.ClickException(
                "Could not extract source_name from existing ingest.py. "
                "Please ensure the file was created with community-connector CLI."
            )

        click.echo(f"  ✓ Detected source: {source_name}")

        # Step 4: Parse and validate the new pipeline spec
        click.echo("\nValidating pipeline spec...")
        pipeline_spec = _parse_pipeline_spec(pipeline_spec_input)
        click.echo("  ✓ Pipeline spec is valid")

        if debug:
            click.echo(f"[DEBUG] New pipeline spec: {pipeline_spec}")

        # Step 5: Generate and write new ingest.py content
        click.echo("\nUpdating ingest.py...")
        ingest_content = _generate_ingest_content(source_name, pipeline_spec)
        _create_workspace_file(workspace_client, ingest_path, ingest_content)
        click.echo(f"  ✓ Updated: {ingest_path}")

        # Step 6: Print success message
        _print_pipeline_success(workspace_client, pipeline_id)

    except click.ClickException:
        raise
    except Exception as e:
        if debug:
            click.echo(f"\n[DEBUG] Full exception: {traceback.format_exc()}", err=True)
        raise click.ClickException(f"Failed to update pipeline: {e}")


@main.command("run_pipeline")
@click.argument("pipeline_name")
@click.option("--full-refresh", is_flag=True, help="Run a full refresh instead of incremental")
@click.pass_context
def run_pipeline(ctx: click.Context, pipeline_name: str, full_refresh: bool):
    """
    Run a community connector pipeline.

    PIPELINE_NAME is the name of the pipeline to run.

    \b
    Example:
        community-connector run_pipeline my_github_pipeline
        community-connector run_pipeline my_github_pipeline --full-refresh
    """
    debug = ctx.obj.get("debug", False)

    workspace_client = WorkspaceClient()
    pipeline_client = PipelineClient(workspace_client)

    try:
        # Find pipeline by name
        pipeline_id = _find_pipeline_by_name(workspace_client, pipeline_name)

        click.echo(f"Starting pipeline: {pipeline_name} (ID: {pipeline_id})")

        update_info = pipeline_client.start(pipeline_id, full_refresh=full_refresh)

        click.echo("  ✓ Pipeline run started!")

        if update_info and hasattr(update_info, "update_id"):
            click.echo(f"  Update ID: {update_info.update_id}")

        # Build the pipeline URL
        workspace_host = workspace_client.config.host
        if workspace_host and workspace_host.endswith("/"):
            workspace_host = workspace_host[:-1]
        pipeline_url = f"{workspace_host}/pipelines/{pipeline_id}"

        click.echo(f"\nView pipeline: {pipeline_url}")

        if debug and update_info:
            click.echo(f"\n[DEBUG] Update info: {update_info}")

    except click.ClickException:
        raise
    except Exception as e:
        raise click.ClickException(f"Failed to start pipeline: {e}")


@main.command("show_pipeline")
@click.argument("pipeline_name")
@click.pass_context
def show_pipeline(ctx: click.Context, pipeline_name: str):
    """
    Show status of a community connector pipeline.

    PIPELINE_NAME is the name of the pipeline to check.

    \b
    Example:
        community-connector show_pipeline my_github_pipeline
    """
    debug = ctx.obj.get("debug", False)

    workspace_client = WorkspaceClient()
    pipeline_client = PipelineClient(workspace_client)

    try:
        # Find pipeline by name
        pipeline_id = _find_pipeline_by_name(workspace_client, pipeline_name)
        pipeline_info = pipeline_client.get(pipeline_id)

        click.echo("Pipeline Status")
        click.echo(f"{'=' * 40}")
        click.echo(f"  Name:   {pipeline_info.name}")
        click.echo(f"  ID:     {pipeline_info.pipeline_id}")
        click.echo(f"  State:  {pipeline_info.state}")

        # Show latest update info if available
        if hasattr(pipeline_info, "latest_updates") and pipeline_info.latest_updates:
            latest = pipeline_info.latest_updates[0]
            click.echo("\nLatest Update:")
            click.echo(f"  Update ID:   {latest.update_id}")
            click.echo(f"  State:       {latest.state}")
            if hasattr(latest, "creation_time") and latest.creation_time:
                click.echo(f"  Started:     {latest.creation_time}")

        # Build the pipeline URL
        workspace_host = workspace_client.config.host
        if workspace_host and workspace_host.endswith("/"):
            workspace_host = workspace_host[:-1]
        pipeline_url = f"{workspace_host}/pipelines/{pipeline_id}"

        click.echo(f"\nView pipeline: {pipeline_url}")

        if debug:
            click.echo(f"\n[DEBUG] Full pipeline info: {pipeline_info}")

    except click.ClickException:
        raise
    except Exception as e:
        raise click.ClickException(f"Failed to get pipeline status: {e}")


@main.command("create_connection")
@click.argument("source_name")
@click.argument("connection_name")
@click.option(
    "--options",
    "-o",
    required=True,
    help='Connection options as JSON string (e.g., \'{"key": "value"}\')',
)
@click.option(
    "--spec",
    "-s",
    "spec_path",
    default=None,
    help="Optional: local path to connector_spec.yaml, or a GitHub repo URL "
    "(e.g., https://github.com/myorg/myrepo). "
    "If a URL, the spec is fetched from sources/{source_name}/connector_spec.yaml in that repo.",
)
@click.pass_context
def create_connection(
    ctx: click.Context,
    source_name: str,
    connection_name: str,
    options: str,
    spec_path: Optional[str],
):
    """
    Create a UC connection for community connectors.

    SOURCE_NAME is the name of the connector source (e.g., 'github', 'stripe', 'hubspot').

    CONNECTION_NAME is the name for the new connection.

    The connection type is set to GENERIC_LAKEFLOW_CONNECT.

    Connection options are validated against the connector spec (connector_spec.yaml).
    The externalOptionsAllowList is automatically added from the spec.

    \b
    Example:
        community-connector create_connection github my_github_conn \\
            -o '{"token": "ghp_xxxx"}'

        # With custom spec file:
        community-connector create_connection github my_github_conn \\
            -o '{"token": "ghp_xxxx"}' --spec ./my_connector_spec.yaml

        # With custom GitHub repo (fetches from sources/github/connector_spec.yaml in that repo):
        community-connector create_connection github my_github_conn \\
            -o '{"token": "ghp_xxxx"}' --spec https://github.com/myorg/myrepo
    """
    debug = ctx.obj.get("debug", False)

    click.echo(f"Creating connection for source: {source_name}")
    click.echo(f"Connection name: {connection_name}")
    click.echo("Connection type: GENERIC_LAKEFLOW_CONNECT")

    options_dict = _prepare_connection_options(source_name, options, spec_path, debug)

    workspace_client = WorkspaceClient()
    body = {
        "name": connection_name,
        "connection_type": "GENERIC_LAKEFLOW_CONNECT",
        "options": options_dict,
        "comment": "created by lakeflow community-connector CLI tool",
    }

    if debug:
        click.echo(f"[DEBUG] API request body: {body}")

    try:
        connection_info = workspace_client.api_client.do(
            "POST", "/api/2.1/unity-catalog/connections", body=body
        )
        click.echo("  ✓ Connection created!")
        click.echo(f"\n{'=' * 60}")
        click.echo(f"Connection Name: {connection_info.get('name', connection_name)}")
        click.echo(f"Connection ID:   {connection_info.get('connection_id', 'N/A')}")
        click.echo(f"{'=' * 60}")
        if debug:
            click.echo(f"\n[DEBUG] Full connection info: {connection_info}")
    except Exception as e:
        _handle_api_error(e, "create", debug)


@main.command("update_connection")
@click.argument("source_name")
@click.argument("connection_name")
@click.option(
    "--options",
    "-o",
    required=True,
    help='Connection options as JSON string (e.g., \'{"key": "value"}\')',
)
@click.option(
    "--spec",
    "-s",
    "spec_path",
    default=None,
    help="Optional: local path to connector_spec.yaml, or a GitHub repo URL "
    "(e.g., https://github.com/myorg/myrepo). "
    "If a URL, the spec is fetched from sources/{source_name}/connector_spec.yaml in that repo.",
)
@click.pass_context
def update_connection(
    ctx: click.Context,
    source_name: str,
    connection_name: str,
    options: str,
    spec_path: Optional[str],
):
    """
    Update a UC connection for community connectors.

    SOURCE_NAME is the name of the connector source (e.g., 'github', 'stripe', 'hubspot').

    CONNECTION_NAME is the name of the existing connection to update.

    The connection type is set to GENERIC_LAKEFLOW_CONNECT.

    Connection options are validated against the connector spec (connector_spec.yaml).
    The externalOptionsAllowList is automatically added from the spec.

    \b
    Example:
        community-connector update_connection github my_github_conn \\
            -o '{"token": "ghp_xxxx"}'

        # With custom spec file:
        community-connector update_connection github my_github_conn \\
            -o '{"token": "ghp_xxxx"}' --spec ./my_connector_spec.yaml

        # With custom GitHub repo (fetches from sources/github/connector_spec.yaml in that repo):
        community-connector update_connection github my_github_conn \\
            -o '{"token": "ghp_xxxx"}' --spec https://github.com/myorg/myrepo
    """
    debug = ctx.obj.get("debug", False)

    click.echo(f"Updating connection for source: {source_name}")
    click.echo(f"Connection name: {connection_name}")

    options_dict = _prepare_connection_options(source_name, options, spec_path, debug)

    workspace_client = WorkspaceClient()
    body = {"name": connection_name, "options": options_dict}

    if debug:
        click.echo(f"[DEBUG] API request body: {body}")

    try:
        connection_info = workspace_client.api_client.do(
            "PATCH", f"/api/2.1/unity-catalog/connections/{connection_name}", body=body
        )
        click.echo("  ✓ Connection updated!")
        click.echo(f"\n{'=' * 60}")
        click.echo(f"Connection Name: {connection_info.get('name', connection_name)}")
        click.echo(f"Connection ID:   {connection_info.get('connection_id', 'N/A')}")
        click.echo(f"{'=' * 60}")
        if debug:
            click.echo(f"\n[DEBUG] Full connection info: {connection_info}")
    except Exception as e:
        _handle_api_error(e, "update", debug)


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
