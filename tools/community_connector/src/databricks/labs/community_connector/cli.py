"""
Command-line interface for the Community Connector tool.

This module provides the CLI commands for setting up and running
Databricks Lakeflow community connectors.

Configuration Precedence:
    CLI arguments → --config file → default_config.yaml → code defaults
"""

import click
from typing import Optional

from databricks.sdk import WorkspaceClient

from databricks.labs.community_connector import __version__
from databricks.labs.community_connector.config import build_config
from databricks.labs.community_connector.repo_client import RepoClient
from databricks.labs.community_connector.pipeline_client import PipelineClient

from pathlib import Path
import base64


class OrderedGroup(click.Group):
    """Custom Click group that preserves command order as defined in code."""

    def list_commands(self, ctx):
        """Return commands in the order they were added, not alphabetically."""
        return list(self.commands.keys())


def _parse_pipeline_spec(spec_input: str, validate: bool = True) -> dict:
    """
    Parse pipeline spec from JSON string, YAML file, or JSON file.

    Args:
        spec_input: JSON string, or path to .yaml/.yml/.json file.
        validate: Whether to validate the parsed spec.

    Returns:
        Parsed dictionary containing the pipeline spec.

    Raises:
        click.ClickException: If parsing or validation fails.
    """
    import json
    import yaml
    from databricks.labs.community_connector.pipeline_spec_validator import (
        validate_pipeline_spec,
        PipelineSpecValidationError,
    )

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
    """
    Find a pipeline by name and return its ID.

    Args:
        workspace_client: The WorkspaceClient instance.
        pipeline_name: Name of the pipeline to find.

    Returns:
        The pipeline ID.

    Raises:
        click.ClickException: If the pipeline is not found.
    """
    filter_str = f"name LIKE '{pipeline_name}'"
    pipelines = list(workspace_client.pipelines.list_pipelines(filter=filter_str))

    if not pipelines:
        raise click.ClickException(f"Pipeline '{pipeline_name}' not found")

    if len(pipelines) > 1:
        click.echo(f"Warning: Found {len(pipelines)} pipelines matching '{pipeline_name}', using first match")

    return pipelines[0].pipeline_id


def _load_ingest_template(template_name: str = "ingest_template.py") -> str:
    """
    Load an ingest template from the bundled templates.

    Args:
        template_name: Name of the template file to load.
            - "ingest_template.py": Full template with examples (default)
            - "ingest_template_base.py": Base template for --pipeline-spec

    Returns:
        Content of the template file.
    """
    template_path = Path(__file__).parent / "templates" / template_name
    with open(template_path, "r") as f:
        return f.read()


def _create_workspace_file(workspace_client, path: str, content: str) -> None:
    """
    Create a file in the Databricks workspace.

    Args:
        workspace_client: The WorkspaceClient instance.
        path: Workspace path where the file will be created.
        content: Content of the file.
    """
    from databricks.sdk.service.workspace import ImportFormat, Language

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


def _delete_workspace_files(workspace_client, base_path: str, files: list, debug: bool = False) -> None:
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
        return {k: _replace_placeholder_in_value(v, placeholder, replacement) for k, v in value.items()}
    elif isinstance(value, list):
        return [_replace_placeholder_in_value(item, placeholder, replacement) for item in value]
    elif isinstance(value, str):
        return value.replace(placeholder, replacement)
    else:
        return value


@click.group(cls=OrderedGroup)
@click.version_option(version=__version__, prog_name="community-connector")
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
    help="Name of the UC connection to use for the connector (required if --pipeline-spec not provided)",
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
        community-connector create_pipeline github my_github_pipeline -n my_github_conn
        community-connector create_pipeline stripe my_stripe_prod -n stripe_conn --catalog main --target my_schema
        community-connector create_pipeline github my_pipeline -ps spec.yaml
        community-connector create_pipeline github my_pipeline -ps '{"connection_name": "my_conn", "objects": [{"table": {"source_table": "users"}}]}'
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

    # Get current user from workspace
    current_user = workspace_client.current_user.me()
    current_user_name = current_user.user_name

    # Step 1: Replace {CURRENT_USER} in workspace_path
    if workspace_path and "{CURRENT_USER}" in workspace_path:
        workspace_path = workspace_path.replace("{CURRENT_USER}", current_user_name)

    # Step 2: Replace {WORKSPACE_PATH} in repo.path
    if repo_config.path:
        repo_config.path = repo_config.path.replace("{WORKSPACE_PATH}", workspace_path)

    # Step 3: Replace {WORKSPACE_PATH} in pipeline.root_path
    if pipeline_config.root_path:
        pipeline_config.root_path = pipeline_config.root_path.replace("{WORKSPACE_PATH}", workspace_path)

    # Step 4: Replace {WORKSPACE_PATH} in libraries
    if pipeline_config.libraries:
        pipeline_config.libraries = _replace_placeholder_in_value(
            pipeline_config.libraries, "{WORKSPACE_PATH}", workspace_path
        )

    if debug:
        click.echo(f"[DEBUG] Resolved workspace_path: {workspace_path}")
        click.echo(f"[DEBUG] Resolved repo.path: {repo_config.path}")
        click.echo(f"[DEBUG] Resolved root_path: {pipeline_config.root_path}")
        click.echo(f"[DEBUG] Resolved libraries: {pipeline_config.libraries}")

    # Get workspace host for building pipeline URL
    workspace_host = workspace_client.config.host
    if workspace_host and workspace_host.endswith("/"):
        workspace_host = workspace_host[:-1]

    # Ensure the parent workspace directory exists
    # Extract parent path (e.g., /Users/user/.lakeflow_community_connectors)
    parent_path = "/".join(workspace_path.rstrip("/").split("/")[:-1])
    if parent_path:
        click.echo(f"\nEnsuring workspace directory exists: {parent_path}")
        try:
            workspace_client.workspace.mkdirs(parent_path)
            click.echo("  ✓ Directory ready")
        except Exception as e:
            # RESOURCE_ALREADY_EXISTS is fine - directory already exists
            if "RESOURCE_ALREADY_EXISTS" in str(e):
                click.echo("  ✓ Directory already exists")
            else:
                raise click.ClickException(f"Failed to create workspace directory: {e}")

    # Step 1: Create the repo
    click.echo("\nStep 1: Creating repo...")
    repo_client = RepoClient(workspace_client)

    try:
        repo_info = repo_client.create(repo_config)
        repo_workspace_path = repo_client.get_repo_path(repo_info)

        if not repo_workspace_path:
            # Fallback to the configured path if API doesn't return it
            repo_workspace_path = repo_config.path
            click.echo(f"  ✓ Repo created (using configured path: {repo_workspace_path})")
        else:
            click.echo(f"  ✓ Repo created at: {repo_workspace_path}")

        if debug:
            click.echo(f"  [DEBUG] Repo ID: {repo_info.id if repo_info else 'N/A'}")
    except Exception as e:
        raise click.ClickException(f"Failed to create repo: {e}")

    # Step 1b: Clean up excluded root files (cone mode includes all root files)
    if repo_config.exclude_root_files:
        click.echo("\n  Cleaning up excluded root files...")
        _delete_workspace_files(
            workspace_client,
            repo_workspace_path,
            repo_config.exclude_root_files,
            debug=debug,
        )
        click.echo(f"  ✓ Cleaned up {len(repo_config.exclude_root_files)} excluded files")

    # Step 2: Create ingest.py in the workspace
    click.echo("\nStep 2: Creating ingest.py...")
    ingest_path = f"{workspace_path}/ingest.py"
    try:
        if pipeline_spec_input:
            # Parse the provided pipeline spec (connection_name is always required in spec)
            import json
            pipeline_spec = _parse_pipeline_spec(pipeline_spec_input)

            # If CLI connection_name provided, it overrides the spec
            if connection_name:
                pipeline_spec["connection_name"] = connection_name

            if debug:
                click.echo(f"  [DEBUG] Using provided pipeline spec: {pipeline_spec}")

            # Use the base template with pipeline_spec placeholder
            ingest_content = _load_ingest_template("ingest_template_base.py")
            ingest_content = ingest_content.replace("{SOURCE_NAME}", source_name)
            ingest_content = ingest_content.replace("{PIPELINE_SPEC}", json.dumps(pipeline_spec, indent=4))
        else:
            # Use the full template with placeholders replaced
            # connection_name is required here (already validated above)
            ingest_content = _load_ingest_template()
            ingest_content = ingest_content.replace("{SOURCE_NAME}", source_name)
            ingest_content = ingest_content.replace("{CONNECTION_NAME}", connection_name)

        _create_workspace_file(workspace_client, ingest_path, ingest_content)
        click.echo(f"  ✓ Created: {ingest_path}")
    except click.ClickException:
        raise
    except Exception as e:
        raise click.ClickException(f"Failed to create ingest.py: {e}")

    # Step 3: Create the pipeline
    click.echo(f"\nStep 3: Creating pipeline '{pipeline_config.name}'...")
    pipeline_client = PipelineClient(workspace_client)

    try:
        pipeline_response = pipeline_client.create(
            pipeline_config,
            repo_path=repo_workspace_path,
            source_name=source_name,
        )
        pipeline_id = pipeline_response.pipeline_id

        # Build the pipeline URL
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
@click.pass_context
def create_connection(ctx: click.Context, source_name: str, connection_name: str, options: str):
    """
    Create a UC connection for community connectors.

    SOURCE_NAME is the name of the connector source (e.g., 'github', 'stripe', 'hubspot').

    CONNECTION_NAME is the name for the new connection.

    The connection type is set to GENERIC_LAKEFLOW_CONNECT.

    \b
    Example:
        community-connector create_connection github my_github_conn -o '{"host": "api.github.com", "externalOptionsAllowList": "repo,owner,startDate"}'
    """
    import json

    debug = ctx.obj.get("debug", False)

    # Parse options JSON
    try:
        options_dict = json.loads(options)
    except json.JSONDecodeError as e:
        raise click.ClickException(f"Invalid JSON for --options: {e}")

    if not isinstance(options_dict, dict):
        raise click.ClickException("--options must be a JSON object (key-value pairs)")

    # Merge source_name into options (API expects camelCase: sourceName)
    options_dict["sourceName"] = source_name

    # Warn if externalOptionsAllowList is not provided
    if "externalOptionsAllowList" not in options_dict:
        click.echo(
            "⚠️  Warning: 'externalOptionsAllowList' is not specified in the options. "
            "This field is usually required to specify additional options needed for ingestion configuration in 'table_configuration'. "
            "Please refer to the source connector documentation for the complete list of allowed options.",
            err=True,
        )

    click.echo(f"Creating connection for source: {source_name}")
    click.echo(f"Connection name: {connection_name}")
    click.echo(f"Connection type: GENERIC_LAKEFLOW_CONNECT")

    if debug:
        click.echo(f"[DEBUG] Options (with source_name): {options_dict}")

    workspace_client = WorkspaceClient()

    try:
        # Use raw API call since GENERIC_LAKEFLOW_CONNECT may not be in SDK's enum yet
        body = {
            "name": connection_name,
            "connection_type": "GENERIC_LAKEFLOW_CONNECT",
            "options": options_dict,
            "comment": "created by lakeflow community-connector CLI tool",
        }

        if debug:
            click.echo(f"[DEBUG] API request body: {body}")

        connection_info = workspace_client.api_client.do(
            "POST",
            "/api/2.1/unity-catalog/connections",
            body=body,
        )

        click.echo("  ✓ Connection created!")
        click.echo(f"\n{'=' * 60}")
        click.echo(f"Connection Name: {connection_info.get('name', connection_name)}")
        click.echo(f"Connection ID:   {connection_info.get('connection_id', 'N/A')}")
        click.echo(f"{'=' * 60}")

        if debug:
            click.echo(f"\n[DEBUG] Full connection info: {connection_info}")

    except Exception as e:
        # Show more detailed error information
        error_msg = str(e)
        if hasattr(e, 'message'):
            error_msg = e.message
        if hasattr(e, 'error_code'):
            error_msg = f"[{e.error_code}] {error_msg}"
        if debug:
            import traceback
            click.echo(f"\n[DEBUG] Full exception: {traceback.format_exc()}", err=True)
        raise click.ClickException(f"Failed to create connection: {error_msg}")


@main.command("update_connection")
@click.argument("source_name")
@click.argument("connection_name")
@click.option(
    "--options",
    "-o",
    required=True,
    help='Connection options as JSON string (e.g., \'{"key": "value"}\')',
)
@click.pass_context
def update_connection(ctx: click.Context, source_name: str, connection_name: str, options: str):
    """
    Update a UC connection for community connectors.

    SOURCE_NAME is the name of the connector source (e.g., 'github', 'stripe', 'hubspot').

    CONNECTION_NAME is the name of the existing connection to update.

    The connection type is set to GENERIC_LAKEFLOW_CONNECT.

    \b
    Example:
        community-connector update_connection github my_github_conn -o '{"host": "api.github.com", "externalOptionsAllowList": "repo,owner,startDate"}'
    """
    import json

    debug = ctx.obj.get("debug", False)

    # Parse options JSON
    try:
        options_dict = json.loads(options)
    except json.JSONDecodeError as e:
        raise click.ClickException(f"Invalid JSON for --options: {e}")

    if not isinstance(options_dict, dict):
        raise click.ClickException("--options must be a JSON object (key-value pairs)")

    # Merge source_name into options (API expects camelCase: sourceName)
    options_dict["sourceName"] = source_name

    # Warn if externalOptionsAllowList is not provided
    if "externalOptionsAllowList" not in options_dict:
        click.echo(
            "⚠️  Warning: 'externalOptionsAllowList' is not specified in the options. "
            "This field is usually required to specify additional options needed for ingestion configuration in 'table_configuration'. "
            "Please refer to the source connector documentation for the complete list of allowed options.",
            err=True,
        )

    click.echo(f"Updating connection for source: {source_name}")
    click.echo(f"Connection name: {connection_name}")

    if debug:
        click.echo(f"[DEBUG] Options (with source_name): {options_dict}")

    workspace_client = WorkspaceClient()

    try:
        # Use raw API call - note: connection_type cannot be updated
        body = {
            "name": connection_name,
            "options": options_dict,
        }

        if debug:
            click.echo(f"[DEBUG] API request body: {body}")

        connection_info = workspace_client.api_client.do(
            "PATCH",
            f"/api/2.1/unity-catalog/connections/{connection_name}",
            body=body,
        )

        click.echo("  ✓ Connection updated!")
        click.echo(f"\n{'=' * 60}")
        click.echo(f"Connection Name: {connection_info.get('name', connection_name)}")
        click.echo(f"Connection ID:   {connection_info.get('connection_id', 'N/A')}")
        click.echo(f"{'=' * 60}")

        if debug:
            click.echo(f"\n[DEBUG] Full connection info: {connection_info}")

    except Exception as e:
        # Show more detailed error information
        error_msg = str(e)
        if hasattr(e, 'message'):
            error_msg = e.message
        if hasattr(e, 'error_code'):
            error_msg = f"[{e.error_code}] {error_msg}"
        if debug:
            import traceback
            click.echo(f"\n[DEBUG] Full exception: {traceback.format_exc()}", err=True)
        raise click.ClickException(f"Failed to update connection: {error_msg}")


if __name__ == "__main__":
    main()
