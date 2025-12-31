"""
Configuration management for the Community Connector CLI.

This module provides configuration classes and utilities for managing
connector setup parameters.

Configuration Precedence (highest to lowest):
    CLI arguments → user config file (--config) → default_config.yaml → code defaults
"""

from dataclasses import dataclass, field
from typing import Optional
import yaml
from pathlib import Path


@dataclass
class RepoConfig:
    """Configuration for creating a Databricks repo."""

    url: str
    """Git repository URL to clone."""

    provider: str = "gitHub"
    """Git provider (e.g., 'gitHub', 'gitLab', 'bitbucketCloud', 'azureDevOpsServices')."""

    path: Optional[str] = None
    """Workspace path where the repo will be created. If None, uses default location."""

    branch: Optional[str] = None
    """Git branch to checkout. If None, uses the default branch."""

    sparse_checkout: Optional[dict] = None
    """Sparse checkout configuration with 'patterns' list."""

    exclude_root_files: Optional[list] = None
    """List of root-level files to delete after checkout (cone mode includes all root files)."""


@dataclass
class PipelineConfig:
    """Configuration for creating a Databricks pipeline."""

    name: str
    """Name of the pipeline."""

    target: Optional[str] = None
    """Target schema/database for the pipeline output."""

    catalog: Optional[str] = None
    """Unity Catalog name for the pipeline."""

    root_path: Optional[str] = None
    """Root path for the pipeline in the workspace."""

    channel: Optional[str] = None
    """Release channel ('CURRENT' or 'PREVIEW')."""

    continuous: Optional[bool] = None
    """Whether the pipeline runs continuously."""

    development: Optional[bool] = None
    """Whether this is a development pipeline."""

    serverless: Optional[bool] = None
    """Whether to use serverless compute."""

    libraries: list = field(default_factory=list)
    """List of library configurations."""

    clusters: list = field(default_factory=list)
    """Cluster configurations for the pipeline."""

    configuration: dict = field(default_factory=dict)
    """Additional pipeline configuration key-value pairs."""


@dataclass
class ConnectorConfig:
    """
    Main configuration class for setting up a community connector.

    This combines repo and pipeline configurations needed to set up
    a complete connector in a Databricks workspace.
    """

    repo: RepoConfig
    """Repository configuration."""

    pipeline: PipelineConfig
    """Pipeline configuration."""

    workspace_host: Optional[str] = None
    """Databricks workspace URL. If None, uses environment variable or config file."""

    @classmethod
    def from_yaml(cls, path: str | Path) -> "ConnectorConfig":
        """
        Load configuration from a YAML file.

        Args:
            path: Path to the YAML configuration file.

        Returns:
            ConnectorConfig instance populated from the file.
        """
        with open(path, "r") as f:
            data = yaml.safe_load(f)

        repo_data = data.get("repo", {})
        pipeline_data = data.get("pipeline", {})

        return cls(
            repo=RepoConfig(**repo_data),
            pipeline=PipelineConfig(**pipeline_data),
            workspace_host=data.get("workspace_host"),
        )

    def to_yaml(self, path: str | Path) -> None:
        """
        Save configuration to a YAML file.

        Args:
            path: Path where the YAML file will be saved.
        """
        from dataclasses import asdict

        data = {
            "workspace_host": self.workspace_host,
            "repo": asdict(self.repo),
            "pipeline": asdict(self.pipeline),
        }

        with open(path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)


def load_default_config() -> dict:
    """
    Load the bundled default configuration.

    Returns:
        Dictionary containing the default configuration values.
    """
    # Load the default_config.yaml bundled with the package
    try:
        # Python 3.9+ way
        config_path = Path(__file__).parent / "default_config.yaml"
        with open(config_path, "r") as f:
            return yaml.safe_load(f) or {}
    except FileNotFoundError:
        return {}


def load_yaml_config(path: str | Path) -> dict:
    """
    Load configuration from a YAML file.

    Args:
        path: Path to the YAML configuration file.

    Returns:
        Dictionary containing the configuration values.
    """
    with open(path, "r") as f:
        return yaml.safe_load(f) or {}


def deep_merge(base: dict, override: dict) -> dict:
    """
    Deep merge two dictionaries, with override taking precedence.

    Args:
        base: Base dictionary with default values.
        override: Dictionary with values that override base.

    Returns:
        Merged dictionary.
    """
    result = base.copy()

    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value

    return result


def build_config(
    source_name: str,
    pipeline_name: str,
    repo_url: Optional[str] = None,
    catalog: Optional[str] = None,
    target: Optional[str] = None,
    config_file: Optional[str] = None,
) -> tuple[str, RepoConfig, PipelineConfig]:
    """
    Build configuration by merging defaults, config file, and CLI arguments.

    Precedence (highest to lowest):
        CLI arguments → config file → default_config.yaml

    Args:
        source_name: Name of the connector source (e.g., 'github', 'stripe').
        pipeline_name: Unique name for this pipeline instance.
        repo_url: Optional Git repository URL (CLI override).
        catalog: Optional Unity Catalog name (CLI override).
        target: Optional target schema (CLI override).
        config_file: Optional path to user config file.

    Returns:
        Tuple of (workspace_path, RepoConfig, PipelineConfig) with merged values.
        Note: workspace_path still contains {CURRENT_USER} placeholder to be resolved in CLI.
    """
    # Start with bundled defaults
    config = load_default_config()

    # Merge with user config file if provided
    if config_file:
        user_config = load_yaml_config(config_file)
        config = deep_merge(config, user_config)

    # Extract repo and pipeline sections
    repo_data = config.get("repo", {})
    pipeline_data = config.get("pipeline", {})

    # Apply CLI overrides (only if provided)
    if repo_url is not None:
        repo_data["url"] = repo_url

    if catalog is not None:
        pipeline_data["catalog"] = catalog

    if target is not None:
        pipeline_data["target"] = target
    elif "target" not in pipeline_data or pipeline_data["target"] is None:
        # Default target to source_name if not specified
        pipeline_data["target"] = source_name

    # Set pipeline name from CLI argument
    pipeline_data["name"] = pipeline_name

    # Replace {SOURCE_NAME} placeholder in sparse_checkout patterns
    sparse_checkout = repo_data.get("sparse_checkout")
    if sparse_checkout and "patterns" in sparse_checkout:
        updated_patterns = [
            pattern.replace("{SOURCE_NAME}", source_name) for pattern in sparse_checkout["patterns"]
        ]
        sparse_checkout = {"patterns": updated_patterns}

    # Process global workspace_path
    # Replace {PIPELINE_NAME} and {SOURCE_NAME} (but not {CURRENT_USER} - that's done in CLI)
    workspace_path = config.get("workspace_path", "")
    if workspace_path:
        workspace_path = workspace_path.replace("{PIPELINE_NAME}", pipeline_name)
        workspace_path = workspace_path.replace("{SOURCE_NAME}", source_name)

    # Build the config objects
    # Note: {WORKSPACE_PATH} and {CURRENT_USER} placeholders are resolved in CLI
    repo_config = RepoConfig(
        url=repo_data.get("url", ""),
        provider=repo_data.get("provider"),
        path=repo_data.get("path"),  # Contains {WORKSPACE_PATH}
        branch=repo_data.get("branch"),
        sparse_checkout=sparse_checkout,
        exclude_root_files=repo_data.get("exclude_root_files"),
    )

    pipeline_config = PipelineConfig(
        name=pipeline_name,
        target=pipeline_data.get("target"),
        catalog=pipeline_data.get("catalog"),
        root_path=pipeline_data.get("root_path"),  # Contains {WORKSPACE_PATH}
        channel=pipeline_data.get("channel"),
        continuous=pipeline_data.get("continuous"),
        development=pipeline_data.get("development"),
        serverless=pipeline_data.get("serverless"),
        libraries=pipeline_data.get("libraries", []),  # Contains {WORKSPACE_PATH}
        clusters=pipeline_data.get("clusters", []),
        configuration=pipeline_data.get("configuration", {}),
    )

    return workspace_path, repo_config, pipeline_config
