"""
Pipeline client for Databricks Delta Live Tables pipelines.

This module provides a client for creating and managing DLT pipelines
in a Databricks workspace using the Databricks SDK.

API Reference: https://docs.databricks.com/api/workspace/pipelines/create
"""

from typing import List, Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import (
    CreatePipelineResponse,
    FileLibrary,
    GetPipelineResponse,
    NotebookLibrary,
    PipelineLibrary,
    StartUpdateResponse,
)

from databricks.labs.community_connector_cli.config import PipelineConfig


class PipelineClient:
    """
    Client for managing Databricks DLT pipelines.

    This client wraps the Databricks SDK pipelines API to provide
    a simplified interface for creating and managing pipelines.
    """

    def __init__(self, workspace_client: Optional[WorkspaceClient] = None):
        """
        Initialize the PipelineClient.

        Args:
            workspace_client: Optional WorkspaceClient instance. If not provided,
                            a new client will be created using default authentication.
        """
        self._client = workspace_client or WorkspaceClient()

    @property
    def client(self) -> WorkspaceClient:
        """Get the underlying WorkspaceClient."""
        return self._client

    def create(
        self,
        config: PipelineConfig,
        repo_path: Optional[str] = None,
        source_name: Optional[str] = None,
    ) -> CreatePipelineResponse:
        """
        Create a new DLT pipeline in the Databricks workspace.

        Args:
            config: PipelineConfig containing the pipeline configuration.
            repo_path: Optional workspace path to the repo. Used to construct
                      library paths if they are relative.
            source_name: Name of the connector source (e.g., 'github', 'stripe').
                        Used to determine library paths.

        Returns:
            CreatePipelineResponse containing the pipeline_id of the created pipeline.

        API Reference: https://docs.databricks.com/api/workspace/pipelines/create
        """
        create_kwargs = self._build_create_payload(config, repo_path, source_name)
        return self._client.pipelines.create(**create_kwargs)

    def _build_create_payload(
        self,
        config: PipelineConfig,
        repo_path: Optional[str] = None,
        source_name: Optional[str] = None,
    ) -> dict:
        """
        Build the payload for the pipelines.create API call.

        Args:
            config: PipelineConfig containing the pipeline configuration.
            repo_path: Optional workspace path to prepend to relative library paths.
            source_name: Name of the connector source (e.g., 'github', 'stripe').

        Returns:
            Dictionary of keyword arguments for the create API call.
        """
        payload = {
            "name": config.name,
        }

        # Add optional fields only if they have values
        if config.channel is not None:
            payload["channel"] = config.channel

        if config.continuous is not None:
            payload["continuous"] = config.continuous

        if config.development is not None:
            payload["development"] = config.development

        if config.serverless is not None:
            payload["serverless"] = config.serverless

        if config.catalog:
            payload["catalog"] = config.catalog

        if config.target:
            payload["target"] = config.target

        if config.root_path:
            payload["root_path"] = config.root_path

        if config.configuration:
            payload["configuration"] = config.configuration

        # Build libraries from config (already has placeholders resolved)
        if config.libraries:
            payload["libraries"] = self._build_libraries(config.libraries)

        return payload

    # pylint: disable=too-many-branches
    def _build_libraries(self, libraries: List) -> List:
        """
        Build the libraries configuration for the pipeline.

        Converts YAML library config to SDK PipelineLibrary objects.
        Supports: notebook, file, and glob (converted to file).

        Args:
            libraries: List of library configurations from PipelineConfig.

        Returns:
            List of PipelineLibrary objects.
        """

        result = []
        for lib in libraries:
            if isinstance(lib, dict):
                if "notebook" in lib:
                    notebook_config = lib["notebook"]
                    if isinstance(notebook_config, dict):
                        path = notebook_config.get("path")
                    else:
                        path = notebook_config
                    result.append(PipelineLibrary(notebook=NotebookLibrary(path=path)))
                elif "file" in lib:
                    file_config = lib["file"]
                    if isinstance(file_config, dict):
                        path = file_config.get("path")
                    else:
                        path = file_config
                    result.append(PipelineLibrary(file=FileLibrary(path=path)))
                elif "glob" in lib:
                    # Convert glob.include to file library
                    glob_config = lib["glob"]
                    if isinstance(glob_config, dict):
                        path = glob_config.get("include")
                    else:
                        path = glob_config
                    result.append(PipelineLibrary(file=FileLibrary(path=path)))
                else:
                    # Unknown format - skip with warning (could log this)
                    pass
            else:
                # If it's already a PipelineLibrary object, use it as-is
                result.append(lib)

        return result

    def get(self, pipeline_id: str) -> GetPipelineResponse:
        """
        Get information about a pipeline.

        Args:
            pipeline_id: The ID of the pipeline.

        Returns:
            GetPipelineResponse object containing pipeline information.
        """
        return self._client.pipelines.get(pipeline_id=pipeline_id)

    def start(self, pipeline_id: str, full_refresh: bool = False) -> StartUpdateResponse:
        """
        Start a pipeline update.

        Args:
            pipeline_id: The ID of the pipeline to start.
            full_refresh: If True, run a full refresh instead of incremental.

        Returns:
            StartUpdateResponse containing the update_id.
        """
        return self._client.pipelines.start_update(
            pipeline_id=pipeline_id,
            full_refresh=full_refresh,
        )

    def list(self, filter: Optional[str] = None, max_results: Optional[int] = None):
        """
        List pipelines in the workspace.

        Args:
            filter: Optional filter string.
            max_results: Maximum number of results to return.

        Returns:
            Iterator of PipelineStateInfo objects.
        """
        return self._client.pipelines.list_pipelines(filter=filter, max_results=max_results)
