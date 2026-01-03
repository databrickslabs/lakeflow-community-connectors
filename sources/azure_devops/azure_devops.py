from __future__ import annotations

import requests
import base64
from typing import Iterator, Any

from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    BooleanType,
)


class LakeflowConnect:
    def __init__(self, options: dict[str, str]) -> None:
        """
        Initialize the Azure DevOps connector with connection-level options.

        Expected options:
            - organization: Azure DevOps organization name.
            - project: Project name or ID.
            - personal_access_token: Personal access token (PAT) for authentication.
        """
        organization = options.get("organization")
        project = options.get("project")
        personal_access_token = options.get("personal_access_token")

        if not organization:
            raise ValueError(
                "Azure DevOps connector requires 'organization' in options"
            )
        if not project:
            raise ValueError("Azure DevOps connector requires 'project' in options")
        if not personal_access_token:
            raise ValueError(
                "Azure DevOps connector requires 'personal_access_token' in options"
            )

        self.organization = organization
        self.project = project
        self.base_url = f"https://dev.azure.com/{organization}"

        # Encode PAT as Base64 in the format :{pat}
        auth_string = f":{personal_access_token}"
        auth_bytes = auth_string.encode("ascii")
        base64_auth = base64.b64encode(auth_bytes).decode("ascii")

        # Configure a session with proper headers for Azure DevOps REST API
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Basic {base64_auth}",
                "Accept": "application/json",
            }
        )

    def list_tables(self) -> list[str]:
        """
        List names of all tables supported by this connector.

        For the initial implementation, only the 'repositories' table is supported.
        """
        return ["repositories"]

    def get_table_schema(
        self, table_name: str, table_options: dict[str, str]
    ) -> StructType:
        """
        Fetch the schema of a table.

        The schema is static and derived from the Azure DevOps REST API documentation
        and connector design for the `repositories` object.
        """
        if table_name not in self.list_tables():
            raise ValueError(f"Unsupported table: {table_name!r}")

        # Nested `project` struct schema
        project_struct = StructType(
            [
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("url", StringType(), True),
                StructField("state", StringType(), True),
                StructField("revision", LongType(), True),
                StructField("visibility", StringType(), True),
                StructField("lastUpdateTime", StringType(), True),
            ]
        )

        # Nested `parentRepository` struct schema (only present for forks)
        parent_repository_struct = StructType(
            [
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("url", StringType(), True),
                StructField("project", project_struct, True),
            ]
        )

        # Nested `_links` struct schema (HAL-style hypermedia links)
        # Each link has an 'href' field
        link_struct = StructType([StructField("href", StringType(), True)])

        links_struct = StructType(
            [
                StructField("self", link_struct, True),
                StructField("project", link_struct, True),
                StructField("web", link_struct, True),
                StructField("ssh", link_struct, True),
                StructField("commits", link_struct, True),
                StructField("refs", link_struct, True),
                StructField("pullRequests", link_struct, True),
                StructField("items", link_struct, True),
                StructField("pushes", link_struct, True),
            ]
        )

        if table_name == "repositories":
            repositories_schema = StructType(
                [
                    StructField("id", StringType(), False),
                    StructField("name", StringType(), True),
                    StructField("url", StringType(), True),
                    StructField("project", project_struct, True),
                    StructField("defaultBranch", StringType(), True),
                    StructField("size", LongType(), True),
                    StructField("remoteUrl", StringType(), True),
                    StructField("sshUrl", StringType(), True),
                    StructField("webUrl", StringType(), True),
                    StructField("isDisabled", BooleanType(), True),
                    StructField("isInMaintenance", BooleanType(), True),
                    StructField("isFork", BooleanType(), True),
                    StructField("parentRepository", parent_repository_struct, True),
                    StructField("_links", links_struct, True),
                    StructField("organization", StringType(), False),
                    StructField("project_name", StringType(), False),
                ]
            )
            return repositories_schema

        raise ValueError(f"Unsupported table: {table_name!r}")

    def read_table_metadata(
        self, table_name: str, table_options: dict[str, str]
    ) -> dict:
        """
        Fetch metadata for the given table.

        For `repositories`:
            - ingestion_type: snapshot
            - primary_keys: ["id"]
        """
        if table_name not in self.list_tables():
            raise ValueError(f"Unsupported table: {table_name!r}")

        if table_name == "repositories":
            return {
                "primary_keys": ["id"],
                "ingestion_type": "snapshot",
            }

        raise ValueError(f"Unsupported table: {table_name!r}")

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read records from a table and return raw JSON-like dictionaries.

        For the `repositories` table this method:
            - Uses `/{organization}/{project}/_apis/git/repositories` endpoint.
            - Returns all repositories in a single batch (no pagination required).
            - Returns an empty offset dict (snapshot ingestion).

        No table_options are required for repositories, as organization and project
        are provided at connection level.
        """
        if table_name not in self.list_tables():
            raise ValueError(f"Unsupported table: {table_name!r}")

        if table_name == "repositories":
            return self._read_repositories(start_offset, table_options)

        raise ValueError(f"Unsupported table: {table_name!r}")

    def _read_repositories(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read the `repositories` snapshot table.

        This implementation lists all Git repositories in the configured project
        using the Azure DevOps REST API:

            GET /{organization}/{project}/_apis/git/repositories?api-version=7.1

        Note: API version 7.1 is used instead of 7.2 to avoid preview version
        requirements. Version 7.2 requires the -preview flag as of this implementation.

        The returned JSON objects are enriched with connector-derived fields:
            - organization: The organization name from connection config.
            - project_name: The project name from connection config.
        """
        url = f"{self.base_url}/{self.project}/_apis/git/repositories"
        params = {
            "api-version": "7.1",
            "includeLinks": "true",
            "includeAllUrls": "true",
        }

        response = self._session.get(url, params=params, timeout=30)
        if response.status_code != 200:
            raise RuntimeError(
                f"Azure DevOps API error for repositories: {response.status_code} {response.text}"
            )

        response_json = response.json()
        if not isinstance(response_json, dict):
            raise ValueError(
                f"Unexpected response format for repositories: {type(response_json).__name__}"
            )

        repos = response_json.get("value", [])
        if not isinstance(repos, list):
            raise ValueError(
                f"Unexpected 'value' format in repositories response: {type(repos).__name__}"
            )

        records: list[dict[str, Any]] = []
        for repo_obj in repos:
            # Shallow-copy the raw JSON and add connector-derived fields
            record: dict[str, Any] = dict(repo_obj)
            record["organization"] = self.organization
            record["project_name"] = self.project

            # Ensure nested structs that are absent are represented as None, not {}
            if "parentRepository" not in record or record["parentRepository"] == {}:
                record["parentRepository"] = None
            if "project" not in record or record["project"] == {}:
                record["project"] = None
            if "_links" not in record or record["_links"] == {}:
                record["_links"] = None

            records.append(record)

        # Snapshot ingestion: return empty offset
        return iter(records), {}

