import requests
from requests.auth import HTTPBasicAuth
from typing import Iterator
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ArrayType,
    LongType,
)


class LakeflowConnect:
    """
    Splunk Metrics Catalog connector for Lakeflow.
    Retrieves metrics, dimensions, dimension values, and rollup policies from Splunk REST API.
    """

    def __init__(self, options: dict) -> None:
        """
        Initialize the Splunk connector with connection parameters.

        Args:
            options: Dictionary containing:
                - host: Splunk server hostname or IP address
                - port: Splunk management port (default: 8089)
                - username: Splunk username with appropriate permissions
                - password: Splunk password
                - verify_ssl: Whether to verify SSL certificates (default: False)
        """
        self.host = options.get("host")
        self.port = options.get("port", 8089)
        self.username = options.get("username")
        self.password = options.get("password")
        self.verify_ssl = options.get("verify_ssl", "false").lower() == "true" if isinstance(
            options.get("verify_ssl"), str
        ) else options.get("verify_ssl", False)

        if not self.host:
            raise ValueError("Missing required option: 'host'")
        if not self.username:
            raise ValueError("Missing required option: 'username'")
        if not self.password:
            raise ValueError("Missing required option: 'password'")

        self.base_url = f"https://{self.host}:{self.port}"
        self.auth = HTTPBasicAuth(self.username, self.password)

        # Default page size for pagination
        self.page_size = 100

    def list_tables(self) -> list[str]:
        """
        Returns a list of available tables from the Splunk Metrics Catalog.

        Returns:
            List of table names: metrics, dimensions, rollup_policies
        """
        return ["metrics", "dimensions", "rollup_policies"]

    def get_table_schema(self, table_name: str) -> StructType:
        """
        Fetch the schema of a table.

        Args:
            table_name: The name of the table to fetch the schema for.

        Returns:
            A StructType object representing the schema of the table.
        """
        schemas = {
            "metrics": StructType(
                [
                    StructField("metric_name", StringType(), False),
                    StructField("index", StringType(), True),
                    StructField(
                        "dimensions",
                        ArrayType(
                            StructType(
                                [
                                    StructField("name", StringType(), True),
                                    StructField("value", StringType(), True),
                                ]
                            )
                        ),
                        True,
                    ),
                ]
            ),
            "dimensions": StructType(
                [
                    StructField("dimension_name", StringType(), False),
                ]
            ),
            "rollup_policies": StructType(
                [
                    StructField("index", StringType(), False),
                    StructField("defaultAggregation", StringType(), True),
                    StructField("metricList", StringType(), True),
                    StructField("metricListType", StringType(), True),
                    StructField("dimensionList", StringType(), True),
                    StructField("dimensionListType", StringType(), True),
                    StructField(
                        "summaries",
                        ArrayType(
                            StructType(
                                [
                                    StructField("rollupIndex", StringType(), True),
                                    StructField("span", StringType(), True),
                                ]
                            )
                        ),
                        True,
                    ),
                ]
            ),
        }

        if table_name not in schemas:
            raise ValueError(f"Table '{table_name}' is not supported.")

        return schemas[table_name]

    def read_table_metadata(self, table_name: str) -> dict:
        """
        Fetch the metadata of a table.

        Args:
            table_name: The name of the table to fetch the metadata for.

        Returns:
            A dictionary containing primary_key and ingestion_type.
            All Splunk Metrics Catalog tables use snapshot ingestion.
        """
        metadata = {
            "metrics": {
                "primary_key": "metric_name",
                "ingestion_type": "snapshot",
            },
            "dimensions": {
                "primary_key": "dimension_name",
                "ingestion_type": "snapshot",
            },
            "rollup_policies": {
                "primary_key": "index",
                "ingestion_type": "snapshot",
            },
        }

        if table_name not in metadata:
            raise ValueError(f"Table '{table_name}' is not supported.")

        return metadata[table_name]

    def read_table(self, table_name: str, start_offset: dict) -> tuple[Iterator[dict], dict]:
        """
        Read the records of a table and return an iterator of records and an offset.

        Args:
            table_name: The name of the table to read.
            start_offset: The offset to start reading from.

        Returns:
            An iterator of records in JSON format and an offset dictionary.
        """
        if table_name == "metrics":
            return self._read_metrics(start_offset)
        elif table_name == "dimensions":
            return self._read_dimensions(start_offset)
        elif table_name == "rollup_policies":
            return self._read_rollup_policies(start_offset)
        else:
            raise ValueError(f"Table '{table_name}' is not supported.")

    def _make_request(self, endpoint: str, params: dict = None) -> dict:
        """
        Make a GET request to the Splunk REST API.

        Args:
            endpoint: API endpoint path
            params: Query parameters

        Returns:
            JSON response as dictionary
        """
        url = f"{self.base_url}{endpoint}"

        if params is None:
            params = {}

        # Always request JSON output
        params["output_mode"] = "json"

        response = requests.get(
            url,
            auth=self.auth,
            params=params,
            verify=self.verify_ssl,
        )

        if response.status_code == 401:
            raise Exception("Splunk API authentication failed. Check username and password.")
        elif response.status_code == 403:
            raise Exception(
                "Splunk API access forbidden. Ensure user has required capabilities "
                "(list_metrics_catalog for read operations)."
            )
        elif response.status_code != 200:
            raise Exception(f"Splunk API error: {response.status_code} {response.text}")

        return response.json()

    def _read_metrics(self, start_offset: dict) -> tuple[Iterator[dict], dict]:
        """
        Read metrics from the Splunk Metrics Catalog.

        Args:
            start_offset: Offset containing 'offset' key for pagination

        Returns:
            Iterator of metric records and next offset
        """
        offset = 0
        if start_offset and "offset" in start_offset:
            offset = start_offset["offset"]

        all_records = []
        current_offset = offset

        while True:
            params = {
                "count": self.page_size,
                "offset": current_offset,
            }

            data = self._make_request("/services/catalog/metricstore/metrics", params)
            entries = data.get("entry", [])

            if not entries:
                break

            for entry in entries:
                content = entry.get("content", {})
                record = self._parse_metric_entry(entry, content)
                all_records.append(record)

            if len(entries) < self.page_size:
                break

            current_offset += self.page_size

        # For snapshot tables, return None offset to indicate completion
        # or the same offset to signal no more data
        return iter(all_records), {"offset": current_offset, "completed": True}

    def _parse_metric_entry(self, entry: dict, content: dict) -> dict:
        """
        Parse a metric entry from the API response.

        Args:
            entry: The entry object from the response
            content: The content object within the entry

        Returns:
            Parsed metric record
        """
        metric_name = entry.get("name") or content.get("metric_name", "")
        index = content.get("index", None)

        # Parse dimensions if present
        dimensions = None
        dims_data = content.get("dimensions")
        if dims_data and isinstance(dims_data, dict):
            dimensions = [
                {"name": k, "value": str(v) if v is not None else None}
                for k, v in dims_data.items()
            ]

        return {
            "metric_name": metric_name,
            "index": index,
            "dimensions": dimensions,
        }

    def _read_dimensions(self, start_offset: dict) -> tuple[Iterator[dict], dict]:
        """
        Read dimensions from the Splunk Metrics Catalog.

        Args:
            start_offset: Offset containing 'offset' key for pagination

        Returns:
            Iterator of dimension records and next offset
        """
        offset = 0
        if start_offset and "offset" in start_offset:
            offset = start_offset["offset"]

        all_records = []
        current_offset = offset

        while True:
            params = {
                "count": self.page_size,
                "offset": current_offset,
            }

            data = self._make_request("/services/catalog/metricstore/dimensions", params)
            entries = data.get("entry", [])

            if not entries:
                break

            for entry in entries:
                content = entry.get("content", {})
                dimension_name = entry.get("name") or content.get("dimension_name", "")

                record = {
                    "dimension_name": dimension_name,
                }
                all_records.append(record)

            if len(entries) < self.page_size:
                break

            current_offset += self.page_size

        return iter(all_records), {"offset": current_offset, "completed": True}

    def _read_rollup_policies(self, start_offset: dict) -> tuple[Iterator[dict], dict]:
        """
        Read rollup policies from the Splunk Metrics Catalog.

        Args:
            start_offset: Offset containing 'offset' key for pagination

        Returns:
            Iterator of rollup policy records and next offset
        """
        offset = 0
        if start_offset and "offset" in start_offset:
            offset = start_offset["offset"]

        all_records = []
        current_offset = offset

        while True:
            params = {
                "count": self.page_size,
                "offset": current_offset,
            }

            data = self._make_request("/services/catalog/metricstore/rollup", params)
            entries = data.get("entry", [])

            if not entries:
                break

            for entry in entries:
                content = entry.get("content", {})
                record = self._parse_rollup_entry(entry, content)
                all_records.append(record)

            if len(entries) < self.page_size:
                break

            current_offset += self.page_size

        return iter(all_records), {"offset": current_offset, "completed": True}

    def _parse_rollup_entry(self, entry: dict, content: dict) -> dict:
        """
        Parse a rollup policy entry from the API response.

        Args:
            entry: The entry object from the response
            content: The content object within the entry

        Returns:
            Parsed rollup policy record
        """
        index_name = entry.get("name") or content.get("index", "")

        # Parse summaries if present
        summaries = None
        summaries_data = content.get("summaries")
        if summaries_data and isinstance(summaries_data, dict):
            summaries = []
            for key, value in summaries_data.items():
                if isinstance(value, dict):
                    summary = {
                        "rollupIndex": value.get("rollupIndex", None),
                        "span": value.get("span", None),
                    }
                    summaries.append(summary)

        return {
            "index": index_name,
            "defaultAggregation": content.get("defaultAggregation", None),
            "metricList": content.get("metricList", None),
            "metricListType": content.get("metricListType", None),
            "dimensionList": content.get("dimensionList", None),
            "dimensionListType": content.get("dimensionListType", None),
            "summaries": summaries,
        }

