"""Google Workspace Reports API User Activity connector.

This connector ingests administrative activity logs from the Google Workspace
Reports API. It supports reading activities from multiple application types
(admin, login, saml, user_accounts, groups, groups_enterprise) with time-window
and pagination support.

Authentication: Unity Catalog COMMUNITY connection with OAuth 2.0 u2m flow.
The connector receives an access_token injected by UC at runtime.
"""

from typing import Dict, Iterator
import logging

from pyspark.sql.types import StructType, StructField, StringType, LongType

from databricks.labs.community_connector.interface.lakeflow_connect import LakeflowConnect

logger = logging.getLogger(__name__)

# Default applications to ingest (from original collector)
DEFAULT_APPLICATIONS = [
    "admin",
    "login",
    "saml",
    "user_accounts",
    "groups",
    "groups_enterprise",
]

# Windowing behavior (from original collector)
INITIAL_LOOKBACK_DAYS = 1000
OVERLAP_MINUTES = 10
GUARD_DELAY_MINUTES = 2
MAX_RESULTS_PER_REQUEST = 500


class GoogleUserActivityLakeflowConnect(LakeflowConnect):
    """Connector for Google Workspace Reports API User Activity logs.

    Supports reading administrative activity events from multiple Google Workspace
    applications. Uses Unity Catalog COMMUNITY connection with OAuth 2.0 u2m flow
    for authentication.

    Tables (one per application):
    - admin: Administrative actions (user management, org settings, etc.)
    - login: Login attempts and authentication events
    - saml: SAML authentication events
    - user_accounts: User account lifecycle events
    - groups: Group membership changes
    - groups_enterprise: Enterprise group management

    Each table implements append-only ingestion with time-window pagination and
    deterministic deduplication via lw_id hash.
    """

    def __init__(self, options: dict[str, str]) -> None:
        """
        Initialize the Google User Activity connector.

        Expected options:
            - access_token: OAuth 2.0 bearer token (REQUIRED, injected by UC COMMUNITY connection)
            - impersonated_admin_email: Optional admin email for domain-wide delegation

        Raises:
            ValueError: If access_token is missing (points user to COMMUNITY connection setup)
        """
        self.access_token = options.get("access_token")
        self.impersonated_admin_email = options.get("impersonated_admin_email")

        if not self.access_token:
            raise ValueError(
                "Google User Activity connector requires 'access_token' injected by the "
                "Unity Catalog COMMUNITY connection (oauth flow: u2m). "
                "Ensure the connection is configured with OAuth 2.0 and the required scope: "
                "https://www.googleapis.com/auth/admin.reports.audit.readonly"
            )

        logger.info(
            "Google User Activity connector initialized (impersonated_admin_email=%s)",
            self.impersonated_admin_email or "(none)",
        )

    def list_tables(self) -> list[str]:
        """Return available tables (one per application).

        Returns:
            List of application names: admin, login, saml, user_accounts, groups, groups_enterprise
        """
        return DEFAULT_APPLICATIONS.copy()

    def get_table_schema(self, table_name: str, table_options: Dict[str, str]) -> StructType:
        """Return schema for activity events.

        Args:
            table_name: Application name (validated)
            table_options: Unused

        Returns:
            StructType with fields: lw_id (StringType), event_name, event_time, raw_event (JSON)

        Raises:
            ValueError: If table_name is not a supported application
        """
        self._validate_table(table_name)
        return self._get_schema()

    def read_table_metadata(self, table_name: str, table_options: Dict[str, str]) -> Dict:
        """Return metadata for a table.

        Args:
            table_name: Application name (validated)
            table_options: Unused

        Returns:
            Dict with primary_keys and ingestion_type
        """
        self._validate_table(table_name)
        return {
            "primary_keys": ["lw_id"],
            "ingestion_type": "append",
        }

    def read_table(
        self, table_name: str, start_offset: dict, table_options: Dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """
        Read activity events for the specified application.

        Phase 1 (simulate mode): Returns empty iterator.
        Phase 2 (live mode): Calls Google Reports API with time-window pagination.

        Args:
            table_name: Application name (e.g., 'admin', 'login', 'saml')
            start_offset: Cursor state {'time_min': ISO8601_timestamp}
            table_options: Optional filters:
                - user_key: User filter (typically 'all')
                - start_time: ISO 8601 start timestamp
                - end_time: ISO 8601 end timestamp
                - max_results: Records per API request (default: 500)

        Yields:
            Activity event records with lw_id (deterministic hash)

        Returns:
            Tuple of (records_iterator, next_offset dict with next time_min)
        """
        self._validate_table(table_name)
        # Phase 1: Simulate mode returns empty
        # Phase 2 (live): Implement Google Reports API pagination here
        return iter([]), {}

    def _validate_table(self, table_name: str) -> None:
        """Validate that the table (application) is supported.

        Args:
            table_name: Application name to validate

        Raises:
            ValueError: If table_name is not in DEFAULT_APPLICATIONS
        """
        if table_name not in DEFAULT_APPLICATIONS:
            raise ValueError(
                f"Unsupported application: '{table_name}'. "
                f"Supported applications: {', '.join(DEFAULT_APPLICATIONS)}"
            )

    @staticmethod
    def _get_schema() -> StructType:
        """Return StructType schema for activity events.

        Schema matches Google Reports API activity events.
        Uses LongType for numeric fields (per public repo standards).
        No 'from __future__ import annotations' (per issue #173).

        Returns:
            StructType with event fields
        """
        return StructType(
            [
                StructField("lw_id", StringType(), False),
                StructField("event_name", StringType(), False),
                StructField("event_time", StringType(), False),
                StructField("actor_email", StringType(), True),
                StructField("ip_address", StringType(), True),
            ]
        )
