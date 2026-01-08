import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Iterator, Optional

from bingads.authorization import (
    AuthorizationData,
    OAuthDesktopMobileAuthCodeGrant,
    OAuthWebAuthCodeGrant,
)
from bingads.service_client import ServiceClient
from pyspark.sql.types import (
    ArrayType,
    DataType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)


class SoapFieldType(Enum):
    """Types of SOAP field serialization."""

    SIMPLE = "simple"  # Direct attribute access
    ENUM = "enum"  # Convert enum to string
    STRING_ARRAY = "string_array"  # SOAP string array → Python list
    DATE = "date"  # SOAP date → YYYY-MM-DD string
    BID = "bid"  # Nested bid object with Amount
    BIDDING_SCHEME = "bidding_scheme"  # Complex bidding scheme object
    ASSET_ARRAY = "asset_array"  # Array of asset links (Headlines, Descriptions)
    AD_ROTATION = "ad_rotation"  # Nested AdRotation.Type


@dataclass
class FieldDef:
    """Single source of truth for a field definition.

    This defines both the Spark schema and SOAP serialization behavior.

    Attributes:
        name: The field name in the output dictionary and Spark schema.
        spark_type: The Spark data type for this field.
        nullable: Whether the field can be null in Spark schema.
        field_type: How to serialize the field from SOAP objects.
        source_attr: The SOAP attribute name if different from name.
    """

    name: str
    spark_type: DataType
    nullable: bool = True
    field_type: SoapFieldType = SoapFieldType.SIMPLE
    source_attr: Optional[str] = None

    @property
    def attr_name(self) -> str:
        """Return the SOAP attribute name to access."""
        return self.source_attr or self.name


def _to_spark_schema(fields: list[FieldDef]) -> StructType:
    """Generate a Spark StructType schema from field definitions.

    Args:
        fields: List of field definitions.

    Returns:
        A StructType representing the Spark schema.
    """
    return StructType(
        [StructField(f.name, f.spark_type, f.nullable) for f in fields]
    )


class LakeflowConnect:
    """Bing Ads connector for Lakeflow Connect."""

    # -------------------------------------------------------------------------
    # Nested Struct Types for Schemas
    # -------------------------------------------------------------------------

    _BIDDING_SCHEME_STRUCT = StructType(
        [
            StructField("Type", StringType(), True),
            StructField("MaxCpc", DoubleType(), True),
            StructField("TargetCpa", DoubleType(), True),
            StructField("TargetRoas", DoubleType(), True),
        ]
    )

    _BID_STRUCT = StructType(
        [
            StructField("Amount", DoubleType(), True),
        ]
    )

    # -------------------------------------------------------------------------
    # SOAP Field Definitions for Entity Tables
    # -------------------------------------------------------------------------

    _SOAP_FIELDS: dict[str, list[FieldDef]] = {
        "accounts": [
            FieldDef("Id", LongType(), nullable=False),
            FieldDef("Name", StringType()),
            FieldDef("Number", StringType()),
            FieldDef("AccountType", StringType(), field_type=SoapFieldType.ENUM),
            FieldDef("AccountLifeCycleStatus", StringType(), field_type=SoapFieldType.ENUM),
            FieldDef("PauseReason", LongType()),
            FieldDef("CurrencyCode", StringType(), field_type=SoapFieldType.ENUM),
            FieldDef("TimeZone", StringType(), field_type=SoapFieldType.ENUM),
            FieldDef("Language", StringType(), field_type=SoapFieldType.ENUM),
            FieldDef("PaymentMethodId", LongType()),
            FieldDef("PaymentMethodType", StringType(), field_type=SoapFieldType.ENUM),
            FieldDef("BillToCustomerId", LongType()),
            FieldDef("AutoTagType", StringType(), field_type=SoapFieldType.ENUM),
        ],
        "campaigns": [
            FieldDef("Id", LongType(), nullable=False),
            FieldDef("Name", StringType()),
            FieldDef("Status", StringType(), field_type=SoapFieldType.ENUM),
            FieldDef("BudgetType", StringType(), field_type=SoapFieldType.ENUM),
            FieldDef("DailyBudget", DoubleType()),
            FieldDef("TimeZone", StringType()),
            FieldDef("CampaignType", StringType(), field_type=SoapFieldType.ENUM),
            FieldDef("Languages", ArrayType(StringType()), field_type=SoapFieldType.STRING_ARRAY),
            FieldDef("BiddingScheme", _BIDDING_SCHEME_STRUCT, field_type=SoapFieldType.BIDDING_SCHEME),
            FieldDef("TrackingUrlTemplate", StringType()),
            FieldDef("AudienceAdsBidAdjustment", LongType()),
            FieldDef("ExperimentId", LongType()),
            FieldDef("FinalUrlSuffix", StringType()),
            FieldDef("MultimediaAdsBidAdjustment", LongType()),
            FieldDef("BidStrategyId", LongType()),
        ],
        "ad_groups": [
            FieldDef("Id", LongType(), nullable=False),
            FieldDef("Name", StringType()),
            FieldDef("Status", StringType(), field_type=SoapFieldType.ENUM),
            FieldDef("StartDate", StringType(), field_type=SoapFieldType.DATE),
            FieldDef("EndDate", StringType(), field_type=SoapFieldType.DATE),
            FieldDef("CpcBid", _BID_STRUCT, field_type=SoapFieldType.BID),
            FieldDef("Language", StringType()),
            FieldDef("Network", StringType(), field_type=SoapFieldType.ENUM),
            FieldDef("AdRotation", StringType(), field_type=SoapFieldType.AD_ROTATION),
            FieldDef("PrivacyStatus", StringType(), field_type=SoapFieldType.ENUM),
            FieldDef("TrackingUrlTemplate", StringType()),
            FieldDef("FinalUrlSuffix", StringType()),
        ],
        "ads": [
            FieldDef("Id", LongType(), nullable=False),
            FieldDef("Type", StringType(), field_type=SoapFieldType.ENUM),
            FieldDef("Status", StringType(), field_type=SoapFieldType.ENUM),
            FieldDef("EditorialStatus", StringType(), field_type=SoapFieldType.ENUM),
            FieldDef("DevicePreference", LongType()),
            FieldDef("TrackingUrlTemplate", StringType()),
            FieldDef("FinalUrls", ArrayType(StringType()), field_type=SoapFieldType.STRING_ARRAY),
            FieldDef("FinalMobileUrls", ArrayType(StringType()), field_type=SoapFieldType.STRING_ARRAY),
            FieldDef("FinalUrlSuffix", StringType()),
            # Text Ad specific fields
            FieldDef("Title", StringType()),
            FieldDef("Text", StringType()),
            FieldDef("DisplayUrl", StringType()),
            FieldDef("DestinationUrl", StringType()),
            # RSA specific fields
            FieldDef("Headlines", ArrayType(StringType()), field_type=SoapFieldType.ASSET_ARRAY),
            FieldDef("Descriptions", ArrayType(StringType()), field_type=SoapFieldType.ASSET_ARRAY),
            FieldDef("Path1", StringType()),
            FieldDef("Path2", StringType()),
        ],
        "keywords": [
            FieldDef("Id", LongType(), nullable=False),
            FieldDef("Text", StringType()),
            FieldDef("MatchType", StringType(), field_type=SoapFieldType.ENUM),
            FieldDef("Status", StringType(), field_type=SoapFieldType.ENUM),
            FieldDef("Bid", _BID_STRUCT, field_type=SoapFieldType.BID),
            FieldDef("DestinationUrl", StringType()),
            FieldDef("FinalUrls", ArrayType(StringType()), field_type=SoapFieldType.STRING_ARRAY),
            FieldDef("FinalMobileUrls", ArrayType(StringType()), field_type=SoapFieldType.STRING_ARRAY),
            FieldDef("TrackingUrlTemplate", StringType()),
            FieldDef("FinalUrlSuffix", StringType()),
        ],
    }

    # -------------------------------------------------------------------------
    # Supported Tables Configuration
    # -------------------------------------------------------------------------
    SUPPORTED_TABLES = {
        "accounts": {
            "primary_keys": ["Id"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
            "schema": _to_spark_schema(_SOAP_FIELDS["accounts"]),
            "fields": _SOAP_FIELDS["accounts"],
        },
        "campaigns": {
            "primary_keys": ["Id"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
            "schema": _to_spark_schema(
                _SOAP_FIELDS["campaigns"] + [FieldDef("AccountId", LongType())]
            ),
            "fields": _SOAP_FIELDS["campaigns"],
        },
        "ad_groups": {
            "primary_keys": ["Id"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
            "schema": _to_spark_schema(
                _SOAP_FIELDS["ad_groups"] + [
                    FieldDef("CampaignId", LongType()),
                    FieldDef("AccountId", LongType()),
                ]
            ),
            "fields": _SOAP_FIELDS["ad_groups"],
        },
        "ads": {
            "primary_keys": ["Id"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
            "schema": _to_spark_schema(
                _SOAP_FIELDS["ads"] + [
                    FieldDef("AdGroupId", LongType()),
                    FieldDef("AccountId", LongType()),
                    FieldDef("CampaignId", LongType()),
                ]
            ),
            "fields": _SOAP_FIELDS["ads"],
        },
        "keywords": {
            "primary_keys": ["Id"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
            "schema": _to_spark_schema(
                _SOAP_FIELDS["keywords"] + [
                    FieldDef("AdGroupId", LongType()),
                    FieldDef("AccountId", LongType()),
                    FieldDef("CampaignId", LongType()),
                ]
            ),
            "fields": _SOAP_FIELDS["keywords"],
        },
        # Report tables - parsed from CSV, no SOAP serialization needed
        "campaign_performance_report": {
            "primary_keys": ["TimePeriod", "AccountId", "CampaignId"],
            "cursor_field": "TimePeriod",
            "ingestion_type": "append",
            "schema": StructType(
                [
                    StructField("TimePeriod", StringType(), False),
                    StructField("AccountId", LongType(), True),
                    StructField("AccountName", StringType(), True),
                    StructField("CampaignId", LongType(), True),
                    StructField("CampaignName", StringType(), True),
                    StructField("CampaignStatus", StringType(), True),
                    StructField("Impressions", LongType(), True),
                    StructField("Clicks", LongType(), True),
                    StructField("Ctr", DoubleType(), True),
                    StructField("AverageCpc", DoubleType(), True),
                    StructField("Spend", DoubleType(), True),
                    StructField("Conversions", LongType(), True),
                    StructField("ConversionRate", DoubleType(), True),
                    StructField("CostPerConversion", DoubleType(), True),
                    StructField("Revenue", DoubleType(), True),
                    StructField("ReturnOnAdSpend", DoubleType(), True),
                ]
            ),
        },
        "ad_group_performance_report": {
            "primary_keys": ["TimePeriod", "AccountId", "CampaignId", "AdGroupId"],
            "cursor_field": "TimePeriod",
            "ingestion_type": "append",
            "schema": StructType(
                [
                    StructField("TimePeriod", StringType(), False),
                    StructField("AccountId", LongType(), True),
                    StructField("AccountName", StringType(), True),
                    StructField("CampaignId", LongType(), True),
                    StructField("CampaignName", StringType(), True),
                    StructField("AdGroupId", LongType(), True),
                    StructField("AdGroupName", StringType(), True),
                    StructField("Impressions", LongType(), True),
                    StructField("Clicks", LongType(), True),
                    StructField("Ctr", DoubleType(), True),
                    StructField("AverageCpc", DoubleType(), True),
                    StructField("Spend", DoubleType(), True),
                    StructField("Conversions", LongType(), True),
                    StructField("ConversionRate", DoubleType(), True),
                    StructField("CostPerConversion", DoubleType(), True),
                    StructField("Revenue", DoubleType(), True),
                    StructField("ReturnOnAdSpend", DoubleType(), True),
                ]
            ),
        },
        "ad_performance_report": {
            "primary_keys": [
                "TimePeriod",
                "AccountId",
                "CampaignId",
                "AdGroupId",
                "AdId",
            ],
            "cursor_field": "TimePeriod",
            "ingestion_type": "append",
            "schema": StructType(
                [
                    StructField("TimePeriod", StringType(), False),
                    StructField("AccountId", LongType(), True),
                    StructField("AccountName", StringType(), True),
                    StructField("CampaignId", LongType(), True),
                    StructField("CampaignName", StringType(), True),
                    StructField("AdGroupId", LongType(), True),
                    StructField("AdGroupName", StringType(), True),
                    StructField("AdId", LongType(), True),
                    StructField("AdTitle", StringType(), True),
                    StructField("AdStatus", StringType(), True),
                    StructField("AdType", StringType(), True),
                    StructField("Impressions", LongType(), True),
                    StructField("Clicks", LongType(), True),
                    StructField("Ctr", DoubleType(), True),
                    StructField("AverageCpc", DoubleType(), True),
                    StructField("Spend", DoubleType(), True),
                    StructField("Conversions", LongType(), True),
                    StructField("ConversionRate", DoubleType(), True),
                    StructField("CostPerConversion", DoubleType(), True),
                    StructField("Revenue", DoubleType(), True),
                    StructField("ReturnOnAdSpend", DoubleType(), True),
                ]
            ),
        },
        "keyword_performance_report": {
            "primary_keys": [
                "TimePeriod",
                "AccountId",
                "CampaignId",
                "AdGroupId",
                "KeywordId",
            ],
            "cursor_field": "TimePeriod",
            "ingestion_type": "append",
            "schema": StructType(
                [
                    StructField("TimePeriod", StringType(), False),
                    StructField("AccountId", LongType(), True),
                    StructField("AccountName", StringType(), True),
                    StructField("CampaignId", LongType(), True),
                    StructField("CampaignName", StringType(), True),
                    StructField("AdGroupId", LongType(), True),
                    StructField("AdGroupName", StringType(), True),
                    StructField("KeywordId", LongType(), True),
                    StructField("Keyword", StringType(), True),
                    StructField("BidMatchType", StringType(), True),
                    StructField("Impressions", LongType(), True),
                    StructField("Clicks", LongType(), True),
                    StructField("Ctr", DoubleType(), True),
                    StructField("AverageCpc", DoubleType(), True),
                    StructField("Spend", DoubleType(), True),
                    StructField("QualityScore", LongType(), True),
                    StructField("Conversions", LongType(), True),
                    StructField("ConversionRate", DoubleType(), True),
                    StructField("CostPerConversion", DoubleType(), True),
                    StructField("Revenue", DoubleType(), True),
                    StructField("ReturnOnAdSpend", DoubleType(), True),
                ]
            ),
        },
    }

    def __init__(self, options: dict[str, str]) -> None:
        """Initialize the Bing Ads connector.

        Args:
            options: A dictionary containing the following keys:
                - client_id: Application (client) ID from Azure Portal
                - client_secret: Client secret (optional for native apps)
                - refresh_token: Long-lived token for obtaining access tokens
                - developer_token: Developer token from Microsoft Advertising
                - customer_id: Target customer ID
                - account_id: Target account ID (optional, can be provided in table_options)
                - env: Environment to use ("production" or "sandbox"), defaults to "production"
        """
        
        print(f"Initializing Bing Ads connector with options: {options}")
        
        self.client_id = options.get("client_id")
        self.client_secret = options.get("client_secret")
        self.refresh_token = options.get("refresh_token")
        self.developer_token = options.get("developer_token")
        self.customer_id = options.get("customer_id")
        self.account_id = options.get("account_id")
        self.env = options.get("env", "production")

        if not self.client_id:
            raise ValueError("Bing Ads connector requires 'client_id' in options")
        if not self.refresh_token:
            raise ValueError("Bing Ads connector requires 'refresh_token' in options")
        if not self.developer_token:
            raise ValueError("Bing Ads connector requires 'developer_token' in options")

        # Initialize authorization
        self._authorization_data = self._create_authorization_data()

    def _create_authorization_data(self) -> AuthorizationData:
        """Create and configure AuthorizationData with OAuth tokens.

        Returns:
            AuthorizationData configured with the OAuth tokens.
        """
        authorization_data = AuthorizationData(
            account_id=self.account_id,
            customer_id=self.customer_id,
            developer_token=self.developer_token,
            authentication=None,
        )

        # Create OAuth authentication based on whether client_secret is provided
        # Use OAuthDesktopMobileAuthCodeGrant for native/desktop apps (no secret)
        # Use OAuthWebAuthCodeGrant for web apps (with secret)
        if self.client_secret:
            authentication = OAuthWebAuthCodeGrant(
                client_id=self.client_id,
                client_secret=self.client_secret,
                env=self.env,
            )
        else:
            authentication = OAuthDesktopMobileAuthCodeGrant(
                client_id=self.client_id,
                env=self.env,
            )

        # Exchange refresh token for access token
        authentication.request_oauth_tokens_by_refresh_token(self.refresh_token)
        authorization_data.authentication = authentication

        return authorization_data

    def _get_service_client(
        self, service_name: str, account_id: Optional[str] = None
    ) -> ServiceClient:
        """Get a service client for the specified service.

        Args:
            service_name: Name of the service (e.g., 'CampaignManagementService')
            account_id: Optional account ID to use instead of the default

        Returns:
            ServiceClient configured for the specified service.
        """
        # Set account_id on authorization_data if provided
        if account_id:
            self._authorization_data.account_id = int(account_id)
        elif self.account_id:
            self._authorization_data.account_id = int(self.account_id)

        # Set customer_id on authorization_data if available
        if self.customer_id:
            self._authorization_data.customer_id = int(self.customer_id)

        return ServiceClient(
            service=service_name,
            version=13,
            authorization_data=self._authorization_data,
            environment=self.env,
        )

    def list_tables(self) -> list[str]:
        """List names of all tables supported by the connector.

        Returns:
            A list of table names.
        """
        return list(self.SUPPORTED_TABLES.keys())

    def _validate_table_name(self, table_name: str) -> None:
        """Validate that the table name is supported.

        Args:
            table_name: The name of the table to validate.

        Raises:
            ValueError: If the table is not supported.
        """
        if table_name not in self.SUPPORTED_TABLES:
            raise ValueError(
                f"Unsupported table: {table_name!r}. "
                f"Supported tables are: {list(self.SUPPORTED_TABLES.keys())}"
            )

    def get_table_schema(
        self, table_name: str, table_options: dict[str, str]
    ) -> StructType:
        """Fetch the schema of a table.

        Args:
            table_name: The name of the table to fetch the schema for.
            table_options: A dictionary of options for accessing the table.

        Returns:
            A StructType object representing the schema of the table.
        """
        self._validate_table_name(table_name)
        return self.SUPPORTED_TABLES[table_name]["schema"]

    def read_table_metadata(
        self, table_name: str, table_options: dict[str, str]
    ) -> dict[str, Any]:
        """Fetch the metadata of a table.

        Args:
            table_name: The name of the table to fetch the metadata for.
            table_options: A dictionary of options for accessing the table.

        Returns:
            A dictionary containing the metadata of the table.
        """
        self._validate_table_name(table_name)
        table_config = self.SUPPORTED_TABLES[table_name]

        metadata = {
            "primary_keys": table_config["primary_keys"],
            "ingestion_type": table_config["ingestion_type"],
        }

        if table_config["cursor_field"]:
            metadata["cursor_field"] = table_config["cursor_field"]

        return metadata

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read the records of a table.

        Args:
            table_name: The name of the table to read.
            start_offset: The offset to start reading from.
            table_options: A dictionary of options for accessing the table.
                - account_id: The account ID to read data from (required for most tables)
                - start_date: Start date for reports (YYYY-MM-DD format)
                - end_date: End date for reports (YYYY-MM-DD format)
                - lookback_days: Number of days to look back for reports (default: 3)

        Returns:
            An iterator of records and the next offset.
        """
        self._validate_table_name(table_name)

        print(f"Reading table '{table_name}' with options: {table_options}")

        if table_name == "accounts":
            return self._read_accounts(start_offset, table_options)
        elif table_name == "campaigns":
            return self._read_campaigns(start_offset, table_options)
        elif table_name == "ad_groups":
            return self._read_ad_groups(start_offset, table_options)
        elif table_name == "ads":
            return self._read_ads(start_offset, table_options)
        elif table_name == "keywords":
            return self._read_keywords(start_offset, table_options)
        elif table_name == "campaign_performance_report":
            return self._read_campaign_performance_report(start_offset, table_options)
        elif table_name == "ad_group_performance_report":
            return self._read_ad_group_performance_report(start_offset, table_options)
        elif table_name == "ad_performance_report":
            return self._read_ad_performance_report(start_offset, table_options)
        elif table_name == "keyword_performance_report":
            return self._read_keyword_performance_report(start_offset, table_options)

        raise ValueError(f"Unsupported table: {table_name!r}")

    def _get_account_id(self, table_options: dict[str, str]) -> str:
        """Get account ID from table_options or instance config.

        Args:
            table_options: Table options dictionary.

        Returns:
            The account ID.

        Raises:
            ValueError: If account_id is not provided.
        """
        account_id = table_options.get("account_id") or self.account_id
        if not account_id:
            raise ValueError(
                "account_id is required in table_options or connector options"
            )
        return str(account_id)

    def _read_accounts(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read accounts for the authenticated user.

        Args:
            start_offset: The offset to start reading from.
            table_options: A dictionary of options.

        Returns:
            An iterator of account records and an empty offset.
        """
        customer_service = self._get_service_client("CustomerManagementService")

        records: list[dict[str, Any]] = []

        try:
            # Get accounts for the customer
            response = customer_service.GetAccountsInfo(
                CustomerId=self.customer_id,
                OnlyParentAccounts=False,
            )

            account_infos = response.AccountInfo if response.AccountInfo else []

            # For each account info, get full account details
            for account_info in account_infos:
                try:
                    account_response = customer_service.GetAccount(
                        AccountId=account_info.Id
                    )
                    account = account_response

                    record = self._serialize_account(account)
                    records.append(record)
                except Exception as e:
                    # Log but continue processing other accounts
                    print(f"Warning: Failed to get account {account_info.Id}: {e}")

        except Exception as e:
            raise RuntimeError(f"Failed to read accounts: {e}")

        return iter(records), {}

    # -------------------------------------------------------------------------
    # SOAP Serialization Helpers
    # -------------------------------------------------------------------------

    def _get_simple_attr(self, obj: Any, attr: str) -> Any:
        """Safely get a simple attribute value."""
        return getattr(obj, attr, None)

    def _get_enum_attr(self, obj: Any, attr: str) -> Optional[str]:
        """Get an enum attribute as a string, or None if missing."""
        if hasattr(obj, attr):
            val = getattr(obj, attr, None)
            return str(val) if val is not None else None
        return None

    def _get_string_array(self, obj: Any, attr: str) -> Optional[list[str]]:
        """Convert a SOAP string array to a Python list."""
        if hasattr(obj, attr) and getattr(obj, attr):
            arr = getattr(obj, attr)
            if hasattr(arr, "string") and arr.string:
                return list(arr.string)
        return None

    def _get_date_attr(self, obj: Any, attr: str) -> Optional[str]:
        """Convert a SOAP date object to YYYY-MM-DD string."""
        if hasattr(obj, attr) and getattr(obj, attr):
            d = getattr(obj, attr)
            if hasattr(d, "Year") and d.Year:
                return f"{d.Year}-{str(d.Month).zfill(2)}-{str(d.Day).zfill(2)}"
        return None

    def _get_bid_attr(self, obj: Any, attr: str) -> Optional[dict[str, Any]]:
        """Extract a bid object with Amount."""
        if hasattr(obj, attr) and getattr(obj, attr):
            bid = getattr(obj, attr)
            return {"Amount": getattr(bid, "Amount", None)}
        return None

    def _get_bidding_scheme(self, obj: Any, attr: str) -> Optional[dict[str, Any]]:
        """Extract a complex bidding scheme object."""
        if hasattr(obj, attr) and getattr(obj, attr):
            bs = getattr(obj, attr)
            return {
                "Type": getattr(bs, "Type", None),
                "MaxCpc": getattr(getattr(bs, "MaxCpc", None), "Amount", None)
                if hasattr(bs, "MaxCpc") and bs.MaxCpc
                else None,
                "TargetCpa": getattr(bs, "TargetCpa", None)
                if hasattr(bs, "TargetCpa")
                else None,
                "TargetRoas": getattr(bs, "TargetRoas", None)
                if hasattr(bs, "TargetRoas")
                else None,
            }
        return None

    def _get_asset_array(self, obj: Any, attr: str) -> Optional[list[str]]:
        """Extract text from an array of asset links (Headlines, Descriptions)."""
        if hasattr(obj, attr) and getattr(obj, attr):
            arr = getattr(obj, attr)
            if hasattr(arr, "AssetLink") and arr.AssetLink:
                return [a.Text for a in arr.AssetLink if hasattr(a, "Text")]
        return None

    def _get_ad_rotation(self, obj: Any, attr: str) -> Optional[str]:
        """Extract AdRotation.Type as string."""
        if hasattr(obj, attr) and getattr(obj, attr):
            ar = getattr(obj, attr)
            if hasattr(ar, "Type"):
                return str(ar.Type) if ar.Type else None
        return None

    def _serialize_soap_object(
        self,
        obj: Any,
        fields: list[FieldDef],
        extra: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """Serialize a SOAP object to a dictionary using field definitions.

        Args:
            obj: The SOAP object to serialize.
            fields: List of field definitions describing how to extract values.
            extra: Optional static values to include (e.g., foreign keys).

        Returns:
            A dictionary representation of the SOAP object.
        """
        result: dict[str, Any] = {}

        for f in fields:
            attr = f.attr_name
            if f.field_type == SoapFieldType.SIMPLE:
                result[f.name] = self._get_simple_attr(obj, attr)
            elif f.field_type == SoapFieldType.ENUM:
                result[f.name] = self._get_enum_attr(obj, attr)
            elif f.field_type == SoapFieldType.STRING_ARRAY:
                result[f.name] = self._get_string_array(obj, attr)
            elif f.field_type == SoapFieldType.DATE:
                result[f.name] = self._get_date_attr(obj, attr)
            elif f.field_type == SoapFieldType.BID:
                result[f.name] = self._get_bid_attr(obj, attr)
            elif f.field_type == SoapFieldType.BIDDING_SCHEME:
                result[f.name] = self._get_bidding_scheme(obj, attr)
            elif f.field_type == SoapFieldType.ASSET_ARRAY:
                result[f.name] = self._get_asset_array(obj, attr)
            elif f.field_type == SoapFieldType.AD_ROTATION:
                result[f.name] = self._get_ad_rotation(obj, attr)

        if extra:
            result.update(extra)

        print("_serialize_soap_object", result)

        return result

    # -------------------------------------------------------------------------
    # Entity Serializers
    # -------------------------------------------------------------------------

    def _serialize_account(self, account: Any) -> dict[str, Any]:
        """Serialize an Account object to a dictionary.

        Args:
            account: The Account object from the API.

        Returns:
            A dictionary representation of the account.
        """
        return self._serialize_soap_object(account, self._SOAP_FIELDS["accounts"])

    def _read_campaigns(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read campaigns for an account.

        Args:
            start_offset: The offset to start reading from.
            table_options: A dictionary of options.
                - account_id: Required. The account ID to read campaigns from.

        Returns:
            An iterator of campaign records and an empty offset.
        """
        account_id = self._get_account_id(table_options)
        campaign_service = self._get_service_client(
            "CampaignManagementService", account_id
        )

        records: list[dict[str, Any]] = []

        try:
            # Get all campaign types
            campaign_types = ["Search", "Shopping", "Audience", "DynamicSearchAds"]

            response = campaign_service.GetCampaignsByAccountId(
                AccountId=int(account_id),
                CampaignType=" ".join(campaign_types),
            )

            # Response is ArrayOfCampaign with Campaign attribute directly
            campaigns = response.Campaign if hasattr(response, 'Campaign') and response.Campaign else []

            for campaign in campaigns:
                record = self._serialize_campaign(campaign, int(account_id))
                records.append(record)

        except Exception as e:
            raise RuntimeError(f"Failed to read campaigns: {e}")

        return iter(records), {}

    def _serialize_campaign(
        self, campaign: Any, account_id: int
    ) -> dict[str, Any]:
        """Serialize a Campaign object to a dictionary.

        Args:
            campaign: The Campaign object from the API.
            account_id: The account ID this campaign belongs to.

        Returns:
            A dictionary representation of the campaign.
        """
        return self._serialize_soap_object(
            campaign, self._SOAP_FIELDS["campaigns"], extra={"AccountId": account_id}
        )

    def _read_ad_groups(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read ad groups for an account.

        Args:
            start_offset: The offset to start reading from.
            table_options: A dictionary of options.
                - account_id: Required. The account ID to read ad groups from.
                - campaign_id: Optional. Filter by specific campaign.

        Returns:
            An iterator of ad group records and an empty offset.
        """
        account_id = self._get_account_id(table_options)
        campaign_id = table_options.get("campaign_id")
        campaign_service = self._get_service_client(
            "CampaignManagementService", account_id
        )

        records: list[dict[str, Any]] = []

        try:
            # If campaign_id specified, get ad groups for that campaign only
            if campaign_id:
                campaign_ids = [int(campaign_id)]
            else:
                # Get all campaigns first, then get ad groups for each
                campaign_types = ["Search", "Shopping", "Audience", "DynamicSearchAds"]
                campaigns_response = campaign_service.GetCampaignsByAccountId(
                    AccountId=int(account_id),
                    CampaignType=" ".join(campaign_types),
                )
                # Response is ArrayOfCampaign with Campaign attribute directly
                campaigns = (
                    campaigns_response.Campaign
                    if hasattr(campaigns_response, 'Campaign') and campaigns_response.Campaign
                    else []
                )
                campaign_ids = [c.Id for c in campaigns]

            # Get ad groups for each campaign
            for cid in campaign_ids:
                try:
                    response = campaign_service.GetAdGroupsByCampaignId(CampaignId=cid)
                    # Response is ArrayOfAdGroup with AdGroup attribute directly
                    ad_groups = response.AdGroup if hasattr(response, 'AdGroup') and response.AdGroup else []

                    for ad_group in ad_groups:
                        record = self._serialize_ad_group(
                            ad_group, int(account_id), cid
                        )
                        records.append(record)
                except Exception as e:
                    print(f"Warning: Failed to get ad groups for campaign {cid}: {e}")

        except Exception as e:
            raise RuntimeError(f"Failed to read ad groups: {e}")

        return iter(records), {}

    def _serialize_ad_group(
        self, ad_group: Any, account_id: int, campaign_id: int
    ) -> dict[str, Any]:
        """Serialize an AdGroup object to a dictionary.

        Args:
            ad_group: The AdGroup object from the API.
            account_id: The account ID.
            campaign_id: The campaign ID.

        Returns:
            A dictionary representation of the ad group.
        """
        return self._serialize_soap_object(
            ad_group,
            self._SOAP_FIELDS["ad_groups"],
            extra={"CampaignId": campaign_id, "AccountId": account_id},
        )

    def _read_ads(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read ads for an account.

        Args:
            start_offset: The offset to start reading from.
            table_options: A dictionary of options.
                - account_id: Required. The account ID to read ads from.
                - ad_group_id: Optional. Filter by specific ad group.

        Returns:
            An iterator of ad records and an empty offset.
        """
        account_id = self._get_account_id(table_options)
        ad_group_id = table_options.get("ad_group_id")
        campaign_service = self._get_service_client(
            "CampaignManagementService", account_id
        )

        records: list[dict[str, Any]] = []

        try:
            # Get all ad group IDs if not specified
            if ad_group_id:
                ad_group_ids = [(int(ad_group_id), None, None)]  # (ad_group_id, campaign_id, account_id)
            else:
                # Get all campaigns, then all ad groups
                ad_group_ids = []
                campaign_types = ["Search", "Shopping", "Audience", "DynamicSearchAds"]
                campaigns_response = campaign_service.GetCampaignsByAccountId(
                    AccountId=int(account_id),
                    CampaignType=" ".join(campaign_types),
                )
                # Response is ArrayOfCampaign with Campaign attribute directly
                campaigns = (
                    campaigns_response.Campaign
                    if hasattr(campaigns_response, 'Campaign') and campaigns_response.Campaign
                    else []
                )

                for campaign in campaigns:
                    try:
                        ad_groups_response = campaign_service.GetAdGroupsByCampaignId(
                            CampaignId=campaign.Id
                        )
                        # Response is ArrayOfAdGroup with AdGroup attribute directly
                        ad_groups = (
                            ad_groups_response.AdGroup
                            if hasattr(ad_groups_response, 'AdGroup') and ad_groups_response.AdGroup
                            else []
                        )
                        for ag in ad_groups:
                            ad_group_ids.append((ag.Id, campaign.Id, int(account_id)))
                    except Exception as e:
                        print(
                            f"Warning: Failed to get ad groups for campaign {campaign.Id}: {e}"
                        )

            # Get ads for each ad group
            for ag_id, cid, acc_id in ad_group_ids:
                try:
                    response = campaign_service.GetAdsByAdGroupId(
                        AdGroupId=ag_id,
                        AdTypes={"AdType": ["Text", "ExpandedText", "ResponsiveSearch", "ResponsiveAd"]},
                    )
                    # Response is ArrayOfAd with Ad attribute directly
                    ads = response.Ad if hasattr(response, 'Ad') and response.Ad else []

                    for ad in ads:
                        record = self._serialize_ad(ad, acc_id or int(account_id), cid, ag_id)
                        records.append(record)
                except Exception as e:
                    print(f"Warning: Failed to get ads for ad group {ag_id}: {e}")

        except Exception as e:
            raise RuntimeError(f"Failed to read ads: {e}")

        return iter(records), {}

    def _serialize_ad(
        self, ad: Any, account_id: int, campaign_id: Optional[int], ad_group_id: int
    ) -> dict[str, Any]:
        """Serialize an Ad object to a dictionary.

        Args:
            ad: The Ad object from the API.
            account_id: The account ID.
            campaign_id: The campaign ID.
            ad_group_id: The ad group ID.

        Returns:
            A dictionary representation of the ad.
        """
        return self._serialize_soap_object(
            ad,
            self._SOAP_FIELDS["ads"],
            extra={
                "AdGroupId": ad_group_id,
                "AccountId": account_id,
                "CampaignId": campaign_id,
            },
        )

    def _read_keywords(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read keywords for an account.

        Args:
            start_offset: The offset to start reading from.
            table_options: A dictionary of options.
                - account_id: Required. The account ID to read keywords from.
                - ad_group_id: Optional. Filter by specific ad group.

        Returns:
            An iterator of keyword records and an empty offset.
        """
        account_id = self._get_account_id(table_options)
        ad_group_id = table_options.get("ad_group_id")
        campaign_service = self._get_service_client(
            "CampaignManagementService", account_id
        )

        records: list[dict[str, Any]] = []

        try:
            # Get all ad group IDs if not specified
            if ad_group_id:
                ad_group_ids = [(int(ad_group_id), None, None)]
            else:
                # Get all campaigns, then all ad groups
                ad_group_ids = []
                campaign_types = ["Search", "Shopping", "Audience", "DynamicSearchAds"]
                campaigns_response = campaign_service.GetCampaignsByAccountId(
                    AccountId=int(account_id),
                    CampaignType=" ".join(campaign_types),
                )
                # Response is ArrayOfCampaign with Campaign attribute directly
                campaigns = (
                    campaigns_response.Campaign
                    if hasattr(campaigns_response, 'Campaign') and campaigns_response.Campaign
                    else []
                )

                for campaign in campaigns:
                    try:
                        ad_groups_response = campaign_service.GetAdGroupsByCampaignId(
                            CampaignId=campaign.Id
                        )
                        # Response is ArrayOfAdGroup with AdGroup attribute directly
                        ad_groups = (
                            ad_groups_response.AdGroup
                            if hasattr(ad_groups_response, 'AdGroup') and ad_groups_response.AdGroup
                            else []
                        )
                        for ag in ad_groups:
                            ad_group_ids.append((ag.Id, campaign.Id, int(account_id)))
                    except Exception as e:
                        print(
                            f"Warning: Failed to get ad groups for campaign {campaign.Id}: {e}"
                        )

            # Get keywords for each ad group
            for ag_id, cid, acc_id in ad_group_ids:
                try:
                    response = campaign_service.GetKeywordsByAdGroupId(AdGroupId=ag_id)
                    # Response is ArrayOfKeyword with Keyword attribute directly
                    keywords = response.Keyword if hasattr(response, 'Keyword') and response.Keyword else []

                    for keyword in keywords:
                        record = self._serialize_keyword(
                            keyword, acc_id or int(account_id), cid, ag_id
                        )
                        records.append(record)
                except Exception as e:
                    print(f"Warning: Failed to get keywords for ad group {ag_id}: {e}")

        except Exception as e:
            raise RuntimeError(f"Failed to read keywords: {e}")

        return iter(records), {}

    def _serialize_keyword(
        self,
        keyword: Any,
        account_id: int,
        campaign_id: Optional[int],
        ad_group_id: int,
    ) -> dict[str, Any]:
        """Serialize a Keyword object to a dictionary.

        Args:
            keyword: The Keyword object from the API.
            account_id: The account ID.
            campaign_id: The campaign ID.
            ad_group_id: The ad group ID.

        Returns:
            A dictionary representation of the keyword.
        """
        return self._serialize_soap_object(
            keyword,
            self._SOAP_FIELDS["keywords"],
            extra={
                "AdGroupId": ad_group_id,
                "AccountId": account_id,
                "CampaignId": campaign_id,
            },
        )

    def _get_report_date_range(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[datetime, datetime]:
        """Get the date range for report requests.

        Args:
            start_offset: The offset containing cursor information.
            table_options: A dictionary of options.
                - start_date: Start date (YYYY-MM-DD format)
                - end_date: End date (YYYY-MM-DD format)
                - lookback_days: Number of days to look back (default: 3)

        Returns:
            A tuple of (start_date, end_date).
        """
        today = datetime.now().date()
        lookback_days = int(table_options.get("lookback_days", 3))

        # Determine start date
        if start_offset and start_offset.get("cursor"):
            # Use cursor from previous run with lookback
            cursor_date = datetime.strptime(start_offset["cursor"], "%Y-%m-%d").date()
            start_date = cursor_date - timedelta(days=lookback_days)
        elif table_options.get("start_date"):
            start_date = datetime.strptime(
                table_options["start_date"], "%Y-%m-%d"
            ).date()
        else:
            # Default to 30 days ago
            start_date = today - timedelta(days=30)

        # Determine end date
        if table_options.get("end_date"):
            end_date = datetime.strptime(table_options["end_date"], "%Y-%m-%d").date()
        else:
            # Default to yesterday (today's data may be incomplete)
            end_date = today - timedelta(days=1)

        # Ensure start_date is not after end_date
        if start_date > end_date:
            start_date = end_date

        return start_date, end_date

    def _read_campaign_performance_report(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read campaign performance report.

        Args:
            start_offset: The offset to start reading from.
            table_options: A dictionary of options.
                - account_id: Required. The account ID to read reports from.
                - start_date: Start date (YYYY-MM-DD format)
                - end_date: End date (YYYY-MM-DD format)
                - lookback_days: Number of days to look back (default: 3)

        Returns:
            An iterator of report records and the next offset.
        """
        return self._read_performance_report(
            report_type="CampaignPerformanceReport",
            columns=[
                "TimePeriod",
                "AccountId",
                "AccountName",
                "CampaignId",
                "CampaignName",
                "CampaignStatus",
                "Impressions",
                "Clicks",
                "Ctr",
                "AverageCpc",
                "Spend",
                "Conversions",
                "ConversionRate",
                "CostPerConversion",
                "Revenue",
                "ReturnOnAdSpend",
            ],
            start_offset=start_offset,
            table_options=table_options,
        )

    def _read_ad_group_performance_report(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read ad group performance report.

        Args:
            start_offset: The offset to start reading from.
            table_options: A dictionary of options.

        Returns:
            An iterator of report records and the next offset.
        """
        return self._read_performance_report(
            report_type="AdGroupPerformanceReport",
            columns=[
                "TimePeriod",
                "AccountId",
                "AccountName",
                "CampaignId",
                "CampaignName",
                "AdGroupId",
                "AdGroupName",
                "Impressions",
                "Clicks",
                "Ctr",
                "AverageCpc",
                "Spend",
                "Conversions",
                "ConversionRate",
                "CostPerConversion",
                "Revenue",
                "ReturnOnAdSpend",
            ],
            start_offset=start_offset,
            table_options=table_options,
        )

    def _read_ad_performance_report(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read ad performance report.

        Args:
            start_offset: The offset to start reading from.
            table_options: A dictionary of options.

        Returns:
            An iterator of report records and the next offset.
        """
        return self._read_performance_report(
            report_type="AdPerformanceReport",
            columns=[
                "TimePeriod",
                "AccountId",
                "AccountName",
                "CampaignId",
                "CampaignName",
                "AdGroupId",
                "AdGroupName",
                "AdId",
                "AdTitle",
                "AdStatus",
                "AdType",
                "Impressions",
                "Clicks",
                "Ctr",
                "AverageCpc",
                "Spend",
                "Conversions",
                "ConversionRate",
                "CostPerConversion",
                "Revenue",
                "ReturnOnAdSpend",
            ],
            start_offset=start_offset,
            table_options=table_options,
        )

    def _read_keyword_performance_report(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read keyword performance report.

        Args:
            start_offset: The offset to start reading from.
            table_options: A dictionary of options.

        Returns:
            An iterator of report records and the next offset.
        """
        return self._read_performance_report(
            report_type="KeywordPerformanceReport",
            columns=[
                "TimePeriod",
                "AccountId",
                "AccountName",
                "CampaignId",
                "CampaignName",
                "AdGroupId",
                "AdGroupName",
                "KeywordId",
                "Keyword",
                "BidMatchType",
                "Impressions",
                "Clicks",
                "Ctr",
                "AverageCpc",
                "Spend",
                "QualityScore",
                "Conversions",
                "ConversionRate",
                "CostPerConversion",
                "Revenue",
                "ReturnOnAdSpend",
            ],
            start_offset=start_offset,
            table_options=table_options,
        )

    def _read_performance_report(
        self,
        report_type: str,
        columns: list[str],
        start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Read a performance report from the Reporting API.

        Args:
            report_type: The type of report (e.g., 'CampaignPerformanceReport')
            columns: List of column names to include in the report.
            start_offset: The offset to start reading from.
            table_options: A dictionary of options.

        Returns:
            An iterator of report records and the next offset.
        """
        account_id = self._get_account_id(table_options)
        start_date, end_date = self._get_report_date_range(start_offset, table_options)

        reporting_service = self._get_service_client("ReportingService", account_id)

        records: list[dict[str, Any]] = []

        try:
            # Build the report request
            report_request = self._build_report_request(
                reporting_service,
                report_type,
                columns,
                int(account_id),
                start_date,
                end_date,
            )

            # Submit the report request
            report_request_id = reporting_service.SubmitGenerateReport(
                ReportRequest=report_request
            )

            # Poll for completion
            report_status = self._poll_report_status(
                reporting_service, report_request_id
            )

            if report_status.Status == "Success":
                # Download and parse the report
                if report_status.ReportDownloadUrl:
                    records = self._download_and_parse_report(
                        report_status.ReportDownloadUrl, columns
                    )

        except Exception as e:
            raise RuntimeError(f"Failed to read {report_type}: {e}")

        # Calculate next offset
        max_date = None
        for record in records:
            time_period = record.get("TimePeriod")
            if time_period:
                try:
                    record_date = datetime.strptime(time_period, "%Y-%m-%d").date()
                    if max_date is None or record_date > max_date:
                        max_date = record_date
                except ValueError:
                    pass

        if max_date:
            next_offset = {"cursor": max_date.strftime("%Y-%m-%d")}
        elif start_offset:
            next_offset = start_offset
        else:
            next_offset = {"cursor": end_date.strftime("%Y-%m-%d")}

        return iter(records), next_offset

    def _build_report_request(
        self,
        reporting_service: ServiceClient,
        report_type: str,
        columns: list[str],
        account_id: int,
        start_date: datetime,
        end_date: datetime,
    ) -> Any:
        """Build a report request object.

        Args:
            reporting_service: The ReportingService client.
            report_type: The type of report.
            columns: List of column names.
            account_id: The account ID.
            start_date: Start date for the report.
            end_date: End date for the report.

        Returns:
            A report request object.
        """
        # Create the report request using the service's factory
        report_request = reporting_service.factory.create(f"{report_type}Request")

        # Set common report settings
        report_request.ExcludeColumnHeaders = False
        report_request.ExcludeReportFooter = True
        report_request.ExcludeReportHeader = True
        report_request.Format = "Csv"
        report_request.FormatVersion = "2.0"
        report_request.ReportName = f"{report_type}"
        report_request.ReturnOnlyCompleteData = False
        report_request.Aggregation = "Daily"

        # Set columns based on report type
        column_type = f"{report_type}Column"
        columns_container = reporting_service.factory.create(f"ArrayOf{column_type}")
        columns_container[column_type] = columns
        report_request.Columns = columns_container

        # Set scope - assign account IDs to the ArrayOflong.long attribute
        scope = reporting_service.factory.create("AccountThroughAdGroupReportScope")
        scope.AccountIds.long = [account_id]
        report_request.Scope = scope

        # Set time period - explicitly set PredefinedTime to None for custom dates
        time_period = reporting_service.factory.create("ReportTime")
        time_period.PredefinedTime = None
        time_period.ReportTimeZone = None

        custom_start = reporting_service.factory.create("Date")
        custom_start.Day = start_date.day
        custom_start.Month = start_date.month
        custom_start.Year = start_date.year
        time_period.CustomDateRangeStart = custom_start

        custom_end = reporting_service.factory.create("Date")
        custom_end.Day = end_date.day
        custom_end.Month = end_date.month
        custom_end.Year = end_date.year
        time_period.CustomDateRangeEnd = custom_end

        report_request.Time = time_period

        return report_request

    def _poll_report_status(
        self,
        reporting_service: ServiceClient,
        report_request_id: str,
        timeout_seconds: int = 300,
        poll_interval_seconds: int = 5,
    ) -> Any:
        """Poll for report generation status.

        Args:
            reporting_service: The ReportingService client.
            report_request_id: The report request ID.
            timeout_seconds: Maximum time to wait for report completion.
            poll_interval_seconds: Interval between status checks.

        Returns:
            The report request status object.
        """
        start_time = time.time()

        while True:
            status = reporting_service.PollGenerateReport(
                ReportRequestId=report_request_id
            )

            if status.Status in ["Success", "Error"]:
                return status

            if time.time() - start_time > timeout_seconds:
                raise RuntimeError(
                    f"Report generation timed out after {timeout_seconds} seconds"
                )

            time.sleep(poll_interval_seconds)

    def _download_and_parse_report(
        self, download_url: str, columns: list[str]
    ) -> list[dict[str, Any]]:
        """Download and parse a report CSV file.

        Args:
            download_url: URL to download the report from.
            columns: List of expected column names.

        Returns:
            A list of parsed report records.
        """
        import csv
        import io
        import zipfile

        import requests

        records: list[dict[str, Any]] = []

        try:
            # Download the report file
            response = requests.get(download_url, timeout=60)
            response.raise_for_status()

            # The report is typically a zip file containing a CSV
            with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
                for filename in zip_file.namelist():
                    if filename.endswith(".csv"):
                        with zip_file.open(filename) as csv_file:
                            # Read CSV content
                            content = csv_file.read().decode("utf-8-sig")
                            reader = csv.DictReader(io.StringIO(content))

                            for row in reader:
                                record = self._parse_report_row(row, columns)
                                records.append(record)

        except Exception as e:
            raise RuntimeError(f"Failed to download/parse report: {e}")

        return records

    def _parse_report_row(
        self, row: dict[str, str], columns: list[str]
    ) -> dict[str, Any]:
        """Parse a single report row, converting types appropriately.

        Args:
            row: The raw CSV row as a dictionary.
            columns: List of expected column names.

        Returns:
            A dictionary with properly typed values.
        """
        record: dict[str, Any] = {}

        # Type mapping for report columns
        long_columns = {
            "AccountId",
            "CampaignId",
            "AdGroupId",
            "AdId",
            "KeywordId",
            "Impressions",
            "Clicks",
            "Conversions",
            "QualityScore",
        }
        double_columns = {
            "Ctr",
            "AverageCpc",
            "Spend",
            "ConversionRate",
            "CostPerConversion",
            "Revenue",
            "ReturnOnAdSpend",
        }

        for col in columns:
            value = row.get(col, "")

            if value == "" or value is None:
                record[col] = None
            elif col in long_columns:
                try:
                    record[col] = int(float(value))
                except (ValueError, TypeError):
                    record[col] = None
            elif col in double_columns:
                try:
                    # Remove percentage signs if present
                    clean_value = value.replace("%", "").strip()
                    record[col] = float(clean_value)
                except (ValueError, TypeError):
                    record[col] = None
            else:
                record[col] = value

        return record
