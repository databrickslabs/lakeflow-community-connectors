"""Fixed schema and reporting-field contract for Google CM360."""

from dataclasses import dataclass

from pyspark.sql.types import (
    DateType,
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


@dataclass(frozen=True)
class FieldSpec:
    """Map one CM360 report field to its CSV and connector representations."""

    api_name: str
    csv_header: str
    output_name: str
    kind: str


FIELD_SPECS = (
    FieldSpec("country", "Country", "delivery_country", "string"),
    FieldSpec("advertiserId", "Advertiser ID", "advertiser_id", "string"),
    FieldSpec("advertiser", "Advertiser", "advertiser_name", "string"),
    FieldSpec("campaignId", "Campaign ID", "campaign_id", "string"),
    FieldSpec("campaign", "Campaign", "campaign_name", "string"),
    FieldSpec("impressions", "Impressions", "impressions", "integer"),
    FieldSpec("clicks", "Clicks", "clicks", "integer"),
    FieldSpec("mediaCost", "Media Cost", "media_cost", "decimal"),
    FieldSpec("richMediaVideoPlays", "Video Plays", "video_starts", "integer"),
    FieldSpec(
        "richMediaVideoViews",
        "Video Views",
        "qualified_video_views",
        "integer",
    ),
    FieldSpec(
        "richMediaVideoMidpoints",
        "Video Mid-points",
        "video_views_50_percent",
        "integer",
    ),
    FieldSpec(
        "richMediaVideoCompletions",
        "Video Completions",
        "video_views_100_percent",
        "integer",
    ),
)

DIMENSION_API_NAMES = (
    "country",
    "advertiserId",
    "advertiser",
    "campaignId",
    "campaign",
)
METRIC_API_NAMES = tuple(
    field.api_name for field in FIELD_SPECS if field.api_name not in DIMENSION_API_NAMES
)


AD_REPORT_DAILY_SCHEMA = StructType(
    [
        StructField("profile_id", StringType(), nullable=False),
        StructField("account_id", StringType(), nullable=False),
        StructField("account_name", StringType(), nullable=False),
        StructField("subaccount_id", StringType(), nullable=True),
        StructField("subaccount_name", StringType(), nullable=True),
        StructField("report_date", DateType(), nullable=False),
        StructField("delivery_country", StringType(), nullable=False),
        StructField("advertiser_id", StringType(), nullable=False),
        StructField("advertiser_name", StringType(), nullable=False),
        StructField("campaign_id", StringType(), nullable=False),
        StructField("campaign_name", StringType(), nullable=True),
        StructField("impressions", LongType(), nullable=True),
        StructField("clicks", LongType(), nullable=True),
        StructField("media_cost", DecimalType(38, 9), nullable=True),
        StructField("video_starts", LongType(), nullable=True),
        StructField("qualified_video_views", LongType(), nullable=True),
        StructField("video_views_50_percent", LongType(), nullable=True),
        StructField("video_views_100_percent", LongType(), nullable=True),
        StructField("report_file_last_modified_at", TimestampType(), nullable=False),
    ]
)


AD_REPORT_DAILY_METADATA = {
    "primary_keys": [
        "profile_id",
        "report_date",
        "delivery_country",
        "advertiser_id",
        "campaign_id",
    ],
    "cursor_field": "report_file_last_modified_at",
    "ingestion_type": "cdc",
}


REACH_FIELD_SPECS = (
    FieldSpec("advertiser", "Advertiser", "advertiser_name", "string"),
    FieldSpec("advertiserId", "Advertiser ID", "advertiser_id", "string"),
    FieldSpec("campaign", "Campaign", "campaign_name", "string"),
    FieldSpec("campaignId", "Campaign ID", "campaign_id", "string"),
    FieldSpec("country", "Country", "delivery_country", "string"),
    FieldSpec("date", "Date", "report_date", "date"),
    FieldSpec(
        "uniqueReachImpressionReach",
        "Unique Reach: Impression Reach",
        "unique_reach_impression_reach",
        "string",
    ),
)
REACH_DIMENSION_API_NAMES = (
    "advertiser",
    "advertiserId",
    "campaign",
    "campaignId",
    "country",
    "date",
)
REACH_METRIC_API_NAMES = ("uniqueReachImpressionReach",)


REACH_REPORT_DAILY_SCHEMA = StructType(
    [
        StructField("profile_id", StringType(), nullable=False),
        StructField("account_id", StringType(), nullable=False),
        StructField("account_name", StringType(), nullable=False),
        StructField("subaccount_id", StringType(), nullable=True),
        StructField("subaccount_name", StringType(), nullable=True),
        StructField("report_date", DateType(), nullable=False),
        StructField("delivery_country", StringType(), nullable=False),
        StructField("advertiser_id", StringType(), nullable=False),
        StructField("advertiser_name", StringType(), nullable=False),
        StructField("campaign_id", StringType(), nullable=False),
        StructField("campaign_name", StringType(), nullable=True),
        StructField("unique_reach_impression_reach", StringType(), nullable=True),
        StructField("report_file_last_modified_at", TimestampType(), nullable=False),
    ]
)


REACH_REPORT_DAILY_METADATA = {
    "primary_keys": [
        "profile_id",
        "report_date",
        "delivery_country",
        "advertiser_id",
        "campaign_id",
    ],
    "cursor_field": "report_file_last_modified_at",
    "ingestion_type": "cdc",
}


# Placement-level ad report: advertiser x campaign x placement, no country
# dimension (markets are assigned downstream by advertiser). Total cost is
# media_cost + dv360_cost and is computed downstream.
PLACEMENT_FIELD_SPECS = (
    FieldSpec("advertiserId", "Advertiser ID", "advertiser_id", "string"),
    FieldSpec("advertiser", "Advertiser", "advertiser_name", "string"),
    FieldSpec("campaignId", "Campaign ID", "campaign_id", "string"),
    FieldSpec("campaign", "Campaign", "campaign_name", "string"),
    FieldSpec("placementId", "Placement ID", "placement_id", "string"),
    FieldSpec("placement", "Placement", "placement_name", "string"),
    *(field for field in FIELD_SPECS if field.api_name not in DIMENSION_API_NAMES),
    FieldSpec("dv360Cost", "DV360 cost (Account currency)", "dv360_cost", "decimal"),
)
PLACEMENT_DIMENSION_API_NAMES = (
    "advertiserId",
    "advertiser",
    "campaignId",
    "campaign",
    "placementId",
    "placement",
)
PLACEMENT_METRIC_API_NAMES = tuple(
    field.api_name
    for field in PLACEMENT_FIELD_SPECS
    if field.api_name not in PLACEMENT_DIMENSION_API_NAMES
)


AD_REPORT_PLACEMENT_DAILY_SCHEMA = StructType(
    [
        StructField("profile_id", StringType(), nullable=False),
        StructField("account_id", StringType(), nullable=False),
        StructField("account_name", StringType(), nullable=False),
        StructField("subaccount_id", StringType(), nullable=True),
        StructField("subaccount_name", StringType(), nullable=True),
        StructField("report_date", DateType(), nullable=False),
        StructField("advertiser_id", StringType(), nullable=False),
        StructField("advertiser_name", StringType(), nullable=False),
        StructField("campaign_id", StringType(), nullable=False),
        StructField("campaign_name", StringType(), nullable=True),
        StructField("placement_id", StringType(), nullable=False),
        StructField("placement_name", StringType(), nullable=True),
        StructField("impressions", LongType(), nullable=True),
        StructField("clicks", LongType(), nullable=True),
        StructField("media_cost", DecimalType(38, 9), nullable=True),
        StructField("dv360_cost", DecimalType(38, 9), nullable=True),
        StructField("video_starts", LongType(), nullable=True),
        StructField("qualified_video_views", LongType(), nullable=True),
        StructField("video_views_50_percent", LongType(), nullable=True),
        StructField("video_views_100_percent", LongType(), nullable=True),
        StructField("report_file_last_modified_at", TimestampType(), nullable=False),
    ]
)


AD_REPORT_PLACEMENT_DAILY_METADATA = {
    "primary_keys": [
        "profile_id",
        "report_date",
        "advertiser_id",
        "campaign_id",
        "placement_id",
    ],
    "cursor_field": "report_file_last_modified_at",
    "ingestion_type": "cdc",
}

# Native cumulative reach retains empty, suppressed and numeric strings separately.
PERIOD_REACH_FIELD_SPECS = tuple(
    FieldSpec(
        f.api_name,
        f.csv_header,
        f.output_name,
        "native_string" if f.output_name == "unique_reach_impression_reach" else f.kind,
    )
    for f in REACH_FIELD_SPECS
    if f.api_name != "date"
)
CAMPAIGN_PERIOD_FIELD_SPECS = (
    *PERIOD_REACH_FIELD_SPECS[:-1],
    FieldSpec("campaignStartDate", "Campaign Start Date", "campaign_start_date", "string"),
    FieldSpec("campaignEndDate", "Campaign End Date", "campaign_end_date", "string"),
    FieldSpec("impressions", "Impressions", "impressions", "integer"),
)
REACH_REPORT_PERIOD_SCHEMA = StructType(
    [
        *[f for f in REACH_REPORT_DAILY_SCHEMA.fields if f.name != "report_date"],
        StructField("campaign_start_date", DateType(), True),
        StructField("campaign_end_date", DateType(), True),
        StructField("period_start", DateType(), True),
        StructField("period_end", DateType(), True),
        StructField("source_cutoff_date", DateType(), False),
        StructField("metadata_anchor", DateType(), False),
        StructField("reporting_timezone", StringType(), False),
        StructField("coverage_status", StringType(), False),
    ]
)
REACH_REPORT_PERIOD_METADATA = {
    "primary_keys": ["profile_id", "delivery_country", "advertiser_id", "campaign_id"],
    "ingestion_type": "snapshot",
}
