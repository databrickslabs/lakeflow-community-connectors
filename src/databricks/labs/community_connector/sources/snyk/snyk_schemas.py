from pyspark.sql.types import *

SUPPORTED_TABLES = ["organizations", "projects", "issues", "targets", "users", "vulnerabilities"]

TABLE_SCHEMAS = {
    "organizations": StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("slug", StringType(), True),
        StructField("url", StringType(), True),
    ]),
    "projects": StructType([
        StructField("id", StringType(), False),
        StructField("type", StringType(), True),
        StructField("name", StringType(), True),
        StructField("status", StringType(), True),
        StructField("origin", StringType(), True),
        StructField("created", StringType(), True),
        StructField("organization_id", StringType(), True),
    ]),
    "issues": StructType([
        StructField("id", StringType(), False),
        StructField("type", StringType(), True),
        StructField("title", StringType(), True),
        StructField("status", StringType(), True),
        StructField("effective_severity_level", StringType(), True),
        StructField("ignored", BooleanType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("organization_id", StringType(), True),
        StructField("scan_item_id", StringType(), True),
        StructField("tool", StringType(), True),
    ]),
    "targets": StructType([
        StructField("id", StringType(), False),
        StructField("type", StringType(), True),
        StructField("display_name", StringType(), True),
        StructField("url", StringType(), True),
        StructField("organization_id", StringType(), True),
    ]),
    "users": StructType([
        StructField("id", StringType(), False),
        StructField("type", StringType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("username", StringType(), True),
        StructField("organization_id", StringType(), True),
    ]),
    "vulnerabilities": StructType([
        StructField("id", StringType(), False),
        StructField("type", StringType(), True),
        StructField("title", StringType(), True),
        StructField("severity", StringType(), True),
        StructField("status", StringType(), True),
        StructField("cve", StringType(), True),
        StructField("cvss_score", DoubleType(), True),
        StructField("package", StringType(), True),
        StructField("version", StringType(), True),
        StructField("fixed_in", StringType(), True),
        StructField("disclosure_time", StringType(), True),
        StructField("publication_time", StringType(), True),
        StructField("organization_id", StringType(), True),
        StructField("project_id", StringType(), True),
    ]),
}

TABLE_METADATA = {
    "organizations": {
        "ingestion_type": "snapshot",
        "primary_keys": ["id"],
    },
    "projects": {
        "ingestion_type": "snapshot",
        "primary_keys": ["id"],
    },
    "issues": {
        "ingestion_type": "cdc",
        "primary_keys": ["id"],
        "cursor_field": "updated_at",
    },
    "targets": {
        "ingestion_type": "snapshot",
        "primary_keys": ["id"],
    },
    "users": {
        "ingestion_type": "snapshot",
        "primary_keys": ["id"],
    },
    "vulnerabilities": {
        "ingestion_type": "snapshot",
        "primary_keys": ["id"],
    },
}