"""Static schema definitions for Employment Hero connector tables.

This module contains all Spark StructType schema definitions and table metadata
for the Employment Hero Lakeflow connector. These are derived from the
Employment Hero REST API documentation.
"""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)


# =============================================================================
# Reusable Nested Struct Definitions
# =============================================================================

"""Nested entity reference struct schema used across multiple tables."""
REFERENCE_STRUCT = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
    ]
)

"""Nested address struct schema used across multiple tables (e.g. residential_address, postal_address)."""
ADDRESS_STRUCT = StructType(
    [
        StructField("address_type", StringType(), True),
        StructField("line_1", StringType(), True),
        StructField("line_2", StringType(), True),
        StructField("line_3", StringType(), True),
        StructField("block_number", StringType(), True),
        StructField("level_number", StringType(), True),
        StructField("unit_number", StringType(), True),
        StructField("city", StringType(), True),
        StructField("postcode", StringType(), True),
        StructField("state", StringType(), True),
        StructField("suburb", StringType(), True),
        StructField("country", StringType(), True),
        StructField("is_residential", BooleanType(), True),
        StructField("street_name", StringType(), True),
        StructField("is_manually_entered", BooleanType(), True),
    ]
)

"""Nested business detail struct schema used across multiple tables."""
BUSINESS_DETAIL_STRUCT = StructType(
    [
        StructField("country", StringType(), True),
        StructField("number", StringType(), True),
        StructField("business_type", StringType(), True),
    ]
)

"""Nested tax and national insurance struct schema used across multiple tables."""
TAX_AND_NATIONAL_INSURANCE_STRUCT = StructType(
    [
        StructField("id", StringType(), True),
        StructField("unique_taxpayer_reference", StringType(), True),
        StructField("national_insurance_number", StringType(), True),
    ]
)

"""Nested struct for custom field permission (custom_field_permissions array)."""
CUSTOM_FIELD_PERMISSION_STRUCT = StructType(
    [
        StructField("id", StringType(), True),
        StructField("permission", StringType(), True),
        StructField("role", StringType(), True),
    ]
)

"""Nested struct for custom field option (custom_field_options array)."""
CUSTOM_FIELD_OPTION_STRUCT = StructType(
    [
        StructField("id", StringType(), True),
        StructField("value", StringType(), True),
    ]
)

"""Nested struct for department in work site (departments array)."""
DEPARTMENT_STRUCT = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("assignment_count", LongType(), True),
    ]
)

"""Nested struct for HR position in work site (hr_positions array)."""
HR_POSITION_STRUCT = StructType(
    [
        StructField("id", StringType(), True),
        StructField("status", StringType(), True),
        StructField("roster_positions_count", LongType(), True),
        StructField("cost_centre", REFERENCE_STRUCT, True),
        StructField("color", StringType(), True),
        StructField("team_assign_mode", StringType(), True),
    ]
)

"""Nested struct for leave request hours_per_day array (date, hours)."""
HOURS_PER_DAY_STRUCT = StructType(
    [
        StructField("date", StringType(), True),
        StructField("hours", DoubleType(), True),
    ]
)

"""Nested struct for time interval array (start_time, end_time)."""
TIME_INTERVAL_STRUCT = StructType(
    [
        StructField("start_time", StringType(), True),
        StructField("end_time", StringType(), True),
    ]
)


# =============================================================================
# Table Schema Definitions
# =============================================================================

# --- Organisation-scoped list endpoints (only require organisation_id in path) ---

"""Schema for the employees table (Get Employees / Get Employee API response)."""
EMPLOYEES_SCHEMA = StructType(
    [
        # Identity
        StructField("id", StringType(), True),
        StructField("account_email", StringType(), True),
        StructField("email", StringType(), True),
        StructField("title", StringType(), True),
        StructField("role", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("middle_name", StringType(), True),
        StructField("known_as", StringType(), True),
        StructField("full_name", StringType(), True),
        StructField("full_legal_name", StringType(), True),
        StructField("legal_name", StringType(), True),
        StructField("pronouns", StringType(), True),
        StructField("avatar_url", StringType(), True),
        # Contact
        StructField("address", StringType(), True),
        StructField("personal_email", StringType(), True),
        StructField("personal_mobile_number", StringType(), True),
        StructField("home_phone", StringType(), True),
        StructField("company_email", StringType(), True),
        StructField("company_mobile", StringType(), True),
        StructField("company_landline", StringType(), True),
        StructField("display_mobile_in_staff_directory", BooleanType(), True),
        # Employment
        StructField("job_title", StringType(), True),
        StructField("code", StringType(), True),
        StructField("location", StringType(), True),
        StructField("employing_entity", StringType(), True),
        StructField("start_date", StringType(), True),
        StructField("termination_date", StringType(), True),
        StructField("status", StringType(), True),
        StructField("termination_summary", StringType(), True),
        StructField("employment_type", StringType(), True),
        StructField("typical_work_day", StringType(), True),
        StructField("roster", StringType(), True),
        StructField("trial_or_probation_type", StringType(), True),
        StructField("trial_length", LongType(), True),
        StructField("probation_length", LongType(), True),
        StructField("global_teams_start_date", StringType(), True),
        StructField("global_teams_probation_end_date", StringType(), True),
        StructField("external_id", StringType(), True),
        StructField("work_country", StringType(), True),
        StructField("payroll_type", StringType(), True),
        StructField("time_zone", StringType(), True),
        StructField("nationality", StringType(), True),
        # Personal / demographic
        StructField("gender", StringType(), True),
        StructField("country", StringType(), True),
        StructField("date_of_birth", StringType(), True),
        StructField("marital_status", StringType(), True),
        StructField("aboriginal_torres_strait_islander", BooleanType(), True),
        StructField("previous_surname", StringType(), True),
        StructField("biography", StringType(), True),
        StructField("instapay_referral_opted_out", BooleanType(), True),
        # Contractor-only
        StructField("independent_contractor", BooleanType(), True),
        StructField("trading_name", StringType(), True),
        StructField("abn", StringType(), True),
        StructField("business_detail", BUSINESS_DETAIL_STRUCT, True),
        StructField("uk_tax_and_national_insurance", TAX_AND_NATIONAL_INSURANCE_STRUCT, True),
        # Nested: teams (Groups in UI)
        StructField("teams", ArrayType(REFERENCE_STRUCT), True),
        StructField("primary_cost_centre", REFERENCE_STRUCT, True),
        StructField("secondary_cost_centres", ArrayType(REFERENCE_STRUCT), True),
        StructField("primary_manager", REFERENCE_STRUCT, True),
        StructField("secondary_manager", REFERENCE_STRUCT, True),
        # Regional: Residential/postal address
        StructField("residential_address", ADDRESS_STRUCT, True),
        StructField("postal_address", ADDRESS_STRUCT, True),
    ]
)

"""Schema for the certifications table (Get Certifications API)."""
CERTIFICATIONS_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("type", StringType(), True),  # e.g. "check", "training"
    ]
)

"""Schema for the cost_centres table (Get Cost Centres API)."""
COST_CENTRES_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
    ]
)

"""Schema for the custom_fields table (Get Custom Fields API)."""
CUSTOM_FIELDS_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("hint", StringType(), True),
        StructField("description", StringType(), True),
        StructField("type", StringType(), True),  # e.g. "free_text", "single_select", "multi_select"
        StructField("in_onboarding", BooleanType(), True),
        StructField("required", BooleanType(), True),
        StructField("custom_field_permissions", ArrayType(CUSTOM_FIELD_PERMISSION_STRUCT), True),
        StructField("custom_field_options", ArrayType(CUSTOM_FIELD_OPTION_STRUCT), True),
    ]
)

"""Schema for the employing_entities table (Get Employing Entities API)."""
EMPLOYING_ENTITIES_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
    ]
)

"""Schema for the leave_categories table (Get Leave Categories API)."""
LEAVE_CATEGORIES_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("unit_type", StringType(), True),  # e.g. "days", "hours"
    ]
)

"""Schema for the policies table (Get Policies API)."""
POLICIES_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("induction", BooleanType(), True),
        StructField("created_at", StringType(), True),
    ]
)

"""Schema for the roles table (Get Roles/Tags API)."""
ROLES_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
    ]
)

"""Schema for the teams table (Get Teams API; shown as Groups in UI)."""
TEAMS_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("status", StringType(), True),  # e.g. "active"
    ]
)

"""Schema for the work_locations table (Get Work Locations API)."""
WORK_LOCATIONS_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("country", StringType(), True),
    ]
)

"""Schema for the work_sites table (Get Work Sites API)."""
WORK_SITES_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("status", StringType(), True),  # e.g. "active"
        StructField("roster_positions_count", LongType(), True),
        StructField("hr_positions", ArrayType(HR_POSITION_STRUCT), True),
        StructField("address", ADDRESS_STRUCT, True),
        StructField("departments", ArrayType(DEPARTMENT_STRUCT), True),
    ]
)

"""Schema for the leave_requests table (Get Leave Requests API)."""
LEAVE_REQUESTS_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("start_date", StringType(), True),
        StructField("end_date", StringType(), True),
        StructField("total_hours", DoubleType(), True),
        StructField("comment", StringType(), True),
        StructField("status", StringType(), True),
        StructField("leave_balance_amount", DoubleType(), True),
        StructField("leave_category_name", StringType(), True),
        StructField("reason", StringType(), True),
        StructField("employee_id", StringType(), True),
        StructField("hours_per_day", ArrayType(HOURS_PER_DAY_STRUCT), True),
    ]
)

"""Schema for the timesheet_entries table (Get Timesheet Entries API)."""
TIMESHEET_ENTRIES_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("date", StringType(), True),
        StructField("start_time", StringType(), True),
        StructField("end_time", StringType(), True),
        StructField("status", StringType(), True),
        StructField("units", DoubleType(), True),
        StructField("unit_type", StringType(), True),
        StructField("break_units", DoubleType(), True),
        StructField("breaks", ArrayType(TIME_INTERVAL_STRUCT), True),
        StructField("reason", StringType(), True),
        StructField("comment", StringType(), True),
        StructField("time", LongType(), True),  # milliseconds
        StructField("cost_centre", REFERENCE_STRUCT, True),
        StructField("work_site_id", StringType(), True),
        StructField("work_site_name", StringType(), True),
        StructField("position_id", StringType(), True),
        StructField("position_name", StringType(), True),
    ]
)


# =============================================================================
# Schema Mapping
# =============================================================================

"""Mapping of table names to their StructType schemas."""
TABLE_SCHEMAS: dict[str, StructType] = {
    "employees": EMPLOYEES_SCHEMA,
    "certifications": CERTIFICATIONS_SCHEMA,
    "cost_centres": COST_CENTRES_SCHEMA,
    "custom_fields": CUSTOM_FIELDS_SCHEMA,
    "employing_entities": EMPLOYING_ENTITIES_SCHEMA,
    "leave_categories": LEAVE_CATEGORIES_SCHEMA,
    "leave_requests": LEAVE_REQUESTS_SCHEMA,
    "policies": POLICIES_SCHEMA,
    "roles": ROLES_SCHEMA,
    "teams": TEAMS_SCHEMA,
    "timesheet_entries": TIMESHEET_ENTRIES_SCHEMA,
    "work_locations": WORK_LOCATIONS_SCHEMA,
    "work_sites": WORK_SITES_SCHEMA,
}


# =============================================================================
# Table Metadata Definitions
# =============================================================================

"""Metadata for each table including primary keys, cursor field, and ingestion type."""
TABLE_METADATA: dict[str, dict] = {
    "employees": {
        "primary_keys": ["id"],
        "ingestion_type": "snapshot",
    },
    "certifications": {
        "primary_keys": ["id"],
        "ingestion_type": "snapshot",
    },
    "cost_centres": {
        "primary_keys": ["id"],
        "ingestion_type": "snapshot",
    },
    "custom_fields": {
        "primary_keys": ["id"],
        "ingestion_type": "snapshot",
    },
    "employing_entities": {
        "primary_keys": ["id"],
        "ingestion_type": "snapshot",
    },
    "leave_categories": {
        "primary_keys": ["id"],
        "ingestion_type": "snapshot",
    },
    "leave_requests": {
        "primary_keys": ["id"],
        "ingestion_type": "snapshot",
    },
    "policies": {
        "primary_keys": ["id"],
        "ingestion_type": "snapshot",
    },
    "roles": {
        "primary_keys": ["id"],
        "ingestion_type": "snapshot",
    },
    "teams": {
        "primary_keys": ["id"],
        "ingestion_type": "snapshot",
    },
    "timesheet_entries": {
        "primary_keys": ["id"],
        "ingestion_type": "snapshot",
        "endpoint_suffix": "employees/-/timesheet_entries",
    },
    "work_locations": {
        "primary_keys": ["id"],
        "ingestion_type": "snapshot",
    },
    "work_sites": {
        "primary_keys": ["id"],
        "ingestion_type": "snapshot",
    },
}


# =============================================================================
# Supported Tables
# =============================================================================

"""List of all table names supported by the Employment Hero connector."""
SUPPORTED_TABLES: list[str] = list(TABLE_SCHEMAS.keys())
