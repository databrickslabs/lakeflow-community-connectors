"""
Type mappings and schema definitions for Zoho CRM connector.

This module centralizes all Spark type conversions and schema definitions
to keep the main connector code clean and maintainable.
"""

import json
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    BooleanType,
    DoubleType,
    ArrayType,
)


# =============================================================================
# Simple Type Mappings
# =============================================================================

# Maps Zoho data types to Spark types for simple (non-nested) fields
SIMPLE_TYPE_MAP = {
    "bigint": LongType(),
    "text": StringType(),
    "textarea": StringType(),
    "email": StringType(),
    "phone": StringType(),
    "website": StringType(),
    "autonumber": StringType(),
    "picklist": StringType(),
    "integer": LongType(),
    "double": DoubleType(),
    "currency": DoubleType(),
    "percent": DoubleType(),
    "boolean": BooleanType(),
    "date": StringType(),  # Stored as ISO 8601 string
    "datetime": StringType(),  # Stored as ISO 8601 string
    "fileupload": StringType(),
    "imageupload": StringType(),
    "profileimage": StringType(),
    "event_reminder": StringType(),
}


# =============================================================================
# Reusable Schema Components
# =============================================================================

# Basic lookup field structure (id + name)
BASIC_LOOKUP_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
])

# Extended lookup field structure (id + name + email)
EXTENDED_LOOKUP_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
])

# RRULE field for recurring events
RRULE_SCHEMA = StructType([
    StructField("FREQ", StringType(), True),
    StructField("INTERVAL", StringType(), True),
])

# ALARM field for event reminders
ALARM_SCHEMA = StructType([
    StructField("ACTION", StringType(), True),
])

# Basic subform item structure
BASIC_SUBFORM_SCHEMA = StructType([
    StructField("id", StringType(), True),
])


# =============================================================================
# Settings Table Schemas
# =============================================================================

USERS_SCHEMA = StructType([
    StructField("id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("role", BASIC_LOOKUP_SCHEMA, True),
    StructField("profile", BASIC_LOOKUP_SCHEMA, True),
    StructField("status", StringType(), True),
    StructField("created_time", StringType(), True),
    StructField("Modified_Time", StringType(), True),
    StructField("confirm", BooleanType(), True),
    StructField("territories", ArrayType(BASIC_LOOKUP_SCHEMA), True),
])

ROLES_SCHEMA = StructType([
    StructField("id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("display_label", StringType(), True),
    StructField("reporting_to", BASIC_LOOKUP_SCHEMA, True),
    StructField("admin_user", BooleanType(), True),
])

PROFILES_SCHEMA = StructType([
    StructField("id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("display_label", StringType(), True),
    StructField("default", BooleanType(), True),
    StructField("description", StringType(), True),
    StructField("created_time", StringType(), True),
    StructField("Modified_Time", StringType(), True),
])

SETTINGS_SCHEMAS = {
    "Users": USERS_SCHEMA,
    "Roles": ROLES_SCHEMA,
    "Profiles": PROFILES_SCHEMA,
}


# =============================================================================
# Subform/Line Item Schema
# =============================================================================

SUBFORM_BASE_FIELDS = [
    StructField("id", StringType(), False),
    StructField("_parent_id", StringType(), False),
    StructField("_parent_module", StringType(), False),
]

SUBFORM_COMMON_FIELDS = [
    StructField("Product_Name", BASIC_LOOKUP_SCHEMA, True),
    StructField("Quantity", DoubleType(), True),
    StructField("Unit_Price", DoubleType(), True),
    StructField("List_Price", DoubleType(), True),
    StructField("Net_Total", DoubleType(), True),
    StructField("Total", DoubleType(), True),
    StructField("Discount", DoubleType(), True),
    StructField("Total_After_Discount", DoubleType(), True),
    StructField("Tax", DoubleType(), True),
    StructField("Description", StringType(), True),
    StructField("Sequence_Number", LongType(), True),
]

LINE_ITEM_SCHEMA = StructType(SUBFORM_BASE_FIELDS + SUBFORM_COMMON_FIELDS)


# =============================================================================
# Junction/Related Table Schemas
# =============================================================================

JUNCTION_BASE_FIELDS = [
    StructField("_junction_id", StringType(), False),
    StructField("_parent_id", StringType(), False),
    StructField("_parent_module", StringType(), False),
    StructField("id", StringType(), False),
]

LEADS_RELATED_FIELDS = [
    StructField("First_Name", StringType(), True),
    StructField("Last_Name", StringType(), True),
    StructField("Email", StringType(), True),
    StructField("Company", StringType(), True),
    StructField("Phone", StringType(), True),
    StructField("Lead_Status", StringType(), True),
]

CONTACTS_RELATED_FIELDS = [
    StructField("First_Name", StringType(), True),
    StructField("Last_Name", StringType(), True),
    StructField("Email", StringType(), True),
    StructField("Phone", StringType(), True),
    StructField("Account_Name", BASIC_LOOKUP_SCHEMA, True),
]

CONTACT_ROLES_RELATED_FIELDS = [
    StructField("Contact_Role", StringType(), True),
    StructField("name", StringType(), True),
    StructField("Email", StringType(), True),
]

DEFAULT_RELATED_FIELDS = [
    StructField("name", StringType(), True),
]

# Map related module names to their field schemas
RELATED_MODULE_FIELDS = {
    "Leads": LEADS_RELATED_FIELDS,
    "Contacts": CONTACTS_RELATED_FIELDS,
    "Contact_Roles": CONTACT_ROLES_RELATED_FIELDS,
}

# API field names for related record queries
RELATED_MODULE_API_FIELDS = {
    "Leads": "id,First_Name,Last_Name,Email,Company,Phone,Lead_Status",
    "Contacts": "id,First_Name,Last_Name,Email,Phone,Account_Name",
    "Deals": "id,Deal_Name,Stage,Amount,Closing_Date,Account_Name",
    "Contact_Roles": "id,Contact_Role,name,Email",
}


# =============================================================================
# Type Conversion Functions
# =============================================================================

def zoho_field_to_spark_type(field: dict) -> StructField:
    """
    Convert a Zoho CRM field definition to a Spark StructField.
    
    Args:
        field: Zoho field metadata dictionary containing api_name, data_type, etc.
    
    Returns:
        A Spark StructField representing the field.
    """
    api_name = field["api_name"]
    data_type = field.get("data_type", "text")
    json_type = field.get("json_type")
    nullable = not field.get("required", False)

    # Check simple type map first
    if data_type in SIMPLE_TYPE_MAP:
        spark_type = SIMPLE_TYPE_MAP[data_type]
    # Handle complex/nested types
    elif data_type == "multiselectpicklist":
        spark_type = ArrayType(StringType(), True)
    elif data_type in ("lookup", "ownerlookup"):
        spark_type = EXTENDED_LOOKUP_SCHEMA
    elif data_type == "multiselectlookup":
        spark_type = ArrayType(BASIC_LOOKUP_SCHEMA, True)
    elif data_type == "subform":
        spark_type = ArrayType(BASIC_SUBFORM_SCHEMA, True)
    elif data_type == "consent_lookup":
        spark_type = BASIC_LOOKUP_SCHEMA
    elif data_type == "RRULE":
        spark_type = RRULE_SCHEMA
    elif data_type == "ALARM":
        spark_type = ALARM_SCHEMA
    else:
        # Default to StringType for unknown types
        spark_type = StringType()

    # Handle special JSON types - store as JSON strings for flexibility
    if json_type in ("jsonarray", "jsonobject"):
        spark_type = StringType()

    return StructField(api_name, spark_type, nullable)


def get_related_table_schema(related_module: str) -> StructType:
    """
    Build schema for a junction/related table.
    
    Args:
        related_module: Name of the related module (e.g., "Leads", "Contacts")
    
    Returns:
        A StructType representing the junction table schema.
    """
    related_fields = RELATED_MODULE_FIELDS.get(related_module, DEFAULT_RELATED_FIELDS)
    return StructType(JUNCTION_BASE_FIELDS + related_fields)


def normalize_record(record: dict, json_fields: set) -> dict:
    """
    Normalize a record for Spark compatibility.
    Only serializes fields that are declared as JSON strings in schema.
    
    Args:
        record: Raw record from Zoho API
        json_fields: Set of field names that should be JSON-serialized
    
    Returns:
        Normalized record dictionary
    """
    normalized = {}
    for key, value in record.items():
        if value is None:
            normalized[key] = None
        elif key in json_fields and isinstance(value, (dict, list)):
            normalized[key] = json.dumps(value)
        else:
            normalized[key] = value
    return normalized
