from typing import Any, Optional
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    BooleanType,
    DoubleType,
    ArrayType,
    IntegerType,
)

from ._metadata_manager import MetadataManager


class SchemaGenerator:
    """
    Generates PySpark SQL schemas for Zoho CRM modules and derived tables.

    This class handles the mapping of Zoho CRM data types to PySpark types and
    constructs complex schemas for settings, subform, and related tables.
    """

    def __init__(self, metadata_manager: MetadataManager, derived_tables: dict) -> None:
        """
        Initializes the SchemaGenerator with a MetadataManager instance and derived table configuration.

        Args:
            metadata_manager: An instance of MetadataManager for fetching field metadata.
            derived_tables: A dictionary defining derived tables.
        """
        self._metadata_manager = metadata_manager
        self.DERIVED_TABLES = derived_tables

        # Initialize caches for subform schemas to minimize redundant API calls and improve performance.
        self._subform_schema_cache: dict[str, StructType] = {}

    def _zoho_type_to_spark_type(self, field: dict) -> StructField:
        """
        Converts a Zoho CRM field definition dictionary into a PySpark StructField.
        This mapping is crucial for defining the schema of ingested data in Spark.
        It handles various Zoho data types, including special cases like lookups and JSON fields.
        """
        field_api_name = field["api_name"]
        zoho_data_type = field.get("data_type", "text")
        zoho_json_type = field.get("json_type")
        is_nullable = not field.get("required", False)

        # Initialize spark_type as StringType by default to catch unmapped types safely.
        spark_type: Any = StringType()

        # Map Zoho CRM data types to corresponding PySpark SQL types.
        if zoho_data_type == "bigint":
            spark_type = LongType()
        elif zoho_data_type in ["text", "textarea", "email", "phone", "website", "autonumber"]:
            spark_type = StringType()
        elif zoho_data_type == "picklist":
            spark_type = StringType()
        elif zoho_data_type == "multiselectpicklist":
            spark_type = ArrayType(StringType(), True)
        elif zoho_data_type == "integer":
            spark_type = LongType()  # Use LongType for integers to avoid potential overflow issues.
        elif zoho_data_type in ["double", "currency", "percent"]:
            spark_type = DoubleType()
        elif zoho_data_type == "boolean":
            spark_type = BooleanType()
        elif zoho_data_type in ["date", "datetime"]:
            spark_type = StringType()  # Store date and datetime values as ISO 8601 strings.
        elif zoho_data_type in ["lookup", "ownerlookup"]:
            # Lookup fields are represented as nested structures, often with 'id', 'name', 'email'.
            spark_type = StructType([
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("email", StringType(), True),
            ])
        elif zoho_data_type == "multiselectlookup":
            # Multi-select lookups are arrays of nested structures, typically with 'id' and 'name'.
            spark_type = ArrayType(StructType([
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
            ]), True)
        elif zoho_data_type in ["fileupload", "imageupload", "profileimage"]:
            spark_type = StringType()  # File/image uploads are stored as URLs or file paths.
        elif zoho_data_type == "subform":
            # Subforms are complex nested structures; a flexible schema is used initially.
            spark_type = ArrayType(StructType([
                StructField("id", StringType(), True),
            ]), True)
        elif zoho_data_type == "consent_lookup":
            spark_type = StructType([
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
            ])
        elif zoho_data_type == "event_reminder":
            spark_type = StringType()
        elif zoho_data_type == "RRULE":
            spark_type = StructType([
                StructField("FREQ", StringType(), True),
                StructField("INTERVAL", StringType(), True),
            ])
        elif zoho_data_type == "ALARM":
            spark_type = StructType([
                StructField("ACTION", StringType(), True),
            ])
        # No 'else' block here as spark_type is initialized to StringType, handling unknown types.

        # Handle special Zoho JSON types by ensuring they are stored as Spark StringType.
        if zoho_json_type in ("jsonarray", "jsonobject"):
            spark_type = StringType() # Force JSON types to be serialized as strings for flexibility.

        return StructField(field_api_name, spark_type, is_nullable)

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        """
        Retrieves the schema for a specified Zoho CRM module or a derived table.
        This method acts as a dispatcher, calling specialized schema-generation
        logic for standard modules and derived tables respectively.
        """
        # If the table is a derived table, delegate schema generation to a specialized method.
        if table_name in self.DERIVED_TABLES:
            return self._get_derived_table_schema(table_name)

        # Ensure the requested table (module) is supported by the connector.
        supported_tables = self._metadata_manager.list_tables()
        if table_name not in supported_tables:
            raise ValueError(f"Table '{table_name}' is not supported. Available tables: {', '.join(supported_tables)}")

        # Fetch field metadata for standard CRM modules.
        module_fields = self._metadata_manager.get_fields(table_name)

        # If no fields are returned by Zoho CRM, provide a minimal schema to prevent errors.
        if not module_fields:
            print(f"Warning: No fields available for {table_name}. Returning minimal schema with 'id' field.")
            return StructType([StructField("id", LongType(), False)])

        # Convert Zoho CRM field definitions to PySpark StructField objects.
        spark_struct_fields = []
        for field_definition in module_fields:
            try:
                spark_struct_fields.append(self._zoho_type_to_spark_type(field_definition))
            except Exception as e:
                # Log a warning and skip fields that cause conversion errors.
                print(f"Warning: Could not convert field '{field_definition.get('api_name')}' for {table_name}: {e}")
                continue

        return StructType(spark_struct_fields)

    def _get_derived_table_schema(self, table_name: str) -> StructType:
        """
        Retrieves the PySpark schema for a specified derived table.
        This method dispatches to more specific schema generation functions
        based on the 'type' of the derived table (settings, subform, or related).
        """
        derived_table_config = self.DERIVED_TABLES[table_name]
        table_type = derived_table_config["type"]

        # Delegate to specialized schema methods based on the derived table's type.
        if table_type == "settings":
            return self._get_settings_table_schema(table_name, derived_table_config)
        elif table_type == "subform":
            return self._get_subform_table_schema(table_name, derived_table_config)
        elif table_type == "related":
            return self._get_related_table_schema(table_name, derived_table_config)
        else:
            # Raise an error for any unhandled derived table types.
            raise ValueError(f"Unknown derived table type encountered: {table_type}")

    def _get_settings_table_schema(self, table_name: str, config: dict) -> StructType:
        """
        Defines the static PySpark schema for Zoho CRM settings/organization tables
        like 'Users', 'Roles', and 'Profiles'.
        """
        if table_name == "Users":
            # Schema for the 'Users' table, including nested 'role' and 'profile' information.
            return StructType([
                StructField("id", StringType(), False),
                StructField("name", StringType(), True),
                StructField("email", StringType(), True),
                StructField("first_name", StringType(), True),
                StructField("last_name", StringType(), True),
                StructField("role", StructType([
                    StructField("id", StringType(), True),
                    StructField("name", StringType(), True),
                ]), True),
                StructField("profile", StructType([
                    StructField("id", StringType(), True),
                    StructField("name", StringType(), True),
                ]), True),
                StructField("status", StringType(), True),
                StructField("created_time", StringType(), True),
                StructField("Modified_Time", StringType(), True),
                StructField("confirm", BooleanType(), True),
                StructField("territories", ArrayType(StructType([
                    StructField("id", StringType(), True),
                    StructField("name", StringType(), True),
                ])), True),
            ])
        elif table_name == "Roles":
            # Schema for the 'Roles' table, including reporting structure details.
            return StructType([
                StructField("id", StringType(), False),
                StructField("name", StringType(), True),
                StructField("display_label", StringType(), True),
                StructField("reporting_to", StructType([
                    StructField("id", StringType(), True),
                    StructField("name", StringType(), True),
                ]), True),
                StructField("admin_user", BooleanType(), True),
            ])
        elif table_name == "Profiles":
            # Schema for the 'Profiles' table.
            return StructType([
                StructField("id", StringType(), False),
                StructField("name", StringType(), True),
                StructField("display_label", StringType(), True),
                StructField("default", BooleanType(), True),
                StructField("description", StringType(), True),
                StructField("created_time", StringType(), True),
                StructField("Modified_Time", StringType(), True),
            ])
        else:
            # Fallback for any other unrecognized settings tables, providing a minimal schema.
            return StructType([StructField("id", StringType(), False)])

    def _get_subform_table_schema(self, table_name: str, config: dict) -> StructType:
        """
        Defines the static PySpark schema for subform/line item tables.
        This schema includes common fields expected in line items and references
        to their parent records.
        """
        # Return cached schema if already generated.
        if table_name in self._subform_schema_cache:
            return self._subform_schema_cache[table_name]

        # Retrieve parent module and subform field names from the configuration.
        parent_module_name = config["parent_module"]
        subform_field_name = config["subform_field"]

        # Define base fields common to all subform line items.
        base_fields = [
            StructField("id", StringType(), False),
            StructField("_parent_id", StringType(), False),  # Foreign key to the parent record.
            StructField("_parent_module", StringType(), False),  # Name of the parent module.
        ]

        # Define additional common fields typically found in product-related line items.
        common_line_item_fields = [
            StructField("Product_Name", StructType([
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
            ]), True),
            StructField("Quantity", DoubleType(), True),
            StructField("Unit_Price", DoubleType(), True),
            StructField("List_Price", DoubleType(), True),
            StructField("Net_Total", DoubleType(), True),
            StructField("Total", DoubleType(), True),
            StructField("Discount", DoubleType(), True),
            StructField("Total_After_Discount", DoubleType(), True),
            StructField("Tax", DoubleType(), True),
            StructField("Description", StringType(), True),
            StructField("Sequence_Number", IntegerType(), True),
        ]

        # Combine base and common fields to form the complete subform schema.
        composed_schema = StructType(base_fields + common_line_item_fields)
        self._subform_schema_cache[table_name] = composed_schema  # Cache the generated schema.
        return composed_schema

    def _get_related_table_schema(self, table_name: str, config: dict) -> StructType:
        """
        Defines the PySpark schema for junction/related tables, which represent
        many-to-many relationships between Zoho CRM modules.
        The schema includes fields from both the parent and related modules.
        """
        parent_module_name = config["parent_module"]
        related_module_name = config["related_module"]

        # Define base fields common to all junction table records.
        base_junction_fields = [
            StructField("_junction_id", StringType(), False),  # Composite key for the junction record.
            StructField("_parent_id", StringType(), False),  # ID of the parent record in the relationship.
            StructField("_parent_module", StringType(), False),  # Module name of the parent record.
            StructField("id", StringType(), False),  # ID of the related record.
        ]

        # Add specific fields based on the type of the related module.
        if related_module_name == "Leads":
            related_module_specific_fields = [
                StructField("First_Name", StringType(), True),
                StructField("Last_Name", StringType(), True),
                StructField("Email", StringType(), True),
                StructField("Company", StringType(), True),
                StructField("Phone", StringType(), True),
                StructField("Lead_Status", StringType(), True),
            ]
        elif related_module_name == "Contacts":
            related_module_specific_fields = [
                StructField("First_Name", StringType(), True),
                StructField("Last_Name", StringType(), True),
                StructField("Email", StringType(), True),
                StructField("Phone", StringType(), True),
                StructField("Account_Name", StructType([
                    StructField("id", StringType(), True),
                    StructField("name", StringType(), True),
                ]), True),
            ]
        elif related_module_name == "Contact_Roles":
            # Special schema for 'Contact_Roles', which links Deals to Contacts with a role.
            related_module_specific_fields = [
                StructField("Contact_Role", StringType(), True),
                StructField("name", StringType(), True),
                StructField("Email", StringType(), True),
            ]
        else:
            # Default fields for unmapped related modules.
            related_module_specific_fields = [
                StructField("name", StringType(), True),
            ]

        # Combine base junction fields with related module-specific fields to form the complete schema.
        return StructType(base_junction_fields + related_module_specific_fields)

