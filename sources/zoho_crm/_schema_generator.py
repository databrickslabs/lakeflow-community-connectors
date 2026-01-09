"""
Schema generator for Zoho CRM connector.

This module handles the conversion of Zoho CRM field metadata into
PySpark SQL schemas (StructType). It provides:
- Type mapping from Zoho CRM types to PySpark SQL types
- Schema generation for standard CRM modules
- Schema generation for derived tables (settings, subforms, junctions)
- Handling of complex field types (lookup, subform, multiselectpicklist)

The SchemaGenerator works closely with MetadataManager to generate
accurate schemas for ingestion into Delta Lake tables.

Example:
    >>> schema_gen = SchemaGenerator(metadata_manager, derived_tables)
    >>> schema = schema_gen.get_table_schema("Leads", {})
    >>> print(schema.simpleString())
    struct<id:string,First_Name:string,Last_Name:string,...>
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
    LongType,
    DoubleType,
    TimestampType,
    ArrayType,
    MapType,
)

from ._metadata_manager import MetadataManager


class SchemaGenerator:
    """
    Generates PySpark schemas from Zoho CRM field metadata.

    This class maps Zoho CRM data types to appropriate PySpark SQL types
    and constructs StructType schemas for ingestion. It handles:
    - Primitive types (text, bigint, boolean, double, etc.)
    - Complex types (lookup, multiselectpicklist, jsonobject, jsonarray)
    - Derived table schemas (settings, subforms, junctions)

    The mapping strategy prioritizes data fidelity - choosing types that
    can represent the full range of source values without loss.

    Type Mapping Reference:
        Zoho Type           -> PySpark Type
        -----------------------------------------
        text, textarea      -> StringType
        bigint, integer     -> LongType
        double, currency    -> DoubleType
        boolean             -> BooleanType
        datetime, date      -> TimestampType
        lookup, ownerlookup -> StringType (stores ID only)
        multiselectpicklist -> ArrayType(StringType)
        jsonobject          -> MapType(StringType, StringType)
        jsonarray           -> ArrayType(MapType(StringType, StringType))
    """

    # Zoho CRM type to PySpark type mapping
    # Using LongType for integers as Zoho bigint can exceed 32-bit range
    TYPE_MAPPING = {
        # Text types
        "text": StringType(),
        "textarea": StringType(),
        "email": StringType(),
        "phone": StringType(),
        "website": StringType(),
        "picklist": StringType(),
        "autonumber": StringType(),

        # Numeric types
        "bigint": LongType(),
        "integer": LongType(),  # Use LongType for safety
        "double": DoubleType(),
        "decimal": DoubleType(),
        "currency": DoubleType(),
        "percent": DoubleType(),

        # Boolean types
        "boolean": BooleanType(),

        # Date/time types - stored as strings for format preservation
        "datetime": TimestampType(),
        "date": StringType(),  # Keep date as string to preserve format
        "time": StringType(),

        # Reference types - store IDs as strings
        "lookup": StringType(),
        "ownerlookup": StringType(),

        # Multi-value types
        "multiselectpicklist": ArrayType(StringType()),
        "multiuserlookup": ArrayType(StringType()),

        # Consent-related types
        "consent_lookup": StringType(),

        # JSON types (for flexible/custom fields)
        "jsonobject": MapType(StringType(), StringType()),
        "jsonarray": ArrayType(MapType(StringType(), StringType())),

        # Profile fields (complex objects stored as string)
        "profileimage": StringType(),
        "formula": StringType(),  # Formula results stored as string
    }

    def __init__(self, metadata_manager: MetadataManager, derived_tables: dict) -> None:
        """
        Initialize the SchemaGenerator.

        Args:
            metadata_manager: MetadataManager instance for field lookups.
            derived_tables: Dictionary of derived table definitions.
        """
        self._metadata_manager = metadata_manager
        self.DERIVED_TABLES = derived_tables

    def _get_spark_type(self, zoho_type: str):
        """
        Map a Zoho CRM field type to the corresponding PySpark SQL type.

        Args:
            zoho_type: The Zoho CRM data type string (e.g., "text", "bigint").

        Returns:
            Appropriate PySpark SQL type. Defaults to StringType for
            unknown types to ensure data is not lost.
        """
        return self.TYPE_MAPPING.get(zoho_type, StringType())

    def get_table_schema(self, table_name: str, options: dict[str, str]) -> StructType:
        """
        Generate a PySpark StructType schema for the specified table.

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

        Args:
            table_name: Name of the table (module or derived table).
            options: Table-specific options (currently unused but included
                for interface compatibility).

        Returns:
            StructType schema definition for the table.

        Raises:
            ValueError: If the table is not found (via _get_module_schema).
        """
        # Check if this is a derived table first
        if table_name in self.DERIVED_TABLES:
            return self._get_derived_table_schema(table_name)

        # Standard module - generate schema from fields API
        return self._get_module_schema(table_name)

    def _get_module_schema(self, module_name: str) -> StructType:
        """
        Generate schema for a standard CRM module using the fields API.

        This method fetches field metadata from Zoho CRM and constructs
        a StructType with appropriate column types. Special handling is
        applied for:
        - Subform fields (excluded from parent schema)
        - Lookup fields (normalized to ID strings)
        - JSON fields (stored as MapType or ArrayType)

        Args:
            module_name: The API name of the CRM module.

        Returns:
            StructType schema for the module.

        Note:
            The schema includes a meta_timestamp field for tracking
            when records were last synced (not present in source data,
            added by the ingestion pipeline).
        """
        fields = self._metadata_manager.get_fields(module_name)
        struct_fields = []

        for field in fields:
            field_name = field.get("api_name", "")
            zoho_type = field.get("data_type", "text")
            json_type = field.get("json_type", "")

            # Skip subform fields - they're handled as separate derived tables
            if zoho_type == "subform":
                continue

            # Handle JSON types (custom/flexible fields)
            if json_type == "jsonobject":
                spark_type = MapType(StringType(), StringType())
            elif json_type == "jsonarray":
                spark_type = ArrayType(MapType(StringType(), StringType()))
            else:
                spark_type = self._get_spark_type(zoho_type)

            struct_fields.append(StructField(field_name, spark_type, nullable=True))

        return StructType(struct_fields)

    def _get_derived_table_schema(self, table_name: str) -> StructType:
        """
        Generate schema for a connector-defined derived table.

        Derived tables include:
        - Settings tables (Users, Roles, Profiles)
        - Subform tables (line items from parent records)
        - Junction/related tables (many-to-many relationships)

        Args:
            table_name: Name of the derived table.

        Returns:
            StructType schema for the derived table.

        Note:
            Junction tables include a synthetic _junction_id field that
            combines parent and child IDs for deduplication.
        """
        derived_config = self.DERIVED_TABLES[table_name]
        table_type = derived_config["type"]

        if table_type == "settings":
            return self._get_settings_table_schema(table_name)
        elif table_type == "subform":
            return self._get_subform_table_schema(table_name, derived_config)
        elif table_type == "related":
            return self._get_related_table_schema(table_name, derived_config)
        else:
            # Fallback to generic string fields
            return StructType([StructField("id", StringType(), nullable=False)])

    def _get_settings_table_schema(self, table_name: str) -> StructType:
        """
        Generate schema for a settings-type derived table.

        Settings tables (Users, Roles, Profiles) have predefined schemas
        based on the Zoho CRM Settings API responses.

        Args:
            table_name: Name of the settings table.

        Returns:
            StructType schema for the settings table.
        """
        # Define schemas for each settings table
        if table_name == "Users":
            return StructType([
                StructField("id", StringType(), nullable=False),
                StructField("name", StringType(), nullable=True),
                StructField("email", StringType(), nullable=True),
                StructField("role", StringType(), nullable=True),
                StructField("profile", StringType(), nullable=True),
                StructField("status", StringType(), nullable=True),
                StructField("Created_Time", TimestampType(), nullable=True),
                StructField("Modified_Time", TimestampType(), nullable=True),
            ])
        elif table_name == "Roles":
            return StructType([
                StructField("id", StringType(), nullable=False),
                StructField("name", StringType(), nullable=True),
                StructField("display_label", StringType(), nullable=True),
                StructField("reporting_to", StringType(), nullable=True),
            ])
        elif table_name == "Profiles":
            return StructType([
                StructField("id", StringType(), nullable=False),
                StructField("name", StringType(), nullable=True),
                StructField("description", StringType(), nullable=True),
                StructField("default", BooleanType(), nullable=True),
            ])
        else:
            # Generic settings schema fallback
            return StructType([
                StructField("id", StringType(), nullable=False),
                StructField("name", StringType(), nullable=True),
            ])

    def _get_subform_table_schema(self, table_name: str, config: dict) -> StructType:
        """
        Generate schema for a subform-type derived table.

        Subform tables represent line-item data embedded within parent records
        (e.g., Quote_Line_Items within Quotes). The schema is derived from
        the subform field definition in the parent module.

        Args:
            table_name: Name of the subform table.
            config: Derived table configuration containing:
                - parent_module: Parent module API name
                - subform_field: Subform field API name in parent

        Returns:
            StructType schema including:
            - All subform fields mapped to PySpark types
            - _parent_id: Reference to parent record ID
        """
        parent_module = config["parent_module"]
        subform_field = config["subform_field"]

        # Fetch parent module fields to find subform definition
        parent_fields = self._metadata_manager.get_fields(parent_module)

        # Find the subform field and get its nested fields
        subform_fields = []
        for field in parent_fields:
            if field.get("api_name") == subform_field:
                subform_fields = field.get("subform", {}).get("fields", [])
                break

        # Build schema from subform field definitions
        struct_fields = [
            # Parent reference field
            StructField("_parent_id", StringType(), nullable=False),
        ]

        for field in subform_fields:
            field_name = field.get("api_name", "")
            zoho_type = field.get("data_type", "text")
            spark_type = self._get_spark_type(zoho_type)
            struct_fields.append(StructField(field_name, spark_type, nullable=True))

        return StructType(struct_fields)

    def _get_related_table_schema(self, table_name: str, config: dict) -> StructType:
        """
        Generate schema for a junction/related-type derived table.

        Junction tables represent many-to-many relationships between modules
        (e.g., Contacts_X_Deals linking Contacts to Deals). The schema includes:
        - References to both parent and related records
        - A synthetic _junction_id for deduplication
        - Any additional junction-specific fields

        Args:
            table_name: Name of the junction table.
            config: Derived table configuration containing:
                - parent_module: Primary module API name
                - related_module: Related module API name

        Returns:
            StructType schema for the junction table.
        """
        parent_module = config["parent_module"]
        related_module = config["related_module"]

        return StructType([
            # Synthetic composite key for deduplication
            StructField("_junction_id", StringType(), nullable=False),
            # Parent record reference
            StructField(f"{parent_module}_id", StringType(), nullable=False),
            # Related record reference
            StructField(f"{related_module}_id", StringType(), nullable=False),
            # Junction table may have additional fields (added dynamically)
        ])
