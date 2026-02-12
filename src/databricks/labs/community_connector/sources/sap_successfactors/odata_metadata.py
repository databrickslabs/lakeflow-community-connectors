# OData v2 $metadata parser for SAP SuccessFactors.
#
# Provides functions to:
#   - Fetch the EDMX metadata document from an OData v2 service endpoint
#   - Parse EDMX XML into structured entity-set / property dictionaries
#   - Map OData EDM types to PySpark types and build StructType schemas
#   - Infer CDC capability from property names
#
# These are used by SapSuccessFactorsLakeflowConnect in "dynamic" metadata mode.

import logging
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional

import requests
from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

logger = logging.getLogger(__name__)

# ----------------------------------------------------------------
# Constants
# ----------------------------------------------------------------

# Mapping from OData EDM primitive types to PySpark type constructors.
EDM_TYPE_MAP = {
    "Edm.String": StringType,
    "Edm.Int64": LongType,
    "Edm.Int32": LongType,
    "Edm.Int16": LongType,
    "Edm.Byte": LongType,
    "Edm.SByte": LongType,
    "Edm.Boolean": BooleanType,
    "Edm.Double": DoubleType,
    "Edm.Decimal": DoubleType,
    "Edm.Single": DoubleType,
    "Edm.Float": DoubleType,
    "Edm.DateTime": StringType,
    "Edm.DateTimeOffset": StringType,
    "Edm.Time": StringType,
    "Edm.Binary": BinaryType,
    "Edm.Guid": StringType,
}

# Common cursor field names used for CDC detection (checked in priority order).
CDC_CURSOR_CANDIDATES: List[str] = [
    "lastModifiedDateTime",
    "lastModifiedDate",
    "lastModifiedOn",
]

# EDMX / OData v2 XML namespaces.
EDMX_NS = {
    "edmx": "http://schemas.microsoft.com/ado/2007/06/edmx",
    "edm": "http://schemas.microsoft.com/ado/2008/09/edm",
    "m": "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata",
}


# ----------------------------------------------------------------
# EDMX parsing
# ----------------------------------------------------------------


def _parse_entity_type(et_elem) -> Dict[str, Any]:
    """
    Parse a single ``<EntityType>`` element into a structured dictionary.

    Args:
        et_elem: An :class:`xml.etree.ElementTree.Element` for the EntityType.

    Returns:
        Dictionary with ``name``, ``primary_keys``, and ``properties``.
    """
    et_name = et_elem.attrib.get("Name", "")

    # Extract primary keys
    primary_keys: List[str] = []
    key_elem = et_elem.find("edm:Key", EDMX_NS)
    if key_elem is not None:
        for prop_ref in key_elem.findall("edm:PropertyRef", EDMX_NS):
            pk_name = prop_ref.attrib.get("Name")
            if pk_name:
                primary_keys.append(pk_name)

    # Extract properties (skip NavigationProperty)
    properties: List[Dict[str, Any]] = []
    for prop_elem in et_elem.findall("edm:Property", EDMX_NS):
        prop_name = prop_elem.attrib.get("Name", "")
        prop_type = prop_elem.attrib.get("Type", "Edm.String")
        nullable_str = prop_elem.attrib.get("Nullable", "true")
        nullable = nullable_str.lower() != "false"
        properties.append(
            {
                "name": prop_name,
                "type": prop_type,
                "nullable": nullable,
            }
        )

    return {
        "name": et_name,
        "primary_keys": primary_keys,
        "properties": properties,
    }


def parse_edmx(xml_text: str) -> Dict[str, Any]:
    """
    Parse an EDMX XML document into a structured metadata dictionary.

    EDMX structure (OData v2)::

        <edmx:Edmx>
          <edmx:DataServices>
            <Schema Namespace="...">
              <EntityType Name="...">
                <Key><PropertyRef Name="..."/></Key>
                <Property Name="..." Type="Edm.String" Nullable="false"/>
              </EntityType>
              <EntityContainer>
                <EntitySet Name="..." EntityType="Namespace.TypeName"/>
              </EntityContainer>
            </Schema>
          </edmx:DataServices>
        </edmx:Edmx>

    Args:
        xml_text: Raw XML string of the EDMX document.

    Returns:
        Dictionary with structure::

            {
                "entity_sets": {
                    "<EntitySetName>": {
                        "entity_set_name": str,
                        "entity_type_name": str,
                        "primary_keys": [str, ...],
                        "properties": [
                            {"name": str, "type": str, "nullable": bool}, ...
                        ],
                    },
                    ...
                }
            }
    """
    root = ET.fromstring(xml_text)

    # Collect all EntityType definitions keyed by their fully-qualified name
    entity_types: Dict[str, Dict[str, Any]] = {}

    for schema_elem in root.findall(".//edm:Schema", EDMX_NS):
        namespace = schema_elem.attrib.get("Namespace", "")

        for et_elem in schema_elem.findall("edm:EntityType", EDMX_NS):
            et_name = et_elem.attrib.get("Name", "")
            fq_name = f"{namespace}.{et_name}" if namespace else et_name
            entity_types[fq_name] = _parse_entity_type(et_elem)

    # Collect EntitySet → EntityType mappings
    entity_sets: Dict[str, Dict[str, Any]] = {}

    for es_elem in root.findall(".//edm:EntityContainer/edm:EntitySet", EDMX_NS):
        es_name = es_elem.attrib.get("Name", "")
        et_fq_name = es_elem.attrib.get("EntityType", "")

        if et_fq_name in entity_types:
            et_info = entity_types[et_fq_name]
            entity_sets[es_name] = {
                "entity_set_name": es_name,
                "entity_type_name": et_info["name"],
                "primary_keys": et_info["primary_keys"],
                "properties": et_info["properties"],
            }

    logger.info(
        "Parsed OData metadata: %d entity types, %d entity sets",
        len(entity_types),
        len(entity_sets),
    )

    return {"entity_sets": entity_sets}


# ----------------------------------------------------------------
# Schema / config builders
# ----------------------------------------------------------------


def build_schema_from_metadata(entity_info: Dict[str, Any]) -> StructType:
    """
    Build a PySpark StructType from dynamically fetched entity metadata.

    Args:
        entity_info: Entity information dict containing a ``properties`` list.

    Returns:
        StructType with fields mapped from EDM types to PySpark types.
    """
    fields = []
    for prop in entity_info["properties"]:
        edm_type = prop["type"]
        spark_type_cls = EDM_TYPE_MAP.get(edm_type, StringType)
        nullable = prop["nullable"]
        fields.append(StructField(prop["name"], spark_type_cls(), nullable))
    return StructType(fields)


def build_table_config_from_metadata(entity_info: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build a table configuration dict from dynamically fetched entity metadata.

    Infers CDC capability by checking if common cursor fields
    (e.g., ``lastModifiedDateTime``) are present among the properties.

    Args:
        entity_info: Entity information dict from parsed metadata.

    Returns:
        Dictionary with ``primary_keys``, ``cursor_field``, ``ingestion_type``.
    """
    property_names = {p["name"] for p in entity_info["properties"]}

    # Auto-detect cursor field for CDC
    cursor_field: Optional[str] = None
    for candidate in CDC_CURSOR_CANDIDATES:
        if candidate in property_names:
            cursor_field = candidate
            break

    ingestion_type = "cdc" if cursor_field else "snapshot"

    return {
        "primary_keys": entity_info["primary_keys"],
        "cursor_field": cursor_field,
        "ingestion_type": ingestion_type,
    }


# ----------------------------------------------------------------
# HTTP fetch
# ----------------------------------------------------------------


def fetch_odata_metadata(
    session: requests.Session,
    base_url: str,
    headers: Dict[str, str],
) -> Dict[str, Any]:
    """
    Fetch the OData ``$metadata`` endpoint and parse the EDMX XML.

    The SAP SuccessFactors ``$metadata`` endpoint returns an EDMX document with:

    - EntityType definitions (with Key/PropertyRef and Property elements)
    - EntityContainer with EntitySet elements mapping set names to types

    Args:
        session: An authenticated :class:`requests.Session`.
        base_url: OData service base URL (e.g. ``https://host/odata/v2``).
        headers: Base HTTP headers (Authorization, etc.).

    Returns:
        Parsed metadata dictionary (same shape as :func:`parse_edmx`).

    Raises:
        Exception: If the metadata request fails.
    """
    url = f"{base_url}/$metadata"
    logger.info("Fetching OData metadata from %s", url)

    # Use Accept: application/xml for the metadata endpoint
    metadata_headers = headers.copy()
    metadata_headers["Accept"] = "application/xml"

    response = session.get(url, headers=metadata_headers, timeout=120)
    if response.status_code != 200:
        raise Exception(
            f"Failed to fetch OData metadata: {response.status_code} - {response.text}"
        )

    return parse_edmx(response.text)
