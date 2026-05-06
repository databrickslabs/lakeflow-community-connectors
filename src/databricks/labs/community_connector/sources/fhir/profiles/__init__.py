"""FHIR connector profile registry.

Maps (resource_type, profile) -> (StructType schema, extractor function).
Fallback chain: uk_core -> base_r4.

To add a new profile (e.g. au_core):
  1. Create profiles/au_core.py with @register("Resource", "au_core", schema) entries
  2. Prepend "au_core" to PROFILE_CHAIN below

To add a new resource:
  1. Add @register("NewResource", "base_r4", schema) entry in profiles/base_r4.py
  2. Optionally add uk_core override in profiles/uk_core.py

Note: base_r4.py and uk_core.py are NOT imported here to avoid circular
imports. They are imported by fhir_schemas.py to trigger registration.
"""

from databricks.labs.community_connector.sources.fhir.fhir_profile_registry import (
    FALLBACK_SCHEMA,
    PROFILE_CHAIN,
    extract,
    get_schema,
    register,
)

__all__ = [
    "FALLBACK_SCHEMA",
    "PROFILE_CHAIN",
    "extract",
    "get_schema",
    "register",
]
