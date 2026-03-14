"""Spark schemas for FHIR R4 resources.

Public API unchanged — get_schema() and FALLBACK_SCHEMA are re-exported here.
Importing this module triggers registration of all base_r4 and uk_core schemas.

Profile selection:
  get_schema(resource_type)               -> uk_core profile (default)
  get_schema(resource_type, "base_r4")    -> base FHIR R4 profile
  get_schema(resource_type, "uk_core")    -> UK Core profile (falls back to base_r4)
"""

# Import base_r4 and uk_core to trigger @register decorators
from databricks.labs.community_connector.sources.fhir.profiles import base_r4 as _base_r4  # noqa: F401
from databricks.labs.community_connector.sources.fhir.profiles import uk_core as _uk_core  # noqa: F401

# Re-export public API — callers import from fhir_schemas, not from profiles directly
from databricks.labs.community_connector.sources.fhir.fhir_profile_registry import (
    FALLBACK_SCHEMA,
    get_schema,
)

__all__ = ["get_schema", "FALLBACK_SCHEMA"]
