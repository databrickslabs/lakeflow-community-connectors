"""FHIR connector profile registry.

Maps (resource_type, profile) -> (StructType schema, extractor function).
Fallback chain: uk_core -> base_r4.

This file contains the profile registry implementation that is also re-exported
via profiles/__init__.py. It is a standalone file so the merge script can include
it in the generated deployment artifact.

To add a new profile (e.g. au_core):
  1. Create profiles/au_core.py with @register("Resource", "au_core", schema) entries
  2. Prepend "au_core" to PROFILE_CHAIN below

To add a new resource:
  1. Add @register("NewResource", "base_r4", schema) entry in profiles/base_r4.py
  2. Optionally add uk_core override in profiles/uk_core.py
"""

from typing import Callable

from pyspark.sql.types import StringType, StructField, StructType, TimestampType

_SCHEMA_REGISTRY: dict = {}
_EXTRACTOR_REGISTRY: dict = {}

# Profiles tried in order. First match wins.
# To add au_core: insert "au_core" before "uk_core"
PROFILE_CHAIN: list = ["uk_core", "base_r4"]

_COMMON_FIELDS: list = [
    StructField("id", StringType(), nullable=True),
    StructField("resourceType", StringType(), nullable=True),
    StructField("lastUpdated", TimestampType(), nullable=True),
    StructField("raw_json", StringType(), nullable=True),
    StructField("extension", StringType(), nullable=True),
]

FALLBACK_SCHEMA = StructType(_COMMON_FIELDS)


def _chain(from_profile: str) -> list:
    """Return the fallback chain starting at from_profile.

    If from_profile is not in PROFILE_CHAIN, it is tried first (exact match),
    then the full PROFILE_CHAIN is appended as fallback.
    """
    try:
        idx = PROFILE_CHAIN.index(from_profile)
        return PROFILE_CHAIN[idx:]
    except ValueError:
        return [from_profile] + PROFILE_CHAIN


def register(resource_type: str, profile: str, schema: StructType) -> Callable:
    """Decorator factory. Registers schema and extractor for (resource_type, profile).

    Usage:
        @register("Patient", "base_r4", _PATIENT_SCHEMA)
        def _patient(r: dict) -> dict:
            return {...}
    """
    def decorator(fn: Callable) -> Callable:
        _SCHEMA_REGISTRY[(resource_type, profile)] = schema
        _EXTRACTOR_REGISTRY[(resource_type, profile)] = fn
        return fn
    return decorator


def get_schema(resource_type: str, profile: str = "uk_core") -> StructType:
    """Return the Spark schema for resource_type, walking the fallback chain.

    Falls back to FALLBACK_SCHEMA if no match found in any profile.
    """
    for p in _chain(profile):
        key = (resource_type, p)
        if key in _SCHEMA_REGISTRY:
            return _SCHEMA_REGISTRY[key]
    return FALLBACK_SCHEMA


def extract(resource: dict, resource_type: str, profile: str = "uk_core") -> dict:
    """Extract typed fields from resource, walking the fallback chain.

    Returns {} if no extractor found (caller adds common fields separately).
    """
    for p in _chain(profile):
        key = (resource_type, p)
        if key in _EXTRACTOR_REGISTRY:
            return _EXTRACTOR_REGISTRY[key](resource)
    return {}
