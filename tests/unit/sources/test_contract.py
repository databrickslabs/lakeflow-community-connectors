"""Offline contract tests for all connector implementations.

These tests require no credentials and make no API calls. They run in CI
without any secrets and catch structural regressions early.

What is tested (and why it's actually useful):
  1. Module imports   — catches syntax errors and missing hard dependencies
  2. Class hierarchy  — catches wrong base class or missing LakeflowConnect
  3. Abstract methods — catches unimplemented required methods
  4. Signatures       — catches parameter-name drift from the interface
  5. Spec YAML        — catches malformed or incomplete connector_spec.yaml

What is NOT tested here (requires live credentials):
  - list_tables, get_table_schema, read_table_metadata, read_table
  - Partition logic, offset contracts, incremental reads
  See the per-connector test suites (test_{source}_lakeflow_connect.py).

Run:
    pytest tests/unit/sources/test_contract.py -v
"""

import importlib
import inspect
from pathlib import Path
from typing import Optional, Type

import pytest
import yaml

from databricks.labs.community_connector.interface.lakeflow_connect import LakeflowConnect
from databricks.labs.community_connector.interface.supports_partition import (
    SupportsPartition,
    SupportsPartitionedStream,
)

# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

_PROJECT_ROOT = Path(__file__).parents[3]
_SOURCES_ROOT = (
    _PROJECT_ROOT / "src" / "databricks" / "labs" / "community_connector" / "sources"
)
_SKIP_DIRS = {"__pycache__"}


def _discover_sources():
    if not _SOURCES_ROOT.exists():
        return
    for d in sorted(_SOURCES_ROOT.iterdir()):
        if d.is_dir() and d.name not in _SKIP_DIRS and not d.name.startswith("_"):
            yield d.name, d


_SOURCE_DIRS: dict[str, Path] = {name: d for name, d in _discover_sources()}
_SOURCE_NAMES: list[str] = list(_SOURCE_DIRS)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_REQUIRED_METHODS = {
    "list_tables": ["self"],
    "get_table_schema": ["self", "table_name", "table_options"],
    "read_table_metadata": ["self", "table_name", "table_options"],
    "read_table": ["self", "table_name", "start_offset", "table_options"],
}
_PARTITION_METHODS = {
    "get_partitions": ["self", "table_name", "table_options"],
    "read_partition": ["self", "table_name", "partition", "table_options"],
}
_STREAM_METHODS = {
    "latest_offset": ["self", "table_name", "table_options"],
}


def _import_module(source_name: str):
    """Return (module, None), (None, "skip:<reason>"), or (None, "fail:<reason>")."""
    path = f"databricks.labs.community_connector.sources.{source_name}.{source_name}"
    try:
        return importlib.import_module(path), None
    except ModuleNotFoundError as e:
        missing = e.name or ""
        own_prefix = f"databricks.labs.community_connector.sources.{source_name}"
        if missing.startswith(own_prefix) or missing == source_name:
            # The connector module itself is missing — real failure
            return None, f"fail:ModuleNotFoundError: {e}"
        # A third-party dependency is missing — skip, not the connector's fault
        return None, f"skip:Missing dependency '{missing}' — install it to run contract checks"
    except ImportError as e:
        # Connector explicitly checks for an optional dep at import time (e.g. raise ImportError)
        return None, f"skip:Missing optional dependency — {e}"
    except Exception as e:
        return None, f"fail:{type(e).__name__}: {e}"


def _find_connector_class(module) -> Optional[Type[LakeflowConnect]]:
    """Return the LakeflowConnect subclass defined in module, or None."""
    for _, obj in inspect.getmembers(module, inspect.isclass):
        if (
            issubclass(obj, LakeflowConnect)
            and obj is not LakeflowConnect
            and obj.__module__ == module.__name__
        ):
            return obj
    return None


def _check_signatures(cls, method_specs: dict) -> list[str]:
    """Return a list of signature violation strings."""
    errors = []
    for method_name, expected_params in method_specs.items():
        method = getattr(cls, method_name, None)
        if method is None:
            errors.append(f"{method_name}(): method not found on class")
            continue
        actual = list(inspect.signature(method).parameters.keys())
        for p in expected_params:
            if p not in actual:
                errors.append(f"{method_name}(): missing parameter '{p}' (got {actual})")
    return errors


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("source_name", _SOURCE_NAMES)
class TestConnectorContract:
    """Structural contract tests — no credentials, no API calls."""

    def test_module_imports(self, source_name):
        """Module imports without error (catches syntax errors, missing deps)."""
        _, err = _import_module(source_name)
        if err and err.startswith("skip:"):
            pytest.skip(err[5:])
        if err:
            pytest.fail(
                f"Cannot import sources.{source_name}.{source_name}: {err[5:]}\n"
                "  Fix: Check for syntax errors or import-time side effects that "
                "require credentials."
            )

    def test_connector_class_exists(self, source_name):
        """A LakeflowConnect subclass is defined in the module."""
        module, err = _import_module(source_name)
        if err:
            pytest.skip(err[5:])
        cls = _find_connector_class(module)
        assert cls is not None, (
            f"No LakeflowConnect subclass found in sources.{source_name}.{source_name}.\n"
            f"  Fix: Define a class extending LakeflowConnect in {source_name}.py."
        )

    def test_no_unimplemented_abstract_methods(self, source_name):
        """All abstract methods from LakeflowConnect are implemented."""
        module, err = _import_module(source_name)
        if err:
            pytest.skip(err[5:])
        cls = _find_connector_class(module)
        if cls is None:
            pytest.skip("No connector class found")

        abstract = getattr(cls, "__abstractmethods__", frozenset())
        if abstract:
            pytest.fail(
                f"{cls.__name__} has unimplemented abstract methods: {sorted(abstract)}\n"
                "  Fix: Implement all abstract methods from LakeflowConnect."
            )

    def test_method_signatures_match_interface(self, source_name):
        """Method signatures have the required parameter names."""
        module, err = _import_module(source_name)
        if err:
            pytest.skip(err[5:])
        cls = _find_connector_class(module)
        if cls is None:
            pytest.skip("No connector class found")

        errors = _check_signatures(cls, _REQUIRED_METHODS)
        if errors:
            pytest.fail(
                f"{cls.__name__} signature violations:\n"
                + "\n".join(f"  - {e}" for e in errors)
                + "\n  Fix: Match parameter names from the LakeflowConnect interface."
            )

    def test_init_accepts_options(self, source_name):
        """__init__ accepts an 'options' dict parameter."""
        module, err = _import_module(source_name)
        if err:
            pytest.skip(err[5:])
        cls = _find_connector_class(module)
        if cls is None:
            pytest.skip("No connector class found")

        params = list(inspect.signature(cls.__init__).parameters.keys())
        assert "options" in params, (
            f"{cls.__name__}.__init__ has no 'options' parameter (got {params}).\n"
            "  Fix: def __init__(self, options: dict[str, str]) -> None: ..."
        )

    def test_partition_mixin_contract(self, source_name):
        """SupportsPartition/Stream mixins are used correctly."""
        module, err = _import_module(source_name)
        if err:
            pytest.skip(err[5:])
        cls = _find_connector_class(module)
        if cls is None:
            pytest.skip("No connector class found")
        if not issubclass(cls, (SupportsPartition, SupportsPartitionedStream)):
            pytest.skip("Connector does not use partition mixins")

        # Must also extend LakeflowConnect
        assert issubclass(cls, LakeflowConnect), (
            f"{cls.__name__} extends a partition mixin but not LakeflowConnect.\n"
            "  Fix: class {cls.__name__}(LakeflowConnect, SupportsPartition): ..."
        )

        required = dict(_PARTITION_METHODS)
        if issubclass(cls, SupportsPartitionedStream):
            required.update(_STREAM_METHODS)

        errors = _check_signatures(cls, required)
        if errors:
            pytest.fail(
                f"{cls.__name__} partition mixin signature violations:\n"
                + "\n".join(f"  - {e}" for e in errors)
            )

    def test_connector_spec_yaml(self, source_name):
        """connector_spec.yaml parses as valid YAML and has required structure."""
        spec_path = _SOURCE_DIRS[source_name] / "connector_spec.yaml"
        if not spec_path.exists():
            pytest.skip("connector_spec.yaml not yet created (added at step 5)")

        try:
            with open(spec_path) as f:
                spec = yaml.safe_load(f)
        except yaml.YAMLError as e:
            pytest.fail(f"connector_spec.yaml is not valid YAML: {e}")

        if not isinstance(spec, dict):
            pytest.fail("connector_spec.yaml must be a YAML mapping at the top level.")

        errors = []

        # display_name
        dn = spec.get("display_name")
        if not dn:
            errors.append("Missing or empty 'display_name'")
        elif not isinstance(dn, str):
            errors.append("'display_name' must be a string")

        # connection.parameters
        conn = spec.get("connection")
        if conn is None:
            errors.append("Missing 'connection' section")
        elif not isinstance(conn, dict):
            errors.append("'connection' must be a mapping")
        else:
            params = conn.get("parameters")
            if params is None:
                errors.append("Missing 'connection.parameters'")
            elif not isinstance(params, list) or not params:
                errors.append("'connection.parameters' must be a non-empty list")
            else:
                for i, param in enumerate(params):
                    if not isinstance(param, dict):
                        errors.append(f"connection.parameters[{i}] must be a mapping")
                        continue
                    for key in ("name", "type"):
                        if key not in param:
                            errors.append(f"connection.parameters[{i}] missing '{key}'")

        if errors:
            pytest.fail(
                "connector_spec.yaml structural errors:\n"
                + "\n".join(f"  - {e}" for e in errors)
            )
