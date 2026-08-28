"""Build managed-ingestion pipeline request bodies for community connectors.

A *managed* ingestion pipeline sets the pipeline's ``ingestion_definition`` (the
connection, the tables to replicate, and their per-source connector options)
instead of pointing the pipeline at Python source files cloned into the
workspace. The connector's Python packages are uploaded to a UC Volume and
referenced from ``environment.dependencies`` so the serverless cluster installs
them.

The installed ``databricks-sdk`` cannot represent COMMUNITY managed ingestion:
its ``IngestionSourceType`` enum has no ``COMMUNITY`` member and its
``ConnectorOptions`` model has no ``community_connector_options`` field. So the
bodies produced here are plain dicts, sent verbatim to
``POST``/``PUT /api/2.0/pipelines`` via ``WorkspaceClient.api_client.do(...)``
(the same raw-dict path the CLI already uses for the connections API).

Two input shapes are supported, disambiguated by
:func:`is_full_pipeline_spec`:

* **bare** — the supplied spec *is* the ingestion definition
  (``connection_name`` + ``objects`` at the top level). The CLI wraps it in a
  full pipeline body and backfills the pieces the user did not spell out.
* **full** — the supplied spec is a complete pipeline object that already
  contains an ``ingestion_definition`` key. The CLI treats it as authoritative
  and only adds the managed-ingestion ``configuration`` flag, the connector
  ``environment`` (when absent), and any values passed explicitly on the CLI.
"""

import copy
from typing import List, Optional

SOURCE_TYPE_COMMUNITY = "COMMUNITY"
"""IngestionSourceType value for community connectors (not in the SDK enum)."""

REGISTER_PYTHON_DATA_SOURCE_KEY = "pipelines.managedIngestion.registerPythonDataSource"
"""Pipeline configuration key that registers the connector's Python data source."""

DEFAULT_SOURCE_SCHEMA = "default"
"""Source schema assumed for community connectors when a table omits it."""


class ManagedPipelineSpecError(Exception):
    """Raised when a managed ingestion spec is structurally invalid."""


def _dict_field(body: dict, key: str) -> dict:
    """Return ``body[key]`` as a dict, replacing a missing/non-dict value.

    The returned dict is stored back on ``body`` so callers can mutate it in
    place. Guards against a spec that carries a non-dict (e.g. null) at ``key``.
    """
    value = body.get(key)
    if not isinstance(value, dict):
        value = {}
        body[key] = value
    return value


def is_full_pipeline_spec(spec: dict) -> bool:
    """Return True when ``spec`` is a full pipeline object, not a bare ingestion def.

    A full pipeline spec carries a top-level ``ingestion_definition`` block
    alongside pipeline fields (``name``, ``catalog``, ``configuration``, …). A
    bare spec *is* the ingestion definition, so its ``objects`` /
    ``connection_name`` live at the top level and there is no
    ``ingestion_definition`` key.
    """
    return isinstance(spec, dict) and "ingestion_definition" in spec


def validate_ingestion_definition(ingestion_def: dict) -> None:
    """Light structural check for a bare ingestion definition.

    ``objects`` may be absent or empty — that produces an "empty" managed
    pipeline with a connection but no tables yet, which the user can populate
    later. When ``objects`` is present it must be a list, and each object must
    select a source via ``table``, ``schema``, or ``report``. Everything else
    (connector options, destinations) is passed through to the API verbatim.
    """
    if not isinstance(ingestion_def, dict):
        raise ManagedPipelineSpecError("ingestion definition must be a mapping")

    objects = ingestion_def.get("objects")
    if objects is None:
        return
    if not isinstance(objects, list):
        raise ManagedPipelineSpecError("'objects' must be a list")

    for i, obj in enumerate(objects):
        if not isinstance(obj, dict) or not any(
            key in obj for key in ("table", "schema", "report")
        ):
            raise ManagedPipelineSpecError(
                f"objects[{i}] must contain one of 'table', 'schema', or 'report'"
            )


def _backfill_destinations(
    objects: list, catalog: Optional[str], schema: Optional[str]
) -> None:
    """Fill per-object destination catalog/schema (and source_schema) in place.

    The pipelines API requires ``destination_catalog`` / ``destination_schema``
    on every ``table`` / ``schema`` object. Rather than force the user to repeat
    them on each object, we backfill from the pipeline-level catalog/schema. A
    value already present on the object always wins.
    """
    for obj in objects:
        if not isinstance(obj, dict):
            continue
        for key in ("table", "schema"):
            target = obj.get(key)
            if not isinstance(target, dict):
                continue
            if catalog and not target.get("destination_catalog"):
                target["destination_catalog"] = catalog
            if schema and not target.get("destination_schema"):
                target["destination_schema"] = schema
        table = obj.get("table")
        if isinstance(table, dict) and not table.get("source_schema"):
            table["source_schema"] = DEFAULT_SOURCE_SCHEMA


def build_ingestion_definition(
    spec: dict,
    connection_name: Optional[str],
    catalog: Optional[str],
    schema: Optional[str],
) -> dict:
    """Turn a bare spec into a COMMUNITY ingestion definition (copy, not in place).

    Injects ``source_type: COMMUNITY`` and the connection name (unless the spec
    already carries one), then backfills per-object destinations.
    """
    ingestion_def = copy.deepcopy(spec)
    ingestion_def.setdefault("source_type", SOURCE_TYPE_COMMUNITY)
    if connection_name and not ingestion_def.get("connection_name"):
        ingestion_def["connection_name"] = connection_name

    objects = ingestion_def.get("objects")
    if isinstance(objects, list):
        _backfill_destinations(objects, catalog, schema)

    return ingestion_def


# pylint: disable=too-many-arguments,too-many-positional-arguments
def build_bare_pipeline_body(
    spec: dict,
    pipeline_name: str,
    connection_name: Optional[str],
    catalog: Optional[str],
    schema: Optional[str],
    dependencies: Optional[List[str]],
    channel: Optional[str] = None,
    serverless: Optional[bool] = None,
    base: Optional[dict] = None,
) -> dict:
    """Assemble a full pipeline body from a bare ingestion definition.

    The bare mode is the CLI-driven convenience path: pipeline-level catalog /
    schema, the managed-ingestion configuration flag, and the connector
    ``environment.dependencies`` are all filled in around the user's ingestion
    definition.

    ``base`` is the existing pipeline spec to merge onto (update path). Because
    the pipelines PUT is a full-settings replace, starting from the existing
    spec preserves fields the CLI does not manage (tags, notifications, budget
    policy, development/continuous, channel, serverless, …) instead of wiping
    them. Managed fields below are overlaid on top. On create, ``base`` is None
    and the body starts empty. A value already on ``base`` is only overwritten
    when the corresponding argument is provided, so callers omit ``channel`` /
    ``serverless`` on update to keep the pipeline's existing values.
    """
    ingestion_def = build_ingestion_definition(spec, connection_name, catalog, schema)

    body: dict = copy.deepcopy(base) if base else {}
    body["name"] = pipeline_name
    body["ingestion_definition"] = ingestion_def
    if catalog:
        body["catalog"] = catalog
    if schema:
        body["schema"] = schema
    if channel:
        body["channel"] = channel
    if serverless is not None:
        body["serverless"] = serverless

    _dict_field(body, "configuration")[REGISTER_PYTHON_DATA_SOURCE_KEY] = "true"

    if dependencies:
        _dict_field(body, "environment")["dependencies"] = list(dependencies)

    return body


# pylint: disable=too-many-arguments,too-many-positional-arguments
def augment_full_pipeline_body(
    spec: dict,
    pipeline_name: Optional[str] = None,
    connection_name: Optional[str] = None,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    dependencies: Optional[List[str]] = None,
) -> dict:
    """Return a copy of a full pipeline spec with the managed-ingestion additions.

    The user's spec is authoritative; this only:

    * ensures ``ingestion_definition.source_type`` is ``COMMUNITY``,
    * adds the ``registerPythonDataSource`` configuration flag if not set,
    * sets ``name`` / ``catalog`` / ``schema`` / ``connection_name`` from the
      values passed on the CLI (each only when explicitly provided), and
    * adds ``environment.dependencies`` only when the spec did not already
      include an ``environment`` block.
    """
    body = copy.deepcopy(spec)

    ingestion_def = body.get("ingestion_definition")
    if isinstance(ingestion_def, dict):
        ingestion_def.setdefault("source_type", SOURCE_TYPE_COMMUNITY)
        if connection_name:
            ingestion_def["connection_name"] = connection_name

    if pipeline_name:
        body["name"] = pipeline_name
    if catalog:
        body["catalog"] = catalog
    if schema:
        body["schema"] = schema

    _dict_field(body, "configuration").setdefault(REGISTER_PYTHON_DATA_SOURCE_KEY, "true")

    if "environment" not in body and dependencies:
        body["environment"] = {"dependencies": list(dependencies)}

    return body
