"""Tests for the Power BI semantic model connector.

Runs offline against the in-process source simulator described by
``source_simulator/specs/power_bi_semantic_model/`` (``endpoints.yaml`` plus
the schema-bootstrapped ``corpus/``).  Four of the endpoints are
handler-backed:

  * ``POST login.microsoftonline.com/{tenant}/oauth2/v2.0/token`` — stub
    Entra ID client-credentials exchange (``handlers/scanner.py``).
  * ``POST /admin/workspaces/getInfo`` + ``GET .../scanStatus/{id}`` — the
    async scanner handshake (``handlers/scanner.py``).
  * ``GET .../scanResult/{id}`` — the nested metadata tree that feeds
    ``dataset_tables`` / ``dataset_columns`` / ``dataset_measures``.
  * ``POST .../datasets/{id}/executeQueries`` — a fixed 5-row DAX envelope
    with bracketed column names, mixed value types and one ``BLANK()``
    rendered as a JSON null (``handlers/dax.py``).

Stand-in credentials below are values of the right shape; the simulator
never validates them.

``dax_query_result`` is the one table that produces nothing until the
pipeline configures it, so the shared shape suites are handed
``table_configs`` (below) that opt it in — otherwise ``get_partitions``
legitimately returns ``[]`` and the suite reads that as a bug.

Beyond the shared shape suites, the scenario tests at the bottom pin the
behaviours that are specific to this connector and easy to regress:

  * the scanner workflow has **no** non-admin fallback (unlike
    ``workspaces`` / ``datasets``, which fall back to the membership-scoped
    endpoints on 401/403);
  * ``dataset_refresh_history`` is append-only and bounded client-side by
    ``(since, until]`` on ``startTime`` — terminal-status transitions on an
    already-emitted refresh are never backfilled, by design;
  * snapshot tables re-emit only once per connector instance, guarded by
    ``_snapshot_emitted`` instance state rather than by the offset (which is
    always ``{}`` for a snapshot);
  * ``dax_query_result`` is opt-in (empty without ``dax_query``), has two
    schema modes (typed columns vs. a ``columns`` string map), validates its
    configuration eagerly, and costs exactly one un-paged POST per read.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    MapType,
    StringType,
    TimestampType,
)

import databricks.labs.community_connector.source_simulator as _sim_pkg
from databricks.labs.community_connector.source_simulator import (
    MODE_SIMULATE,
    Simulator,
)
from databricks.labs.community_connector.sources.power_bi_semantic_model.power_bi_semantic_model import (  # noqa: E501
    PowerBiSemanticModelLakeflowConnect,
)
from databricks.labs.community_connector.sources.power_bi_semantic_model.power_bi_semantic_model_schemas import (  # noqa: E501
    DATASET_COLUMNS,
    DATASET_MEASURES,
    DATASET_REFRESH_HISTORY,
    DATASET_TABLES,
    DATASETS,
    DAX_QUERY_RESULT,
    DAX_QUERY_RESULT_SCHEMA,
    SCANNER_TABLES,
    SNAPSHOT_TABLES,
    SUPPORTED_TABLES,
    WORKSPACES,
)
from databricks.labs.community_connector.sources.power_bi_semantic_model.power_bi_semantic_model_utils import (  # noqa: E501
    PowerBiAdminAccessDenied,
    PowerBiApiError,
    PowerBiClient,
    parse_iso,
    query_fingerprint,
)
from tests.unit.sources.test_partition_suite import SupportsPartitionedStreamTests
from tests.unit.sources.test_suite import LakeflowConnectTests

_REPLAY_CONFIG = {
    "tenant_id": "00000000-0000-0000-0000-000000000000",
    "client_id": "11111111-1111-1111-1111-111111111111",
    "client_secret": "simulator-fake-client-secret",
}

_SPEC_DIR = Path(_sim_pkg.__file__).parent / "specs" / "power_bi_semantic_model"
_SPEC_PATH = _SPEC_DIR / "endpoints.yaml"
_CORPUS_DIR = _SPEC_DIR / "corpus"

# ---------------------------------------------------------------------------
# dax_query_result configuration
# ---------------------------------------------------------------------------
#
# The DAX column names below are exactly the keys the simulator's
# ``handlers/dax.py`` returns.  They cover both spellings DAX emits —
# ``Table[Column]`` for a projected column and ``[Measure]`` for a measure —
# and every supported ``type``, so the typed-column path is exercised end to
# end rather than in the all-strings degenerate case.
_DAX_QUERY = (
    'EVALUATE SUMMARIZECOLUMNS(Sales[Region], Sales[OrderDate], "Total Units", [Total Units])'
)

_DAX_COLUMN_SPECS = [
    {"dax": "Sales[Region]", "name": "region", "type": "string"},
    {"dax": "Sales[OrderDate]", "name": "order_date", "type": "timestamp"},
    {"dax": "Sales[Amount]", "name": "amount", "type": "double"},
    {"dax": "[Total Units]", "name": "total_units", "type": "long"},
    {"dax": "[Is Target Met]", "name": "is_target_met", "type": "boolean"},
]

# Minimum viable opt-in: a query plus the single model to run it against.
# Yields the ``columns`` map<string,string> fallback schema.
_DAX_MAP_OPTIONS = {
    "dax_query": _DAX_QUERY,
    "workspace_id": "ws-1",
    "dataset_id": "ds-1",
}

# The same, plus declared columns — yields the typed schema.
_DAX_TYPED_OPTIONS = dict(_DAX_MAP_OPTIONS, dax_columns=json.dumps(_DAX_COLUMN_SPECS))

# Per-table options handed to the shared shape suites.  Only
# ``dax_query_result`` needs any: without ``dax_query`` it is empty by design,
# which the suites (correctly, for every other table) treat as a failure.
# The typed variant is the one under suite coverage because it is the richer
# path — the map fallback is pinned by the scenario tests below.
_TABLE_OPTIONS: dict[str, dict] = {DAX_QUERY_RESULT: dict(_DAX_TYPED_OPTIONS)}


_POWER_BI_UTILS_POST = (
    "databricks.labs.community_connector.sources.power_bi_semantic_model."
    "power_bi_semantic_model_utils.requests.post"
)


def _token_response(access_token="tok-1", expires_in=3600):
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = {"access_token": access_token, "expires_in": expires_in}
    return resp


class TestPowerBiClientAuth:
    """Auth method selection and token-exchange mechanics for PowerBiClient.

    Power BI's own docs describe two ways to call its REST API — the
    connector runs the Entra ID exchange itself for either, since neither
    is a UC-managed OAuth flow:

    - ``service_principal`` — client-credentials grant, from client_secret.
    - ``user`` — Resource Owner Password Credentials (ROPC) grant, from
      username/password (the classic "master user" pattern).

    ``client_secret`` takes precedence when both are supplied.
    """

    def test_client_secret_uses_client_credentials_grant(self):
        with patch(_POWER_BI_UTILS_POST, return_value=_token_response()) as post:
            client = PowerBiClient("tenant-1", "client-1", client_secret="secret-1")
            token = client.get_access_token()

        assert token == "tok-1"
        args, kwargs = post.call_args
        assert args[0] == "https://login.microsoftonline.com/tenant-1/oauth2/v2.0/token"
        assert kwargs["data"]["grant_type"] == "client_credentials"
        assert kwargs["data"]["client_secret"] == "secret-1"
        assert "username" not in kwargs["data"]

    def test_username_password_uses_password_grant(self):
        with patch(_POWER_BI_UTILS_POST, return_value=_token_response()) as post:
            client = PowerBiClient(
                "tenant-1", "client-1", username="user@tenant.onmicrosoft.com", password="pw"
            )
            token = client.get_access_token()

        assert token == "tok-1"
        _, kwargs = post.call_args
        assert kwargs["data"]["grant_type"] == "password"
        assert kwargs["data"]["username"] == "user@tenant.onmicrosoft.com"
        assert kwargs["data"]["password"] == "pw"
        assert "client_secret" not in kwargs["data"]

    def test_client_secret_takes_precedence_over_username_password(self):
        with patch(_POWER_BI_UTILS_POST, return_value=_token_response()):
            client = PowerBiClient(
                "tenant-1",
                "client-1",
                client_secret="secret-1",
                username="user@tenant.onmicrosoft.com",
                password="pw",
            )
            client.get_access_token()

        assert client._access_token == "tok-1"

    def test_missing_credentials_raises(self):
        client = PowerBiClient("tenant-1", "client-1")
        with pytest.raises(ValueError, match="client_secret.*username.*password"):
            client.get_access_token()

    def test_missing_tenant_or_client_id_raises(self):
        client = PowerBiClient(None, None, client_secret="secret-1")
        with pytest.raises(ValueError, match="tenant_id.*client_id"):
            client.get_access_token()

    def test_caches_token_across_calls(self):
        with patch(_POWER_BI_UTILS_POST, return_value=_token_response()) as post:
            client = PowerBiClient("tenant-1", "client-1", client_secret="secret-1")
            client.get_access_token()
            client.get_access_token()

        assert post.call_count == 1


class TestPowerBiSemanticModelConnector(LakeflowConnectTests, SupportsPartitionedStreamTests):
    connector_class = PowerBiSemanticModelLakeflowConnect
    simulator_source = "power_bi_semantic_model"
    sample_records = 100
    replay_config = dict(_REPLAY_CONFIG)
    table_configs = {table: dict(opts) for table, opts in _TABLE_OPTIONS.items()}


# ---------------------------------------------------------------------------
# Scenario tests — connector-specific behaviour the shape suites don't cover
# ---------------------------------------------------------------------------


@pytest.fixture
def simulator():
    """A ``Simulator(SIMULATE)`` bound to the committed Power BI corpus."""
    with Simulator(mode=MODE_SIMULATE, spec_path=_SPEC_PATH, corpus_dir=_CORPUS_DIR) as sim:
        yield sim


@pytest.fixture
def connector(simulator):  # pylint: disable=unused-argument
    """A fresh connector instance under the simulator."""
    return PowerBiSemanticModelLakeflowConnect(dict(_REPLAY_CONFIG))


def _read_all(connector_, table, opts=None):
    """Drain ``read_table`` once, returning the records and the end offset."""
    if opts is None:
        opts = _TABLE_OPTIONS.get(table, {})
    iterator, offset = connector_.read_table(table, {}, opts)
    return list(iterator), offset


def test_list_tables_covers_all_seven(connector):
    assert connector.list_tables() == list(SUPPORTED_TABLES)
    assert len(SUPPORTED_TABLES) == 7
    assert DAX_QUERY_RESULT in SUPPORTED_TABLES


def test_every_table_is_partitioned(connector):
    """Every table fans out; none falls back to the un-partitioned reader."""
    for table in SUPPORTED_TABLES:
        assert connector.is_partitioned(table) is True


# -- snapshot re-emission guard --------------------------------------------


@pytest.mark.parametrize("table", SNAPSHOT_TABLES)
def test_snapshot_emits_once_per_instance(connector, table):
    """Snapshot offsets are always ``{}``, so the guard is instance state.

    A second ``read_table`` on the *same* instance must return nothing (the
    micro-batch loop is over), while a brand-new instance re-emits the full
    snapshot — that is what a new trigger looks like.
    """
    first, first_offset = _read_all(connector, table)
    assert first, f"[{table}] first snapshot read produced no records"
    assert first_offset == {}

    second, second_offset = _read_all(connector, table)
    assert second == [], f"[{table}] snapshot re-emitted within one instance"
    assert second_offset == {}

    fresh = PowerBiSemanticModelLakeflowConnect(dict(_REPLAY_CONFIG))
    refetched, _ = _read_all(fresh, table)
    assert len(refetched) == len(first), (
        f"[{table}] a new connector instance must re-emit the whole snapshot"
    )


@pytest.mark.parametrize("table", SNAPSHOT_TABLES)
def test_snapshot_metadata_has_no_cursor(connector, table):
    meta = connector.read_table_metadata(table, {})
    assert meta["ingestion_type"] == "snapshot"
    assert meta["cursor_field"] is None


# -- scanner workflow -------------------------------------------------------


def test_scanner_tables_share_one_scan_shape(connector):
    """getInfo -> scanStatus -> scanResult produces internally consistent rows.

    ``workspace_id`` / ``dataset_id`` / ``table_name`` are re-derived from the
    nesting of the scan tree, not copied from the flat corpus columns, so the
    three tables must agree on the identifiers they emit.
    """
    tables, _ = _read_all(connector, DATASET_TABLES)
    columns, _ = _read_all(connector, DATASET_COLUMNS)
    measures, _ = _read_all(connector, DATASET_MEASURES)

    assert tables and columns and measures

    table_keys = {(r["workspace_id"], r["dataset_id"], r["name"]) for r in tables}
    for child in (*columns, *measures):
        key = (child["workspace_id"], child["dataset_id"], child["table_name"])
        assert key in table_keys, f"child row {key} references a table absent from dataset_tables"


def test_scanner_has_no_admin_fallback(connector, monkeypatch):
    """Unlike workspaces/datasets, a denied Admin scan is a hard failure.

    ``/admin/workspaces/getInfo`` has no membership-scoped twin, so the
    connector must let ``PowerBiAdminAccessDenied`` propagate rather than
    silently degrade to an empty result set.
    """
    original_post = connector._client.post  # pylint: disable=protected-access

    def denied(url, *args, **kwargs):
        if "workspaces/getInfo" in url:
            raise PowerBiAdminAccessDenied("simulated admin denial")
        return original_post(url, *args, **kwargs)

    monkeypatch.setattr(connector._client, "post", denied)  # pylint: disable=protected-access

    for table in SCANNER_TABLES:
        partitions = connector.get_partitions(table, {})
        assert partitions
        with pytest.raises(PowerBiAdminAccessDenied):
            list(connector.read_partition(table, partitions[0], {}))


def test_workspaces_falls_back_when_admin_denied(connector, monkeypatch):
    """The contrast case: workspaces *does* degrade to ``GET /groups``."""
    original_get = connector._client.get  # pylint: disable=protected-access
    seen: list[str] = []

    def denied(url, *args, **kwargs):
        seen.append(url)
        if "/admin/groups" in url:
            raise PowerBiAdminAccessDenied("simulated admin denial")
        return original_get(url, *args, **kwargs)

    monkeypatch.setattr(connector._client, "get", denied)  # pylint: disable=protected-access

    records, _ = _read_all(connector, WORKSPACES)
    assert records, "workspaces should fall back to the non-admin endpoint"
    assert any(u.endswith("/myorg/groups") or "/myorg/groups?" in u for u in seen)


def test_datasets_falls_back_when_admin_denied(connector, monkeypatch):
    original_get = connector._client.get  # pylint: disable=protected-access

    def denied(url, *args, **kwargs):
        if "/admin/datasets" in url:
            raise PowerBiAdminAccessDenied("simulated admin denial")
        return original_get(url, *args, **kwargs)

    monkeypatch.setattr(connector._client, "get", denied)  # pylint: disable=protected-access

    records, _ = _read_all(connector, DATASETS)
    assert records, "datasets should fall back to the per-workspace endpoint"
    # The per-workspace endpoint doesn't echo the workspace back; the
    # connector has to stamp it on.
    assert all(r.get("workspaceId") for r in records)


# -- refresh history (append) ----------------------------------------------


def test_refresh_history_is_append_with_start_time_cursor(connector):
    meta = connector.read_table_metadata(DATASET_REFRESH_HISTORY, {})
    assert meta["ingestion_type"] == "append"
    assert meta["cursor_field"] == "startTime"


def test_refresh_history_first_read_excludes_future_records(connector):
    """The spec synthesises future-dated refreshes; the init-time cap drops them.

    ``latest_offset`` is pinned to the instance's construction time, and
    ``_fetch_refreshes`` applies that as a client-side ``until`` bound because
    the REST API has no since/until filter of its own.
    """
    records, offset = _read_all(connector, DATASET_REFRESH_HISTORY)
    assert records
    until = parse_iso(offset["cursor"])
    assert all(parse_iso(r["startTime"]) <= until for r in records)
    # The spec synthesises 3 future rows per dataset; if the cap regressed
    # they would show up here, so prove they exist and were dropped.
    raw = connector._client.get(  # pylint: disable=protected-access
        f"{connector._base_url}/groups/w/datasets/d/refreshes",  # pylint: disable=protected-access
        params={"$top": "60"},
    )
    future = [r for r in raw["value"] if parse_iso(r["startTime"]) > until]
    assert future, "simulator spec no longer synthesises future-dated refreshes"


def test_refresh_history_second_read_is_empty(connector):
    """Resuming from the returned offset yields nothing and holds the offset."""
    _, first_offset = _read_all(connector, DATASET_REFRESH_HISTORY)
    iterator, second_offset = connector.read_table(DATASET_REFRESH_HISTORY, first_offset, {})
    assert list(iterator) == []
    assert second_offset == first_offset


def test_refresh_history_never_backfills_terminal_status(connector):
    """Append-only: an already-emitted refresh is never re-read.

    The cursor bound is strict (``startTime > since``), so a refresh whose
    status later flips from ``Unknown`` to ``Completed`` keeps whatever status
    it had when it was first emitted. That is intentional — re-emitting it
    would duplicate the row on an append-only table.
    """
    records, offset = _read_all(connector, DATASET_REFRESH_HISTORY)
    assert records
    emitted = {(r["dataset_id"], r["requestId"]) for r in records}

    # Same window, replayed from the returned cursor: nothing comes back,
    # even though every one of those rows is still served by the API.
    iterator, _ = connector.read_table(DATASET_REFRESH_HISTORY, offset, {})
    assert not [r for r in iterator if (r["dataset_id"], r["requestId"]) in emitted]


def test_refresh_history_resumes_mid_fanout(connector):
    """``max_records_per_batch`` splits the fan-out without truncating a dataset.

    A partial batch carries the *original* window forward plus a
    ``dataset_index``; only the final batch advances the cursor.
    """
    opts = {"max_records_per_batch": "1"}
    seen: list[tuple] = []
    offset: dict = {}
    for _ in range(50):
        iterator, next_offset = connector.read_table(DATASET_REFRESH_HISTORY, offset, opts)
        seen.extend((r["dataset_id"], r["requestId"]) for r in iterator)
        if next_offset == offset:
            break
        offset = next_offset
    else:
        pytest.fail("mid-fan-out resume did not converge in 50 iterations")

    assert seen, "resumable read produced no records"
    assert len(seen) == len(set(seen)), "mid-fan-out resume duplicated records"
    assert offset["dataset_index"] == 0


# -- dax_query_result (executeQueries) --------------------------------------
#
# The simulator's handler returns a fixed 5-row result whose columns are the
# five DAX names in ``_DAX_COLUMN_SPECS``; the last row is the BLANK() case
# (``Sales[OrderDate]`` and ``Sales[Amount]`` both null).


def _dax_post_urls(connector_, monkeypatch) -> list[str]:
    """Record every executeQueries URL the connector POSTs to."""
    seen: list[str] = []
    original_post = connector_._client.post  # pylint: disable=protected-access

    def spy(url, *args, **kwargs):
        if "executeQueries" in url:
            seen.append(url)
        return original_post(url, *args, **kwargs)

    monkeypatch.setattr(connector_._client, "post", spy)  # pylint: disable=protected-access
    return seen


def test_dax_is_empty_until_a_query_is_configured(connector):
    """No ``dax_query`` means no work — by design, not a bug.

    The other six tables share the pipeline with this one; a pipeline that
    never opted into DAX must not be failed by it, so the table degrades to
    zero partitions rather than raising.
    """
    assert connector.get_partitions(DAX_QUERY_RESULT, {}) == []
    records, offset = _read_all(connector, DAX_QUERY_RESULT, {})
    assert records == []
    assert offset == {}


def test_dax_missing_query_still_reports_metadata_and_schema(connector):
    """Discovery must work on an un-configured table, or planning breaks."""
    meta = connector.read_table_metadata(DAX_QUERY_RESULT, {})
    assert meta["ingestion_type"] == "snapshot"
    assert meta["primary_keys"] == ["dataset_id", "query_hash", "row_index"]
    assert meta["cursor_field"] is None
    assert connector.get_table_schema(DAX_QUERY_RESULT, {}) is DAX_QUERY_RESULT_SCHEMA


def test_dax_is_a_single_unpaged_partition(connector, monkeypatch):
    """executeQueries is one POST: not pageable, no range filter, no fan-out.

    The endpoint's budget is 120 requests/minute for the whole *tenant*, so a
    regression that paged or fanned this out would be expensive and silent.
    """
    partitions = connector.get_partitions(DAX_QUERY_RESULT, _DAX_TYPED_OPTIONS)
    assert len(partitions) == 1
    assert partitions[0] == {"kind": "dax_query"}

    posts = _dax_post_urls(connector, monkeypatch)
    records = list(connector.read_partition(DAX_QUERY_RESULT, partitions[0], _DAX_TYPED_OPTIONS))
    assert records
    assert len(posts) == 1, f"expected exactly one executeQueries POST, got {posts}"
    assert posts[0].endswith("/groups/ws-1/datasets/ds-1/executeQueries")


def test_dax_typed_mode_builds_declared_columns(connector):
    """``dax_columns`` turns the result into real Spark columns."""
    schema = connector.get_table_schema(DAX_QUERY_RESULT, _DAX_TYPED_OPTIONS)
    by_name = {field.name: field for field in schema.fields}

    # Connector-contributed identity + trailer columns survive in both modes.
    for name in (
        "workspace_id",
        "dataset_id",
        "query_hash",
        "row_index",
        "row_json",
        "truncated",
        "ingestion_timestamp",
    ):
        assert name in by_name, f"{name} missing from the typed schema"
    assert "columns" not in by_name, "the map fallback must not appear once dax_columns is declared"

    assert isinstance(by_name["region"].dataType, StringType)
    assert isinstance(by_name["order_date"].dataType, TimestampType)
    assert isinstance(by_name["amount"].dataType, DoubleType)
    assert isinstance(by_name["total_units"].dataType, LongType)
    assert isinstance(by_name["is_target_met"].dataType, BooleanType)
    # DAX emits BLANK() freely, so every declared column has to be nullable.
    assert all(by_name[spec["name"]].nullable for spec in _DAX_COLUMN_SPECS)


def test_dax_typed_mode_lands_values_under_the_spark_names(connector):
    records, _ = _read_all(connector, DAX_QUERY_RESULT, _DAX_TYPED_OPTIONS)
    assert len(records) == 5

    first = records[0]
    assert first["region"] == "North"
    assert first["amount"] == 15230.75
    assert first["total_units"] == 412
    assert first["is_target_met"] is True
    # The bracketed DAX names never leak into the record.
    assert not [key for key in first if "[" in key]

    # BLANK() with includeNulls arrives as a JSON null, not a dropped key.
    blank_row = records[-1]
    assert blank_row["region"] == "Unassigned"
    assert blank_row["order_date"] is None
    assert blank_row["amount"] is None


def test_dax_map_mode_keeps_dax_names_and_stringifies(connector):
    """Without ``dax_columns`` the query's own columns live in a string map."""
    schema = connector.get_table_schema(DAX_QUERY_RESULT, _DAX_MAP_OPTIONS)
    assert schema == DAX_QUERY_RESULT_SCHEMA
    columns_field = schema["columns"]
    assert isinstance(columns_field.dataType, MapType)
    assert isinstance(columns_field.dataType.keyType, StringType)
    assert isinstance(columns_field.dataType.valueType, StringType)

    records, _ = _read_all(connector, DAX_QUERY_RESULT, _DAX_MAP_OPTIONS)
    assert len(records) == 5

    expected_keys = {spec["dax"] for spec in _DAX_COLUMN_SPECS}
    for record in records:
        assert set(record["columns"]) == expected_keys
        assert all(value is None or isinstance(value, str) for value in record["columns"].values())

    first = records[0]["columns"]
    assert first["Sales[Region]"] == "North"
    assert first["Sales[Amount]"] == "15230.75"
    assert first["[Total Units]"] == "412"
    # JSON's "true"/"false" round-trips better than Python's "True"/"False".
    assert first["[Is Target Met]"] == "true"
    assert records[-1]["columns"]["Sales[Amount]"] is None


@pytest.mark.parametrize("options", [_DAX_MAP_OPTIONS, _DAX_TYPED_OPTIONS])
def test_dax_identity_and_trailer_columns_are_mode_independent(connector, options):
    """Both schema modes carry the same connector-contributed columns."""
    records, _ = _read_all(connector, DAX_QUERY_RESULT, options)
    assert records

    for index, record in enumerate(records):
        assert record["workspace_id"] == "ws-1"
        assert record["dataset_id"] == "ds-1"
        assert record["query_hash"] == query_fingerprint(_DAX_QUERY)
        assert record["row_index"] == index
        assert record["truncated"] is False
        assert record["ingestion_timestamp"]
        # row_json is the lossless copy, kept in both modes — it is where
        # columns the user did not declare in dax_columns survive.
        assert json.loads(record["row_json"]).keys() == {spec["dax"] for spec in _DAX_COLUMN_SPECS}


def _read_dax_on_a_fresh_instance(options):
    """Read ``dax_query_result`` on a brand-new connector.

    ``_snapshot_emitted`` is keyed by table name alone, so one instance emits
    the DAX table exactly once no matter what options the second call passes.
    Any test that needs two different configurations has to use two instances —
    which is also what two pipeline triggers actually look like.
    """
    fresh = PowerBiSemanticModelLakeflowConnect(dict(_REPLAY_CONFIG))
    records, _ = _read_all(fresh, DAX_QUERY_RESULT, options)
    return records


def test_dax_query_hash_ignores_reformatting_but_not_edits(simulator):  # pylint: disable=unused-argument
    """The PK is (dataset, query, row) — reformatting must not orphan rows."""
    baseline = _read_dax_on_a_fresh_instance(_DAX_MAP_OPTIONS)
    same = _read_dax_on_a_fresh_instance(
        dict(_DAX_MAP_OPTIONS, dax_query=f"  {_DAX_QUERY.replace(' ', '  ')}  ")
    )
    different = _read_dax_on_a_fresh_instance(dict(_DAX_MAP_OPTIONS, dax_query="EVALUATE Products"))

    assert baseline and same and different
    assert baseline[0]["query_hash"] == same[0]["query_hash"]
    assert baseline[0]["query_hash"] != different[0]["query_hash"]


def test_dax_request_body_carries_serializer_and_impersonation(
    simulator,
    monkeypatch,  # pylint: disable=unused-argument
):
    """``includeNulls`` and RLS impersonation have to reach the wire.

    Without ``includeNulls`` a BLANK() cell is dropped from the row object
    entirely, which silently shifts a typed column to null for the wrong rows.
    """
    bodies: list[dict] = []

    def read_capturing_body(options):
        fresh = PowerBiSemanticModelLakeflowConnect(dict(_REPLAY_CONFIG))
        original_post = fresh._client.post  # pylint: disable=protected-access

        def spy(url, *args, **kwargs):
            if "executeQueries" in url:
                bodies.append(kwargs.get("json_body"))
            return original_post(url, *args, **kwargs)

        monkeypatch.setattr(fresh._client, "post", spy)  # pylint: disable=protected-access
        _read_all(fresh, DAX_QUERY_RESULT, options)

    read_capturing_body(_DAX_MAP_OPTIONS)
    assert bodies[0]["queries"] == [{"query": _DAX_QUERY}]
    assert bodies[0]["serializerSettings"] == {"includeNulls": True}
    assert "impersonatedUserName" not in bodies[0]

    read_capturing_body(
        dict(
            _DAX_MAP_OPTIONS,
            include_nulls="false",
            impersonated_user_name="analyst@contoso.com",
        )
    )
    assert bodies[1]["serializerSettings"] == {"includeNulls": False}
    assert bodies[1]["impersonatedUserName"] == "analyst@contoso.com"


def test_dax_infers_the_model_from_singular_allow_lists(connector):
    """A pipeline already scoped by ``dataset_ids`` need not repeat itself."""
    records, _ = _read_all(
        connector,
        DAX_QUERY_RESULT,
        {
            "dax_query": _DAX_QUERY,
            "workspace_ids": "ws-solo",
            "dataset_ids": "ds-solo",
        },
    )
    assert records
    assert records[0]["workspace_id"] == "ws-solo"
    assert records[0]["dataset_id"] == "ds-solo"


@pytest.mark.parametrize(
    "options, expected",
    [
        # Query set but no model to run it against.
        ({"dax_query": _DAX_QUERY}, "workspace_id"),
        ({"dax_query": _DAX_QUERY, "workspace_id": "ws-1"}, "dataset_id"),
        # Ambiguous: one DAX query is only meaningful against one model.
        (
            {
                "dax_query": _DAX_QUERY,
                "workspace_ids": "ws-1,ws-2",
                "dataset_id": "ds-1",
            },
            "workspace_id",
        ),
        (
            {
                "dax_query": _DAX_QUERY,
                "workspace_id": "ws-1",
                "dataset_ids": "ds-1,ds-2",
            },
            "dataset_id",
        ),
    ],
)
def test_dax_misconfigured_model_raises(connector, options, expected):
    with pytest.raises(ValueError) as excinfo:
        _read_all(connector, DAX_QUERY_RESULT, options)
    assert expected in str(excinfo.value)


@pytest.mark.parametrize(
    "dax_columns, expected",
    [
        ("not json at all", "not valid JSON"),
        ('{"dax": "Sales[Region]"}', "must be a JSON array"),
        ("[42]", "must be an object or a string"),
        ('[{"name": "region"}]', "missing the required 'dax' key"),
        # Reserved: would collide with a connector-contributed column.
        ('[{"dax": "Sales[Region]", "name": "row_json"}]', "reserved column name"),
        ('[{"dax": "Sales[Amount]", "name": "columns"}]', "reserved column name"),
        (
            '[{"dax": "Sales[Region]", "name": "r"}, {"dax": "Sales[Amount]", "name": "r"}]',
            "more than once",
        ),
        (
            '[{"dax": "Sales[Amount]", "name": "amount", "type": "geography"}]',
            "unsupported type",
        ),
    ],
)
def test_dax_bad_columns_option_raises(connector, dax_columns, expected):
    """A typo in ``dax_columns`` must fail loudly, not land an all-null column.

    It has to fail on both surfaces: schema resolution happens at planning
    time, well before any read.
    """
    options = dict(_DAX_MAP_OPTIONS, dax_columns=dax_columns)

    with pytest.raises(ValueError) as schema_error:
        connector.get_table_schema(DAX_QUERY_RESULT, options)
    assert expected in str(schema_error.value)

    with pytest.raises(ValueError) as read_error:
        _read_all(connector, DAX_QUERY_RESULT, options)
    assert expected in str(read_error.value)


def test_dax_bare_string_column_is_shorthand(connector):
    """``"Sales[Region]"`` == ``{"dax": ..., "name": ..., "type": "string"}``."""
    options = dict(_DAX_MAP_OPTIONS, dax_columns='["Sales[Region]"]')
    schema = connector.get_table_schema(DAX_QUERY_RESULT, options)
    field = schema["Sales[Region]"]
    assert isinstance(field.dataType, StringType)

    records, _ = _read_all(connector, DAX_QUERY_RESULT, options)
    assert records[0]["Sales[Region]"] == "North"


def test_dax_declared_column_the_query_never_returns_is_null(connector):
    """A declared-but-absent column nulls out rather than failing the batch.

    It is a configuration error, but row_json still carries the truth, so
    failing every row would cost more than it catches.
    """
    options = dict(
        _DAX_MAP_OPTIONS,
        dax_columns=json.dumps(
            [
                {"dax": "Sales[Region]", "name": "region", "type": "string"},
                {"dax": "Sales[Nonexistent]", "name": "ghost", "type": "string"},
            ]
        ),
    )
    records, _ = _read_all(connector, DAX_QUERY_RESULT, options)
    assert records
    assert all(record["ghost"] is None for record in records)
    assert records[0]["region"] == "North"
    assert "Sales[Nonexistent]" not in json.loads(records[0]["row_json"])


def test_dax_error_envelope_is_raised_not_swallowed(connector):
    """executeQueries reports per-result errors in a 200 body.

    Returning an empty table for those would look like "the model has no
    matching rows", which is exactly the wrong signal for a broken query.
    """
    extract = type(connector)._extract_dax_rows  # pylint: disable=protected-access

    for envelope in (
        {"error": {"code": "DatasetExecuteQueriesError"}},
        {"results": [{"error": {"code": "QueryUserError"}}]},
        {"results": [{"tables": [{"error": {"code": "TableError"}}]}]},
    ):
        with pytest.raises(PowerBiApiError):
            extract(envelope)


def test_dax_snapshot_emits_once_then_re_emits_on_a_new_instance(connector):
    """The snapshot guard is instance state, and it covers DAX too.

    Its offset is always ``{}``, so a second read within one trigger has to be
    empty; a new trigger builds a new connector and re-runs the query.
    """
    first, first_offset = _read_all(connector, DAX_QUERY_RESULT, _DAX_TYPED_OPTIONS)
    assert len(first) == 5
    assert first_offset == {}

    second, _ = _read_all(connector, DAX_QUERY_RESULT, _DAX_TYPED_OPTIONS)
    assert second == []

    fresh = PowerBiSemanticModelLakeflowConnect(dict(_REPLAY_CONFIG))
    refetched, _ = _read_all(fresh, DAX_QUERY_RESULT, _DAX_TYPED_OPTIONS)
    assert len(refetched) == len(first)
