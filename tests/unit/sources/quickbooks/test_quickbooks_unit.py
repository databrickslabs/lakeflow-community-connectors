"""Focused failure-mode and normalization tests for QuickBooks Online."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import Mock

import pytest
import requests

from databricks.labs.community_connector.sources.quickbooks import quickbooks as quickbooks_module
from databricks.labs.community_connector.sources.quickbooks.quickbooks import (
    QuickBooksApiClient,
    QuickBooksLakeflowConnect,
    _normalize_delete_entity,
    _normalize_entity,
    _retry_after_seconds,
)


def _options(**overrides: str) -> dict[str, str]:
    return {
        "access_token": "token",
        "realm_id": "realm",
        "environment": "sandbox",
        **overrides,
    }


def _checkpoint(
    updated_through: str,
    realm_id: str = "realm",
    table_name: str = "customers",
    flow: str = "updates",
) -> dict:
    return {
        "version": 2,
        "realm_id": realm_id,
        "table_name": table_name,
        "flow": flow,
        "updated_through": updated_through,
    }


def _response(status: int, payload: object | None = None, **headers: str) -> Mock:
    response = Mock(spec=requests.Response)
    response.status_code = status
    response.headers = headers
    response.json.return_value = payload
    if status >= 400:
        response.raise_for_status.side_effect = requests.HTTPError(str(status))
    else:
        response.raise_for_status.return_value = None
    return response


@pytest.mark.parametrize("missing", ["access_token", "realm_id"])
def test_required_connection_values(missing: str) -> None:
    options = _options()
    options[missing] = " "
    with pytest.raises(ValueError, match=missing):
        QuickBooksLakeflowConnect(options)


def test_invalid_environment() -> None:
    with pytest.raises(ValueError, match="environment"):
        QuickBooksLakeflowConnect(_options(environment="staging"))


def test_table_discovery_and_specialized_schemas() -> None:
    connector = QuickBooksLakeflowConnect(_options())
    assert connector.list_tables() == [
        "customers",
        "vendors",
        "accounts",
        "items",
        "invoices",
        "bills",
    ]
    assert "primary_email" in connector.get_table_schema("customers", {}).fieldNames()
    assert "account_type" in connector.get_table_schema("accounts", {}).fieldNames()
    assert "quantity_on_hand" in connector.get_table_schema("items", {}).fieldNames()
    assert "customer_ref" in connector.get_table_schema("invoices", {}).fieldNames()
    assert "vendor_ref" in connector.get_table_schema("bills", {}).fieldNames()
    assert connector.get_table_schema("customers", {})["realm_id"].nullable is False


def test_table_metadata_distinguishes_inactivation_from_hard_deletes() -> None:
    connector = QuickBooksLakeflowConnect(_options())

    for table in ("customers", "vendors", "accounts", "items"):
        assert connector.read_table_metadata(table, {}) == {
            "primary_keys": ["realm_id", "id"],
            "cursor_field": "last_updated_at",
            "ingestion_type": "cdc",
        }
    for table in ("invoices", "bills"):
        assert connector.read_table_metadata(table, {}) == {
            "primary_keys": ["realm_id", "id"],
            "cursor_field": "last_updated_at",
            "ingestion_type": "cdc_with_deletes",
        }


def test_identical_source_ids_in_two_realms_have_distinct_composite_keys() -> None:
    source_row = {
        "Id": "1",
        "MetaData": {"LastUpdatedTime": "2026-07-26T12:00:00Z"},
    }

    realm_a = _normalize_entity("customers", source_row, realm_id="realm-a")
    realm_b = _normalize_entity("customers", source_row, realm_id="realm-b")

    assert (realm_a["realm_id"], realm_a["id"]) == ("realm-a", "1")
    assert (realm_b["realm_id"], realm_b["id"]) == ("realm-b", "1")
    assert (realm_a["realm_id"], realm_a["id"]) != (realm_b["realm_id"], realm_b["id"])


def test_one_tenant_ingestion_failure_does_not_affect_another_tenant() -> None:
    tenant_a = QuickBooksLakeflowConnect(_options(realm_id="realm-a"))
    tenant_b = QuickBooksLakeflowConnect(_options(realm_id="realm-b"))
    tenant_a._client.iter_entity = Mock(  # noqa: SLF001
        return_value=iter([{"Id": "1"}])
    )
    tenant_b._client.iter_entity = Mock(  # noqa: SLF001
        return_value=iter(
            [
                {
                    "Id": "1",
                    "MetaData": {"LastUpdatedTime": "2026-07-26T12:00:00Z"},
                }
            ]
        )
    )

    failed_records, _ = tenant_a.read_table("customers", {}, {})
    with pytest.raises(RuntimeError, match="LastUpdatedTime"):
        list(failed_records)

    healthy_records, _ = tenant_b.read_table("customers", {}, {})
    assert [(row["realm_id"], row["id"]) for row in healthy_records] == [
        ("realm-b", "1")
    ]


@pytest.mark.parametrize("table,entity", quickbooks_module.TABLE_TO_ENTITY.items())
def test_all_tables_use_versioned_snapshot_to_incremental_handoff(
    monkeypatch: pytest.MonkeyPatch,
    table: str,
    entity: str,
) -> None:
    monkeypatch.setattr(
        quickbooks_module,
        "_utc_now",
        lambda: datetime(2026, 7, 26, 12, 30, tzinfo=timezone.utc),
    )
    get = Mock(
        side_effect=[
            _response(
                200,
                {
                    "QueryResponse": {
                        entity: [
                            {
                                "Id": "1",
                                "MetaData": {
                                    "LastUpdatedTime": "2026-07-26T12:00:00Z",
                                },
                            }
                        ]
                    }
                },
            ),
            _response(200, {"QueryResponse": {entity: []}}),
        ]
    )
    monkeypatch.setattr(requests, "get", get)
    connector = QuickBooksLakeflowConnect(_options())

    snapshot_records, snapshot_offset = connector.read_table(table, {}, {})
    assert [record["id"] for record in snapshot_records] == ["1"]
    assert snapshot_offset == _checkpoint("2026-07-26T12:30:00Z", table_name=table)
    assert all(record["realm_id"] == "realm" for record in snapshot_records)
    snapshot_query = get.call_args_list[0].kwargs["params"]["query"]
    assert f"SELECT * FROM {entity}" in snapshot_query
    if table in quickbooks_module.LIST_TABLES:
        assert "WHERE Active IN (true, false)" in snapshot_query
    else:
        assert " WHERE " not in snapshot_query

    incremental_records, incremental_offset = connector.read_table(
        table,
        _checkpoint("2026-07-26T11:30:00Z", table_name=table),
        {"incremental_overlap_seconds": "60"},
    )
    assert list(incremental_records) == []
    assert incremental_offset == snapshot_offset
    incremental_query = get.call_args_list[1].kwargs["params"]["query"]
    assert f"SELECT * FROM {entity} WHERE" in incremental_query
    if table in quickbooks_module.LIST_TABLES:
        assert "Active IN (true, false) AND" in incremental_query
    assert "MetaData.LastUpdatedTime >= '2026-07-26T11:29:00Z'" in incremental_query
    assert "MetaData.LastUpdatedTime <= '2026-07-26T12:30:00Z'" in incremental_query


def test_customer_first_read_is_snapshot_with_versioned_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    init_time = datetime(2026, 7, 26, 12, 30, tzinfo=timezone.utc)
    monkeypatch.setattr(quickbooks_module, "_utc_now", lambda: init_time)
    get = Mock(
        return_value=_response(
            200,
            {
                "QueryResponse": {
                    "Customer": [
                        {
                            "Id": "1",
                            "MetaData": {
                                "LastUpdatedTime": "2026-07-26T12:00:00Z",
                            },
                        }
                    ]
                }
            },
        )
    )
    monkeypatch.setattr(requests, "get", get)
    connector = QuickBooksLakeflowConnect(_options())

    records, end_offset = connector.read_table("customers", {}, {"page_size": "10"})

    assert [record["id"] for record in records] == ["1"]
    assert end_offset == _checkpoint("2026-07-26T12:30:00Z")
    assert "WHERE Active IN (true, false)" in get.call_args.kwargs["params"]["query"]

    records, repeated_offset = connector.read_table(
        "customers",
        end_offset,
        {"page_size": "10"},
    )
    assert list(records) == []
    assert repeated_offset == end_offset
    assert get.call_count == 1


def test_customer_incremental_read_uses_overlap_and_bounded_upper_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        quickbooks_module,
        "_utc_now",
        lambda: datetime(2026, 7, 26, 12, 30, tzinfo=timezone.utc),
    )
    get = Mock(
        return_value=_response(
            200,
            {
                "QueryResponse": {
                    "Customer": [
                        {
                            "Id": "2",
                            "MetaData": {
                                "LastUpdatedTime": "2026-07-25T12:00:00Z",
                            },
                        }
                    ]
                }
            },
        )
    )
    monkeypatch.setattr(requests, "get", get)
    connector = QuickBooksLakeflowConnect(_options())
    start_offset = _checkpoint("2026-07-25T11:30:00Z")

    records, end_offset = connector.read_table(
        "customers",
        start_offset,
        {
            "page_size": "10",
            "incremental_overlap_seconds": "60",
            "max_incremental_window_seconds": "3600",
        },
    )

    assert [record["id"] for record in records] == ["2"]
    assert end_offset == _checkpoint("2026-07-25T12:30:00Z")
    query = get.call_args.kwargs["params"]["query"]
    assert "MetaData.LastUpdatedTime >= '2026-07-25T11:29:00Z'" in query
    assert "MetaData.LastUpdatedTime <= '2026-07-25T12:30:00Z'" in query


def test_customer_incremental_replay_is_deterministic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        quickbooks_module,
        "_utc_now",
        lambda: datetime(2026, 7, 26, 12, 30, tzinfo=timezone.utc),
    )
    response = _response(200, {"QueryResponse": {"Customer": []}})
    get = Mock(return_value=response)
    monkeypatch.setattr(requests, "get", get)
    connector = QuickBooksLakeflowConnect(_options())
    start_offset = _checkpoint("2026-07-26T10:30:00Z")

    first_records, first_offset = connector.read_table("customers", start_offset, {})
    second_records, second_offset = connector.read_table("customers", start_offset, {})

    assert list(first_records) == []
    assert list(second_records) == []
    assert first_offset == second_offset
    assert (
        get.call_args_list[0].kwargs["params"]["query"]
        == (get.call_args_list[1].kwargs["params"]["query"])
    )


def test_customer_incremental_failure_replays_from_same_offset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        quickbooks_module,
        "_utc_now",
        lambda: datetime(2026, 7, 26, 12, 30, tzinfo=timezone.utc),
    )
    get = Mock(
        side_effect=[
            _response(200, {"QueryResponse": {"Customer": [{"Id": "missing-metadata"}]}}),
            _response(200, {"QueryResponse": {"Customer": []}}),
        ]
    )
    monkeypatch.setattr(requests, "get", get)
    connector = QuickBooksLakeflowConnect(_options())
    start_offset = _checkpoint("2026-07-26T10:30:00Z")

    with pytest.raises(RuntimeError, match="LastUpdatedTime"):
        connector.read_table("customers", start_offset, {})

    replayed_records, replayed_end_offset = connector.read_table("customers", start_offset, {})

    assert list(replayed_records) == []
    assert replayed_end_offset == _checkpoint("2026-07-26T12:30:00Z")
    assert (
        get.call_args_list[0].kwargs["params"]["query"]
        == get.call_args_list[1].kwargs["params"]["query"]
    )


@pytest.mark.parametrize(
    "offset,match",
    [
        ({"updated_through": "2026-07-26T10:00:00Z"}, "version"),
        (
            {
                "version": 99,
                "realm_id": "realm",
                "updated_through": "2026-07-26T10:00:00Z",
            },
            "version",
        ),
        ({"version": 2, "updated_through": "2026-07-26T10:00:00Z"}, "realm_id"),
        (_checkpoint("2026-07-26T10:00:00Z", realm_id="other"), "realm_id"),
        (_checkpoint("2026-07-26T10:00:00Z", table_name="vendors"), "table_name"),
        (_checkpoint("2026-07-26T10:00:00Z", flow="deletes"), "flow"),
        (
            {
                "version": 2,
                "realm_id": "realm",
                "table_name": "customers",
                "flow": "updates",
            },
            "updated_through",
        ),
        (_checkpoint("not-a-time"), "timestamp"),
    ],
)
def test_customer_offset_validation(offset: dict, match: str) -> None:
    connector = QuickBooksLakeflowConnect(_options())

    with pytest.raises(ValueError, match=match):
        connector.read_table("customers", offset, {})


@pytest.mark.parametrize("table,entity", quickbooks_module.TABLE_TO_ENTITY.items())
def test_cdc_requires_last_updated_time(
    monkeypatch: pytest.MonkeyPatch,
    table: str,
    entity: str,
) -> None:
    get = Mock(
        return_value=_response(
            200,
            {"QueryResponse": {entity: [{"Id": "1"}]}},
        )
    )
    monkeypatch.setattr(requests, "get", get)
    connector = QuickBooksLakeflowConnect(_options())

    records, _ = connector.read_table(table, {}, {})

    with pytest.raises(RuntimeError, match="LastUpdatedTime"):
        list(records)


@pytest.mark.parametrize(
    "options,match",
    [
        ({"incremental_overlap_seconds": "-1"}, "incremental_overlap_seconds"),
        ({"incremental_overlap_seconds": "bad"}, "incremental_overlap_seconds"),
        ({"max_incremental_window_seconds": "59"}, "max_incremental_window_seconds"),
        ({"max_incremental_window_seconds": "bad"}, "max_incremental_window_seconds"),
        ({"max_records_per_batch": "0"}, "max_records_per_batch"),
        ({"max_records_per_batch": "bad"}, "max_records_per_batch"),
    ],
)
def test_customer_incremental_option_validation(
    options: dict[str, str],
    match: str,
) -> None:
    connector = QuickBooksLakeflowConnect(_options())
    start_offset = _checkpoint("2026-07-20T10:00:00Z")

    with pytest.raises(ValueError, match=match):
        connector.read_table("customers", start_offset, options)


def test_incremental_admission_control_shrinks_and_drains_complete_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        quickbooks_module,
        "_utc_now",
        lambda: datetime(2026, 7, 26, 12, 30, tzinfo=timezone.utc),
    )
    connector = QuickBooksLakeflowConnect(_options())
    calls: list[tuple[int, str]] = []

    def iter_entity(
        _entity: str,
        *,
        page_size: int,
        where_clause: str,
    ):
        calls.append((page_size, where_clause))
        upper = "2026-07-26T12:30:00Z"
        count = 3
        if upper not in where_clause:
            count = 2
        return iter(
            {
                "Id": str(index),
                "MetaData": {"LastUpdatedTime": "2026-07-26T11:45:00Z"},
            }
            for index in range(count)
        )

    connector._client.iter_entity = iter_entity  # type: ignore[method-assign]  # noqa: SLF001
    records, end_offset = connector.read_table(
        "customers",
        _checkpoint("2026-07-26T11:30:00Z"),
        {
            "incremental_overlap_seconds": "0",
            "max_incremental_window_seconds": "3600",
            "max_records_per_batch": "2",
            "page_size": "1000",
        },
    )

    assert [record["id"] for record in records] == ["0", "1"]
    assert end_offset == _checkpoint("2026-07-26T12:00:00Z")
    assert len(calls) == 2
    assert all(page_size == 2 for page_size, _ in calls)
    assert "LastUpdatedTime <= '2026-07-26T12:30:00Z'" in calls[0][1]
    assert "LastUpdatedTime <= '2026-07-26T12:00:00Z'" in calls[1][1]


def test_incremental_admission_control_keeps_indivisible_timestamp_cohort(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        quickbooks_module,
        "_utc_now",
        lambda: datetime(2026, 7, 26, 12, 30, tzinfo=timezone.utc),
    )
    connector = QuickBooksLakeflowConnect(_options())
    connector._client.iter_entity = Mock(  # noqa: SLF001
        return_value=iter(
            [
                {
                    "Id": "1",
                    "MetaData": {"LastUpdatedTime": "2026-07-26T12:30:00Z"},
                },
                {
                    "Id": "2",
                    "MetaData": {"LastUpdatedTime": "2026-07-26T12:30:00Z"},
                },
            ]
        )
    )

    records, end_offset = connector.read_table(
        "customers",
        _checkpoint("2026-07-26T12:29:59Z"),
        {
            "incremental_overlap_seconds": "0",
            "max_records_per_batch": "1",
        },
    )

    assert [record["id"] for record in records] == ["1", "2"]
    assert end_offset == _checkpoint("2026-07-26T12:30:00Z")


def test_delete_admission_control_fails_without_advancing_checkpoint() -> None:
    connector = QuickBooksLakeflowConnect(_options())
    connector._client.get_entity_changes = Mock(  # noqa: SLF001
        return_value=(
            [
                {
                    "Id": "1",
                    "status": "Deleted",
                    "MetaData": {"LastUpdatedTime": "2026-07-26T12:01:00Z"},
                },
                {
                    "Id": "2",
                    "status": "Deleted",
                    "MetaData": {"LastUpdatedTime": "2026-07-26T12:01:01Z"},
                },
            ],
            "2026-07-26T12:02:00Z",
        )
    )

    with pytest.raises(RuntimeError, match="max_records_per_batch"):
        connector.read_table_deletes(
            "invoices",
            _checkpoint(
                "2026-07-26T12:00:00Z",
                table_name="invoices",
                flow="deletes",
            ),
            {"max_records_per_batch": "1"},
        )


def test_pagination_requests_empty_page_after_exact_full_page(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = [
        _response(200, {"QueryResponse": {"Customer": [{"Id": "1"}, {"Id": "2"}]}}),
        _response(200, {"QueryResponse": {"Customer": []}}),
    ]
    get = Mock(side_effect=responses)
    monkeypatch.setattr(requests, "get", get)
    client = QuickBooksApiClient(
        access_token="token",
        realm_id="realm",
        environment="sandbox",
        minor_version=75,
    )

    assert [row["Id"] for row in client.iter_entity("Customer", page_size=2)] == ["1", "2"]
    assert get.call_count == 2
    assert "STARTPOSITION 1 MAXRESULTS 2" in get.call_args_list[0].kwargs["params"]["query"]
    assert "STARTPOSITION 3 MAXRESULTS 2" in get.call_args_list[1].kwargs["params"]["query"]


def test_incremental_pagination_preserves_where_clause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = [
        _response(200, {"QueryResponse": {"Customer": [{"Id": "1"}, {"Id": "2"}]}}),
        _response(200, {"QueryResponse": {"Customer": [{"Id": "3"}]}}),
    ]
    get = Mock(side_effect=responses)
    monkeypatch.setattr(requests, "get", get)
    client = QuickBooksApiClient(
        access_token="token",
        realm_id="realm",
        environment="sandbox",
        minor_version=75,
    )
    where_clause = (
        "MetaData.LastUpdatedTime >= '2026-07-25T10:00:00Z' "
        "AND MetaData.LastUpdatedTime <= '2026-07-26T10:00:00Z'"
    )

    rows = list(
        client.iter_entity(
            "Customer",
            page_size=2,
            where_clause=where_clause,
        )
    )

    assert [row["Id"] for row in rows] == ["1", "2", "3"]
    for call in get.call_args_list:
        assert f"WHERE {where_clause}" in call.kwargs["params"]["query"]
    assert "STARTPOSITION 1 MAXRESULTS 2" in get.call_args_list[0].kwargs["params"]["query"]
    assert "STARTPOSITION 3 MAXRESULTS 2" in get.call_args_list[1].kwargs["params"]["query"]


def test_cdc_client_parses_entity_changes_and_server_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    get = Mock(
        return_value=_response(
            200,
            {
                "CDCResponse": [
                    {
                        "QueryResponse": [
                            {
                                "Invoice": [
                                    {
                                        "Id": "40",
                                        "status": "Deleted",
                                        "MetaData": {
                                            "LastUpdatedTime": "2026-07-26T12:01:00Z"
                                        },
                                    }
                                ],
                                "maxResults": 1,
                            }
                        ]
                    }
                ],
                "time": "2026-07-26T12:02:00.123Z",
            },
        )
    )
    monkeypatch.setattr(requests, "get", get)
    client = QuickBooksApiClient(
        access_token="token",
        realm_id="realm",
        environment="sandbox",
        minor_version=75,
    )

    rows, response_time = client.get_entity_changes(
        "Invoice",
        changed_since="2026-07-26T12:00:00Z",
    )

    assert [row["Id"] for row in rows] == ["40"]
    assert response_time == "2026-07-26T12:02:00Z"
    assert get.call_args.kwargs["params"]["entities"] == "Invoice"
    assert get.call_args.kwargs["params"]["changedSince"] == "2026-07-26T12:00:00Z"


@pytest.mark.parametrize("table,entity", [("invoices", "Invoice"), ("bills", "Bill")])
def test_transaction_delete_read_emits_schema_complete_tombstone(
    monkeypatch: pytest.MonkeyPatch,
    table: str,
    entity: str,
) -> None:
    monkeypatch.setattr(
        quickbooks_module,
        "_utc_now",
        lambda: datetime(2026, 7, 26, 12, 30, tzinfo=timezone.utc),
    )
    get = Mock(
        return_value=_response(
            200,
            {
                "CDCResponse": [
                    {
                        "QueryResponse": [
                            {
                                entity: [
                                    {
                                        "Id": "deleted-1",
                                        "SyncToken": "4",
                                        "status": "Deleted",
                                        "MetaData": {
                                            "LastUpdatedTime": "2026-07-26T12:29:30Z"
                                        },
                                    },
                                    {
                                        "Id": "updated-1",
                                        "status": "Updated",
                                        "MetaData": {
                                            "LastUpdatedTime": "2026-07-26T12:29:45Z"
                                        },
                                    },
                                ],
                                "maxResults": 2,
                            }
                        ]
                    }
                ],
                "time": "2026-07-26T12:30:01Z",
            },
        )
    )
    monkeypatch.setattr(requests, "get", get)
    connector = QuickBooksLakeflowConnect(_options())

    records, end_offset = connector.read_table_deletes(table, {}, {})
    tombstones = list(records)

    assert len(tombstones) == 1
    assert set(tombstones[0]) == set(connector.get_table_schema(table, {}).fieldNames())
    assert tombstones[0]["id"] == "deleted-1"
    assert tombstones[0]["realm_id"] == "realm"
    assert tombstones[0]["sync_token"] == "4"
    assert tombstones[0]["last_updated_at"] == datetime.fromisoformat(
        "2026-07-26T12:29:30+00:00"
    )
    assert tombstones[0]["raw_json"]
    assert end_offset == _checkpoint(
        "2026-07-26T12:30:01Z",
        table_name=table,
        flow="deletes",
    )
    assert get.call_args.kwargs["params"]["changedSince"] == "2026-07-26T12:25:00Z"


def test_delete_replay_is_deterministic(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        quickbooks_module,
        "_utc_now",
        lambda: datetime(2026, 7, 26, 12, 30, tzinfo=timezone.utc),
    )
    response = _response(
        200,
        {
            "CDCResponse": [{"QueryResponse": [{}]}],
            "time": "2026-07-26T12:30:01Z",
        },
    )
    get = Mock(return_value=response)
    monkeypatch.setattr(requests, "get", get)
    connector = QuickBooksLakeflowConnect(_options())
    start_offset = _checkpoint(
        "2026-07-26T12:00:00Z",
        table_name="invoices",
        flow="deletes",
    )

    first_records, first_offset = connector.read_table_deletes(
        "invoices", start_offset, {}
    )
    second_records, second_offset = connector.read_table_deletes(
        "invoices", start_offset, {}
    )

    assert list(first_records) == []
    assert list(second_records) == []
    assert first_offset == second_offset
    assert (
        get.call_args_list[0].kwargs["params"]["changedSince"]
        == get.call_args_list[1].kwargs["params"]["changedSince"]
        == "2026-07-26T11:59:00Z"
    )


def test_delete_checkpoint_older_than_cdc_horizon_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        quickbooks_module,
        "_utc_now",
        lambda: datetime(2026, 7, 26, 12, 30, tzinfo=timezone.utc),
    )
    get = Mock()
    monkeypatch.setattr(requests, "get", get)
    connector = QuickBooksLakeflowConnect(_options())

    with pytest.raises(RuntimeError, match="previous 30 days"):
        connector.read_table_deletes(
            "invoices",
            _checkpoint(
                "2026-06-20T12:30:00Z",
                table_name="invoices",
                flow="deletes",
            ),
            {},
        )
    get.assert_not_called()


def test_cdc_limit_fails_without_returning_a_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        requests,
        "get",
        Mock(
            return_value=_response(
                200,
                {
                    "CDCResponse": [
                        {
                            "QueryResponse": [
                                {"Invoice": [], "maxResults": 1000}
                            ]
                        }
                    ],
                    "time": "2026-07-26T12:30:01Z",
                },
            )
        ),
    )
    client = QuickBooksApiClient(
        access_token="token",
        realm_id="realm",
        environment="sandbox",
        minor_version=75,
    )

    with pytest.raises(RuntimeError, match="1,000-object"):
        client.get_entity_changes(
            "Invoice",
            changed_since="2026-07-26T12:00:00Z",
        )


def test_list_entities_reject_delete_reads() -> None:
    connector = QuickBooksLakeflowConnect(_options())
    with pytest.raises(ValueError, match="inactivated"):
        connector.read_table_deletes("customers", {}, {})


def test_delete_tombstone_requires_id_and_cursor() -> None:
    with pytest.raises(RuntimeError, match="missing Id"):
        _normalize_delete_entity(
            "invoices",
            {
                "status": "Deleted",
                "MetaData": {"LastUpdatedTime": "2026-07-26T12:00:00Z"},
            },
            realm_id="realm",
        )
    with pytest.raises(RuntimeError, match="LastUpdatedTime"):
        _normalize_delete_entity(
            "invoices",
            {"Id": "1", "status": "Deleted"},
            realm_id="realm",
        )


@pytest.mark.parametrize("status", [401, 403])
def test_auth_failures_are_not_retried(monkeypatch: pytest.MonkeyPatch, status: int) -> None:
    get = Mock(return_value=_response(status))
    monkeypatch.setattr(requests, "get", get)
    client = QuickBooksApiClient(
        access_token="token",
        realm_id="realm",
        environment="production",
        minor_version=75,
    )
    with pytest.raises(PermissionError, match="authentication failed"):
        list(client.iter_entity("Customer", page_size=10))
    assert get.call_count == 1


def test_retry_after_is_honored(monkeypatch: pytest.MonkeyPatch) -> None:
    get = Mock(
        side_effect=[
            _response(429, None, **{"Retry-After": "2"}),
            _response(200, {"QueryResponse": {"Customer": []}}),
        ]
    )
    sleep = Mock()
    monkeypatch.setattr(requests, "get", get)
    monkeypatch.setattr("time.sleep", sleep)
    client = QuickBooksApiClient(
        access_token="token",
        realm_id="realm",
        environment="sandbox",
        minor_version=75,
    )
    assert list(client.iter_entity("Customer", page_size=10)) == []
    sleep.assert_called_once_with(2.0)


def test_transient_failure_exhaustion(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(requests, "get", Mock(return_value=_response(503)))
    monkeypatch.setattr("time.sleep", Mock())
    client = QuickBooksApiClient(
        access_token="token",
        realm_id="realm",
        environment="sandbox",
        minor_version=75,
        max_retries=2,
    )
    with pytest.raises(RuntimeError, match="retry exhaustion"):
        list(client.iter_entity("Customer", page_size=10))


def test_network_failure_exhaustion(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(requests, "get", Mock(side_effect=requests.ConnectionError("offline")))
    monkeypatch.setattr("time.sleep", Mock())
    client = QuickBooksApiClient(
        access_token="token",
        realm_id="realm",
        environment="sandbox",
        minor_version=75,
        max_retries=2,
    )
    with pytest.raises(RuntimeError, match="retry exhaustion"):
        list(client.iter_entity("Customer", page_size=10))


def test_invalid_json(monkeypatch: pytest.MonkeyPatch) -> None:
    response = _response(200)
    response.json.side_effect = ValueError("bad json")
    monkeypatch.setattr(requests, "get", Mock(return_value=response))
    client = QuickBooksApiClient(
        access_token="token",
        realm_id="realm",
        environment="sandbox",
        minor_version=75,
    )
    with pytest.raises(RuntimeError, match="invalid JSON"):
        list(client.iter_entity("Customer", page_size=10))


def test_customer_decimal_timestamp_and_raw_payload() -> None:
    record = _normalize_entity(
        "customers",
        {
            "Id": "1",
            "Balance": "12.340",
            "Active": False,
            "DisplayName": "Example",
            "MetaData": {
                "CreateTime": "2026-07-20T10:00:00Z",
                "LastUpdatedTime": "2026-07-21T11:30:00+00:00",
            },
        },
        realm_id="realm",
    )
    assert record["realm_id"] == "realm"
    assert record["balance"] == Decimal("12.340")
    assert record["created_at"] == datetime.fromisoformat("2026-07-20T10:00:00+00:00")
    assert record["active"] is False
    assert '"Id":"1"' in record["raw_json"]


def test_transaction_dates_and_lines() -> None:
    record = _normalize_entity(
        "invoices",
        {
            "Id": "40",
            "TxnDate": "2026-07-01",
            "DueDate": "2026-07-31",
            "TotalAmt": 10.25,
            "Line": [{"Id": "1"}],
        },
        realm_id="realm",
    )
    assert record["txn_date"] == "2026-07-01"
    assert record["due_date"] == "2026-07-31"
    assert record["total_amount"] == Decimal("10.25")
    assert record["line_json"] == '[{"Id":"1"}]'


@pytest.mark.parametrize("value,expected", [(None, None), ("", None), ("3", 3.0), ("0", 0.0)])
def test_retry_after_seconds(value: str | None, expected: float | None) -> None:
    assert _retry_after_seconds(value) == expected
