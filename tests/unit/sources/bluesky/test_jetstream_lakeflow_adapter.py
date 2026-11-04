"""Characterize the published client's compatibility with Lakeflow rows."""

import json

import pytest
from jetstream_lakehouse import Client, Config

from databricks.labs.community_connector.sources.bluesky.bluesky import BlueskyLakeflowConnect
from databricks.labs.community_connector.sources.bluesky.errors import ProtocolError
from databricks.labs.community_connector.sources.bluesky.jetstream_lakeflow_adapter import (
    lakeflow_row,
    translated_errors,
)
from databricks.labs.community_connector.sources.bluesky.options import Options


def snapshot_rows(options: Options, after: int, max_batch_bytes: int) -> tuple[list[dict], int]:
    """Compare the public library reader with the connector's pinned reader."""
    client = Client(
        Config(
            endpoint=options.endpoint,
            api_key=options.api_key,
            collections=options.collections,
            kinds=options.kinds,
            dids=options.dids,
            max_sequence_span=options.max_events_per_refresh,
            max_batch_bytes=max_batch_bytes,
            request_timeout_seconds=options.request_timeout_seconds,
            refresh_timeout_seconds=options.refresh_timeout_seconds,
            max_retries=options.max_retries,
            include_raw_payload=options.include_raw_payload,
        )
    )
    with translated_errors():
        batch = client.snapshot(after)
    rows = [lakeflow_row(event, options.include_raw_payload) for event in batch.events]
    return rows, batch.through_seq


@pytest.mark.parametrize(
    "settings",
    [
        {},
        {"collections": "app.bsky.feed.post"},
        {"kinds": "identity,account"},
        {"dids": "did:plc:unknown"},
        {"include_raw_payload": "false"},
        {"max_events_per_refresh": "2"},
    ],
)
def test_published_snapshot_matches_connector_rows(service, settings):
    options = Options.parse(settings, {})
    existing = BlueskyLakeflowConnect(options)
    old_rows, old_offset = existing.read_table("events", None, {})
    new_rows, new_cursor = snapshot_rows(options, options.starting_cursor, 64 * 1024 * 1024)

    old_rows = list(old_rows)
    assert new_cursor == old_offset["seq"]
    assert len(new_rows) == len(old_rows)
    for old, new in zip(old_rows, new_rows):
        assert old.keys() == new.keys()
        for key in old:
            if key in ("record", "event_payload"):
                assert (json.loads(old[key]) if old[key] is not None else None) == (
                    json.loads(new[key]) if new[key] is not None else None
                )
            else:
                assert old[key] == new[key]


def test_published_snapshot_preserves_delete_row_schema(service):
    rows, _ = snapshot_rows(Options.parse({}, {}), 0, 64 * 1024 * 1024)
    deleted = next(row for row in rows if row["operation"] == "delete")
    assert deleted["record"] is None
    assert deleted["event_payload"] == "null"
    assert deleted["raw_payload"] == ""


def test_published_snapshot_maps_protocol_errors(service):
    service.data = service.data[:-1] + bytes([service.data[-1] ^ 1])
    with pytest.raises(ProtocolError, match="checksum"):
        snapshot_rows(Options.parse({}, {}), 0, 64 * 1024 * 1024)
