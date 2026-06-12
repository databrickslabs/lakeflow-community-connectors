"""Tests for the HL7 v2 community connector (GCP Healthcare API).

Runs the standard LakeflowConnectTests suite against the simulator by
default (offline, deterministic, no creds required). To exercise a real
GCP HL7v2 store, set ``CONNECTOR_TEST_MODE=live`` and provide credentials
via ``CONNECTOR_TEST_CONFIG_JSON`` or ``CONNECTOR_TEST_CONFIG_PATH``.
"""

import base64
import json
from unittest.mock import patch

from databricks.labs.community_connector.sources.hl7_v2.hl7_v2 import HL7V2LakeflowConnect
from tests.unit.sources.hl7_v2._hl7v2_null_cols import allow_null_columns as _HL7V2_NULL_COLS
from tests.unit.sources.test_suite import LakeflowConnectTests


def _gcp_message(message_id: str, create_time: str, send_time: str) -> dict:
    """Build a GCP-API-shaped message dict carrying one PID segment.

    ``data`` is base64-encoded raw HL7 text (matching what the real GCP
    Healthcare API returns and what ``_parse_api_messages`` base64-decodes
    in GCP mode).  Each message yields exactly one PID row, so row counts
    map 1:1 to messages — convenient for exercising the capped-batch cut.
    """
    raw = (
        f"MSH|^~\\&|APP|FAC|RCV|RCVFAC|20240115080000||ADT^A01|{message_id}|P|2.5\r"
        f"PID|1||{message_id}_MRN^^^MRN||DOE^JOHN"
    )
    return {
        "data": base64.b64encode(raw.encode("utf-8")).decode("ascii"),
        "createTime": create_time,
        "sendTime": send_time,
        "name": f"projects/p/locations/l/.../messages/{message_id}",
    }


def _generate_pem_key() -> str:
    """Build a real RSA key in PKCS#8 PEM form.

    The connector calls ``google.oauth2.service_account.Credentials.
    from_service_account_info`` in its ``__init__``, which deserializes
    the PEM eagerly. The simulator intercepts every outbound HTTP call,
    so the key is never used for actual signing — but it must parse.
    """
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return pem.decode("ascii")


class TestHL7V2Connector(LakeflowConnectTests):
    connector_class = HL7V2LakeflowConnect
    simulator_source = "hl7_v2"
    sample_records = 5
    allow_null_columns = _HL7V2_NULL_COLS

    @classmethod
    def _replay_config(cls):
        return {
            "source_type": "gcp",
            "project_id": "sim-project",
            "location": "us-central1",
            "dataset_id": "sim-dataset",
            "hl7v2_store_id": "sim-store",
            "service_account_json": json.dumps({
                "type": "service_account",
                "project_id": "sim-project",
                "private_key_id": "sim-key-id",
                "private_key": _generate_pem_key(),
                "client_email": "sim@sim-project.iam.gserviceaccount.com",
                "client_id": "0",
                "token_uri": "https://oauth2.googleapis.com/token",
            }),
        }

    # ------------------------------------------------------------------
    # Z-segment / custom-segment coverage
    #
    # The base ``LakeflowConnectTests`` iterates ``connector.list_tables()``
    # which only exposes the typed segment names (MSH, PID, OBX, ...).
    # Custom Z-segments are opted into per-table via the ``segment_type``
    # table option (see the connector README), so they are not reached by
    # any of the auto-generated read / schema / metadata checks. These
    # tests fill that gap by driving the full connector path (schema →
    # metadata → read) for a Z-segment table backed by the simulator
    # corpus message ``sample_adt_zsegments`` (which carries two ZPI
    # segments and one ZIN segment).
    # ------------------------------------------------------------------

    _Z_TABLE = "custom_zpi"
    _Z_OPTS = {"segment_type": "ZPI", "window_seconds": "31536000"}

    def test_z_segment_schema_falls_back_to_generic(self):
        """A Z-segment table resolves to the 25-field generic schema."""
        schema = self.connector.get_table_schema(self._Z_TABLE, self._Z_OPTS)
        names = schema.fieldNames()
        assert "segment_type" in names, (
            "GENERIC_SEGMENT_SCHEMA must declare the segment_type column so "
            "downstream consumers can distinguish multiplexed Z-segments."
        )
        for i in range(1, 26):
            assert f"field_{i}" in names, f"missing field_{i} in generic schema"
        for meta_col in ("message_id", "message_timestamp", "create_time", "raw_segment"):
            assert meta_col in names, f"missing metadata column {meta_col!r}"

    def test_z_segment_metadata_uses_message_id_only(self):
        """Unknown / Z-segments fall back to ``primary_keys=['message_id']``.

        The composite ``(message_id, set_id)`` PK is only used for typed
        multi-segment tables — the generic schema doesn't declare set_id,
        so the connector intentionally narrows the PK here.
        """
        meta = self.connector.read_table_metadata(self._Z_TABLE, self._Z_OPTS)
        assert meta["ingestion_type"] == "append"
        assert meta["cursor_field"] == "create_time"
        assert meta["primary_keys"] == ["message_id"]

    def test_z_segment_read_extracts_rows_from_corpus(self):
        """End-to-end: read_table for a Z-segment yields generic-schema rows.

        Exercises the full pipeline: simulator → list/get message →
        base64-decode → parse HL7 → fallback to ``_extract_generic`` for
        an unknown segment → propagate metadata / raw_segment / segment_type.
        """
        iterator, offset = self.connector.read_table(
            self._Z_TABLE, {}, self._Z_OPTS
        )
        rows = list(iterator)
        assert rows, (
            "Expected at least one ZPI row from the simulator corpus "
            "(sample_adt_zsegments carries two ZPI segments)."
        )
        assert isinstance(offset, dict) and offset.get("cursor"), (
            "read_table must advance the cursor for a Z-segment table just "
            "like any other incremental table."
        )

        for row in rows:
            assert row["segment_type"] == "ZPI", (
                "Generic extractor must stamp the segment_type column with "
                "the upper-cased segment identifier."
            )
            assert row["message_id"], "metadata.message_id must be populated"
            assert row["raw_segment"].startswith("ZPI|"), (
                "raw_segment must carry the original pipe-delimited line for "
                "lossless recovery."
            )
            # field_1 is the set ID baked into the segment payload; field_2..
            # carry the custom Z-segment columns. Verify both shapes work.
            assert row["field_1"] in {"1", "2"}, (
                f"Unexpected ZPI.field_1 value: {row['field_1']!r}"
            )

        # Multi-segment fan-out: the corpus message has two ZPI segments,
        # so a single source message must produce two rows.
        by_msg: dict[str, list] = {}
        for r in rows:
            by_msg.setdefault(r["message_id"], []).append(r)
        assert any(len(rs) >= 2 for rs in by_msg.values()), (
            "Expected at least one message to fan out into multiple ZPI rows "
            f"but got: {[(k, len(v)) for k, v in by_msg.items()]}"
        )

    def test_z_segment_distinct_custom_segment(self):
        """A second Z-segment type (ZIN) on the same corpus reads independently.

        Confirms the ``segment_type`` table-option is the only thing that
        controls which segments are emitted — switching it from ZPI to ZIN
        on the same connector instance yields a different (non-overlapping)
        row set.
        """
        zin_opts = {"segment_type": "ZIN", "window_seconds": "31536000"}
        iterator, _ = self.connector.read_table("custom_zin", {}, zin_opts)
        zin_rows = list(iterator)
        assert zin_rows, "Expected at least one ZIN row from the corpus"
        for row in zin_rows:
            assert row["segment_type"] == "ZIN"
            assert row["raw_segment"].startswith("ZIN|")
            assert row["field_2"] == "PRIMARY"
            assert row["field_3"] == "MEMBER_Z001"

    def test_z_segment_rows_omit_set_id_and_match_schema(self):
        """Z-segment rows must not carry a ``set_id`` the schema can't hold.

        Regression for the row/schema/PK mismatch: unknown segments fall back
        to ``GENERIC_SEGMENT_SCHEMA`` (no ``set_id``) and use
        ``primary_keys=['message_id']``, so the row builder must NOT inject
        ``set_id`` here even though Z-segments are multi-segment.  Every key
        the connector emits must be declared by the generic schema, otherwise
        the framework drops the column or errors during row→Arrow coercion.
        """
        schema_names = set(
            self.connector.get_table_schema(self._Z_TABLE, self._Z_OPTS).fieldNames()
        )
        iterator, _ = self.connector.read_table(self._Z_TABLE, {}, self._Z_OPTS)
        rows = list(iterator)
        assert rows, "Expected ZPI rows from the simulator corpus"
        # The corpus message has two ZPI segments, so this DOES go through the
        # multi-segment fan-out branch — the exact place set_id used to leak.
        assert any(
            sum(1 for r in rows if r["message_id"] == mid) >= 2
            for mid in {r["message_id"] for r in rows}
        ), "Expected the multi-ZPI fan-out so the set_id-injection branch runs"
        for row in rows:
            assert "set_id" not in row, (
                "Z-segment rows must not include set_id (absent from "
                "GENERIC_SEGMENT_SCHEMA and from primary_keys)."
            )
            extra = set(row.keys()) - schema_names
            assert not extra, (
                f"Z-segment row has keys not declared by the generic schema: {extra}"
            )

    # ------------------------------------------------------------------
    # Capped-batch (max_records_per_batch) cursor correctness
    #
    # GCP fetches are ordered by sendTime, but the cursor field is
    # create_time.  read_table sorts by create_time before applying the
    # per-batch cap so the rewind ("resume from last create_time emitted")
    # cannot drop or duplicate records.  These tests feed deliberately
    # out-of-order create_time data through a fake fetch that honors the
    # (since, until] filter, then walk the cursor across batches.
    # ------------------------------------------------------------------

    def _drive_batches(self, messages, opts):
        """Run read_table to exhaustion, returning rows emitted per batch.

        The fake fetch mirrors the real ``createTime > since AND <= until``
        semantics so the cross-batch cursor behaviour is exercised end to end.
        """
        def fake_fetch(since, until):
            return [
                m for m in messages if since < m["createTime"] <= until
            ]

        batches = []
        offset = {"cursor": "2024-01-15T08:00:00Z"}
        with patch.object(self.connector, "_fetch_messages_in_window", fake_fetch):
            for _ in range(20):  # generous guard against an infinite loop
                iterator, new_offset = self.connector.read_table("pid", offset, opts)
                rows = list(iterator)
                if not rows and new_offset == offset:
                    break
                if rows:
                    batches.append(rows)
                offset = new_offset
                if offset.get("cursor", "") >= self.connector._init_ts:
                    break
        return batches

    def test_capped_batch_sorts_by_create_time(self):
        """A single capped batch emits rows in create_time order, not fetch order."""
        # Fetch order (sendTime) deliberately differs from create_time order.
        messages = [
            _gcp_message("M_A", "2024-01-15T08:00:03Z", "2024-01-15T09:00:00Z"),
            _gcp_message("M_B", "2024-01-15T08:00:01Z", "2024-01-15T09:00:01Z"),
            _gcp_message("M_C", "2024-01-15T08:00:02Z", "2024-01-15T09:00:02Z"),
        ]
        opts = {"window_seconds": "31536000", "max_records_per_batch": "2"}

        def fake_fetch(since, until):
            return [m for m in messages if since < m["createTime"] <= until]

        with patch.object(self.connector, "_fetch_messages_in_window", fake_fetch):
            iterator, offset = self.connector.read_table(
                "pid", {"cursor": "2024-01-15T08:00:00Z"}, opts
            )
            rows = list(iterator)

        cts = [r["create_time"] for r in rows]
        assert cts == sorted(cts), f"Batch not sorted by create_time: {cts}"
        # Cap=2 → the two lowest create_times (B, C) come out first.
        assert cts == ["2024-01-15T08:00:01Z", "2024-01-15T08:00:02Z"]
        # Cursor rewinds to the highest create_time actually emitted.
        assert offset == {"cursor": "2024-01-15T08:00:02Z"}

    def test_capped_batch_no_drops_or_dupes_across_batches(self):
        """Walking the cursor across capped batches yields each record exactly once."""
        messages = [
            _gcp_message("M_A", "2024-01-15T08:00:03Z", "2024-01-15T09:00:00Z"),
            _gcp_message("M_B", "2024-01-15T08:00:01Z", "2024-01-15T09:00:01Z"),
            _gcp_message("M_C", "2024-01-15T08:00:02Z", "2024-01-15T09:00:02Z"),
        ]
        opts = {"window_seconds": "31536000", "max_records_per_batch": "2"}

        batches = self._drive_batches(messages, opts)
        emitted = [r["message_id"] for b in batches for r in b]

        assert sorted(emitted) == ["M_A", "M_B", "M_C"], (
            f"Expected each record exactly once, got: {emitted}"
        )
        assert len(emitted) == len(set(emitted)), f"Duplicate rows emitted: {emitted}"

    def test_capped_batch_keeps_equal_create_time_group_together(self):
        """Records sharing a create_time are never split across a batch boundary.

        This is the core data-loss case: under sendTime fetch order the equal
        create_time rows can be non-contiguous, so without the create_time
        sort the cut could split the group and the next ``createTime > cursor``
        fetch would silently skip the un-emitted member.
        """
        # M_B1 and M_B2 share a create_time but are interleaved (by sendTime)
        # with M_C, exactly the non-contiguous arrangement that breaks an
        # unsorted cut.
        messages = [
            _gcp_message("M_B1", "2024-01-15T08:00:01Z", "2024-01-15T09:00:00Z"),
            _gcp_message("M_C",  "2024-01-15T08:00:02Z", "2024-01-15T09:00:01Z"),
            _gcp_message("M_B2", "2024-01-15T08:00:01Z", "2024-01-15T09:00:02Z"),
        ]
        opts = {"window_seconds": "31536000", "max_records_per_batch": "1"}

        batches = self._drive_batches(messages, opts)
        emitted = [r["message_id"] for b in batches for r in b]

        assert sorted(emitted) == ["M_B1", "M_B2", "M_C"], (
            f"A same-create_time record was dropped/duplicated: {emitted}"
        )
        # The two equal-create_time rows must land in the SAME batch (the cap
        # is coalesced up to the create_time boundary).
        first_batch_ids = {r["message_id"] for r in batches[0]}
        assert {"M_B1", "M_B2"} <= first_batch_ids, (
            "Equal-create_time group was split across batches: "
            f"first batch = {first_batch_ids}"
        )

    # ------------------------------------------------------------------
    # start_timestamp normalization (cursor lexicographic comparison)
    # ------------------------------------------------------------------

    def test_normalize_timestamp_canonicalizes_user_input(self):
        """Non-canonical start_timestamp shapes normalize to %Y-%m-%dT%H:%M:%SZ."""
        norm = HL7V2LakeflowConnect._normalize_timestamp
        # Bare date.
        assert norm("2024-01-01") == "2024-01-01T00:00:00Z"
        # Already canonical (idempotent).
        assert norm("2024-01-15T08:00:00Z") == "2024-01-15T08:00:00Z"
        # Numeric offset is converted to UTC.
        assert norm("2024-01-01T05:30:00+05:30") == "2024-01-01T00:00:00Z"
        # Fractional seconds are truncated.
        assert norm("2024-01-15T08:00:00.123456Z") == "2024-01-15T08:00:00Z"

    def test_start_timestamp_option_is_normalized_before_use(self):
        """A bare-date start_timestamp resolves correctly through read_table.

        Without normalization the bare date would be compared lexicographically
        against the canonical init timestamp and embedded raw in the GCP filter
        string, both of which assume the full ...T..:..:..Z shape.
        """
        messages = [
            _gcp_message("M_A", "2024-06-01T00:00:00Z", "2024-06-01T00:00:00Z"),
        ]

        def fake_fetch(since, until):
            # Assert the connector handed us a canonical, full-shape cursor.
            assert since == "2024-01-01T00:00:00Z", (
                f"start_timestamp was not normalized before fetch: since={since!r}"
            )
            return [m for m in messages if since < m["createTime"] <= until]

        opts = {"window_seconds": "31536000", "start_timestamp": "2024-01-01"}
        with patch.object(self.connector, "_fetch_messages_in_window", fake_fetch):
            iterator, _ = self.connector.read_table("pid", {}, opts)
            rows = list(iterator)

        assert [r["message_id"] for r in rows] == ["M_A"]
