from unittest.mock import patch

from databricks.labs.community_connector.sources.actitime.actitime import (
    ActitimeLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestActitimeConnector(LakeflowConnectTests):
    connector_class = ActitimeLakeflowConnect
    simulator_source = "actitime"
    replay_config = {
        # A real actiTIME Online deployment uses
        # ``https://online.actitime.com/<tenant>`` — the tenant slug is part
        # of the URL PATH, not the subdomain. We mirror that here with a
        # ``/sim`` segment so the simulator's path patterns
        # (``/{tenant}/api/v1/<table>``) match in both simulate and record
        # mode without needing a separate spec per environment.
        "base_url": "https://simulator.actitime.example/sim",
        "username": "simulator",
        "password": "simulator-fake-password",
    }
    # Empty-first-read allowances:
    #
    # * ``settings``, ``userGroups``, ``holidays``, ``approvalStatus`` —
    #   these are feature-gated on the actiTIME tenant. On the test
    #   tenant they return HTTP 404 ("api.error.unknown_error"). The
    #   connector swallows 404 on these endpoints (treat as absent
    #   table) and emits zero records; we allow the empty first read so
    #   that fact alone doesn't fail the suite. Note: ``settings`` is a
    #   ``snapshot`` and is therefore already exempt from the empty-read
    #   check, but listing it keeps the intent obvious.
    allow_empty_first_read = frozenset({
        "settings",
        "userGroups",
        "holidays",
        "approvalStatus",
    })

    # ------------------------------------------------------------------
    # Issue #178 — per-user fan-out on /timetrack and /leavetime
    # ------------------------------------------------------------------

    def test_time_window_fans_out_by_user_batch(self):
        """``user_batch_size`` splits the request across multiple ``userIds=``
        GETs and the connector unions the results without duplicates.

        With ``user_batch_size=2`` the connector issues one GET per pair of
        users (each filtered by the ``userIds=`` query param the
        endpoint's spec declares as an ``op: in`` filter — see
        ``source_simulator/specs/actitime/endpoints.yaml``). The union of
        responses should:

        - cover at least two distinct ``user_id`` values (proving fan-out
          actually happened and didn't collapse to a single call), and
        - contain no duplicate rows on the composite primary key
          ``(user_id, date, task_id)`` (proving the simulator/live API
          honoured the per-batch filter and the connector didn't
          double-emit).

        Tenant-agnostic: works against the 5-user simulator corpus and
        against a live tenant of arbitrary size.
        """
        records, _ = self.connector.read_table(
            "timetrack",
            {},
            {"user_batch_size": "2", "window_days": "60", "lookback_days": "60"},
        )
        rows = list(records)
        user_ids_seen = {r["user_id"] for r in rows}
        if len(rows) == 0:
            # Live tenant might have zero timetrack entries in the
            # microbatch window; the fan-out path still ran but produced
            # no records to assert on. Don't fail in that case.
            return
        assert len(user_ids_seen) >= 2, (
            f"expected fan-out across ≥ 2 users; saw only {user_ids_seen} "
            f"(probable single-call regression)"
        )
        composite_keys = [(r["user_id"], r["date"], r["task_id"]) for r in rows]
        assert len(composite_keys) == len(set(composite_keys)), (
            "duplicate (user_id, date, task_id) rows — per-batch filter "
            "did not partition cleanly"
        )

    def test_time_window_empty_user_list_advances_cursor(self):
        """Tenant with zero users short-circuits but still advances the
        cursor so the framework makes progress instead of looping."""
        with patch.object(
            self.connector, "_read_offset_limit", return_value=iter([])
        ):
            records, next_offset = self.connector.read_table("timetrack", {}, {})
        assert list(records) == []
        assert "cursor" in next_offset, (
            "expected cursor to advance even when there are no users"
        )

    # ------------------------------------------------------------------
    # PR #176 review (Young, comment 3228778226) — max_records_per_batch
    # must not leak into internal /users discovery walks.
    # ------------------------------------------------------------------

    def test_user_rates_internal_walk_not_capped_by_max_records(self):
        """Setting a tiny ``max_records_per_batch`` on userRates must not
        truncate the internal ``/users`` discovery walk.

        The simulator corpus has 5 users with 3 rate rows each (15 total).
        Pre-fix: ``max_records_per_batch=2`` would cap the inner
        ``_read_offset_limit("users", ...)`` walk at 2 users, then fan out
        to only 2 ``/userRates/{id}`` calls — emitting 6 rate rows.
        Post-fix: the ``_raw=True`` flag gates the cap so internal lookups
        drain regardless, and the call site passes ``{}`` so userRates
        options never reach the users walk in the first place.
        """
        records, _ = self.connector.read_table(
            "userRates", {}, {"max_records_per_batch": "2"}
        )
        rows = list(records)
        if not rows:
            # Live tenant may have no userRates rows configured for the
            # service account; skip rather than flake.
            return
        distinct_users = {r["user_id"] for r in rows}
        assert len(distinct_users) >= 3, (
            f"max_records_per_batch=2 truncated the internal /users walk: "
            f"only {len(distinct_users)} distinct users observed in rate "
            f"output (expected at least 3). User ids seen: {distinct_users}"
        )

    def test_timetrack_internal_walk_not_capped_by_max_records(self):
        """Same defence for the /timetrack fan-out path.

        ``max_records_per_batch=2`` on timetrack must not cap the internal
        ``/users`` enumeration that feeds the per-batch ``userIds=`` filter
        — otherwise users beyond index 1 would never be fanned out.
        """
        records, _ = self.connector.read_table(
            "timetrack",
            {},
            {
                "max_records_per_batch": "2",
                "user_batch_size": "10",  # force one fan-out batch
                "window_days": "60",
                "lookback_days": "60",
            },
        )
        rows = list(records)
        if not rows:
            return
        distinct_users = {r["user_id"] for r in rows}
        assert len(distinct_users) >= 3, (
            f"max_records_per_batch=2 leaked into the inner /users walk: "
            f"only {len(distinct_users)} distinct users observed in "
            f"timetrack fan-out (expected at least 3). User ids seen: "
            f"{distinct_users}"
        )
