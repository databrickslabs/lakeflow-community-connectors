"""Tests for the Fin.ai (Intercom) LakeflowConnect connector.

Runs against the in-process source simulator described by
``source_simulator/specs/fin_ai/``. Fin.ai is an Intercom REST API
connector, so the simulator stands in for ``api.intercom.io``:

  * ``POST /conversations/search`` / ``/contacts/search`` / ``/tickets/search``
    — the incremental Search-API tables (custom ``search`` handler; the
    ``updated_at`` filter, sort and ``starting_after`` pagination live in a
    JSON POST body). These are the three partitioned (``SupportsPartitionedStream``)
    tables, exercised by the partition suite.
  * ``GET /companies/scroll`` — the companies snapshot via the Scroll API
    (custom ``companies_scroll`` handler with an opaque ``scroll_param``).
  * ``GET /admins`` / ``/tags`` / ``/segments`` / ``/data_attributes`` /
    ``/teams`` — single-page GET snapshots (declarative).

Stand-in credentials below are values of the right shape; the simulator
does not validate them (auth headers — Bearer token, Intercom-Version —
are ignored).
"""

from __future__ import annotations

from databricks.labs.community_connector.sources.fin_ai.fin_ai import (
    FinAiLakeflowConnect,
)
from tests.unit.sources.test_partition_suite import (
    SupportsPartitionedStreamTests,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestFinAiConnector(LakeflowConnectTests, SupportsPartitionedStreamTests):
    connector_class = FinAiLakeflowConnect
    simulator_source = "fin_ai"

    # Stand-in credentials — the simulator never validates these; any
    # values of the right shape work. ``access_token`` is the only required
    # option (an Intercom private-app Access Token); region defaults to
    # ``us`` -> ``https://api.intercom.io``.
    replay_config = {
        "access_token": "simulator-fake-access-token",
        "region": "us",
    }

    # Columns that are legitimately null across this Intercom workspace's
    # recorded live sample (the corpus was re-seeded from the record-mode
    # cassette via ``tools.cassette_to_corpus``). Each is a real, correctly
    # read Intercom field that the sampled records simply don't populate —
    # verified against the raw records, not schema/flatten drift. Exempting
    # them keeps ``test_every_column_populated_*`` guarding the core columns
    # without fabricating corpus data. Reason noted per group.
    allow_null_columns = {
        # The ascending-``updated_at`` search returns the oldest threads first,
        # and those are imported historical conversations (their
        # ``custom_attributes`` carry "Imported via standalone") with no Fin
        # participation, CSAT, team assignment, linked ticket, or snooze.
        "conversations": {
            "ai_agent",             # Fin sub-object — null when Fin didn't participate (ai_agent_participated is False here)
            "conversation_rating",  # no CSAT on these threads
            "team_assignee_id",     # assigned to an admin, not a team (or unassigned)
            "ticket",               # no linked ticket
            "snoozed_until",        # none snoozed
        },
        # This workspace's contacts are email/lead records with no web/mobile
        # messenger sessions, so all device telemetry + optional profile fields
        # are empty in the sample.
        "contacts": {
            "email_domain", "phone", "owner_id", "language_override",
            "custom_attributes", "avatar",
            "last_email_opened_at", "last_email_clicked_at",
            "browser", "browser_version", "browser_language", "os",
            "android_app_name", "android_app_version", "android_device",
            "android_os_version", "android_sdk_version", "android_last_seen_at",
            "ios_app_name", "ios_app_version", "ios_device",
            "ios_os_version", "ios_sdk_version", "ios_last_seen_at",
        },
        # No company in the sample carries a plan / firmographics / usage
        # metrics; Intercom returns ``plan`` and ``custom_attributes`` as ``{}``
        # (normalized to null by the connector).
        "companies": {
            "plan", "custom_attributes", "last_request_at",
            "size", "website", "industry",
        },
        # These admins have no job title, are not away, and have no per-team
        # priority overrides (``team_priority_level`` comes back ``{}`` -> null).
        "admins": {"job_title", "away_status_reason_id", "team_priority_level"},
        # ``applied_at`` / ``applied_by`` appear only on tags embedded on a
        # taggable object, never on the standalone GET /tags list.
        "tags": {"applied_at", "applied_by"},
        # ``count`` is only returned when include_count=true (opt-in option).
        "segments": {"count"},
        # ``id`` / ``created_at`` / ``updated_at`` / ``admin_id`` exist only on
        # *custom* data attributes and ``options`` only on list-typed ones; the
        # sample is standard attributes.
        "data_attributes": {
            "id", "options", "created_at", "updated_at", "admin_id",
        },
    }
