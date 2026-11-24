# Recovery after an archived generation disappears

This runbook covers a pending Bluesky batch that now fails with
`ArchiveChanged`. The connector pins archive generations in its checkpoint and
will not replan a retry. Recovery uses a new target and a new checkpoint while
preserving the existing table and checkpoint.

## Record the failed update

Stop the failing pipeline update. Record these values in the incident ticket:

| Value | Example placeholder |
| --- | --- |
| Workspace and runtime | `<test-workspace>`, `<runtime-version>` |
| Connector and library | `<connector-version>`, `jetstream-lakehouse==0.2.0` |
| Old table and checkpoint | `<catalog.schema.old_events>`, `<checkpoint-identity>` |
| Archive endpoint origin | `<https://archive.example>` |
| Last committed sequence | `<committed-seq>` |
| Pending interval | `(<pending-after>, <pending-through>]` |
| Filters and raw-payload setting | `<same values as old pipeline>` |

Do not record an API key, bearer header, request body, or raw event payload.
Keep the old target table and checkpoint intact. Do not run a full refresh or
edit checkpoint files.

## Choose and validate a restart cursor

Choose `<restart-seq>` only after confirming that the archive can still serve
the required history from that sequence. A successful planner response alone
does not establish that every earlier record is still retained. Keep the same
endpoint, filters, and payload setting for the new ingestion.

Create a new pipeline and target table `<catalog.schema.new_events>` with a
fresh checkpoint. Set `starting_cursor` to `<restart-seq>` and cap
`max_events_per_refresh` to bound each microbatch's sequence span. This does not
cap the entire update: AvailableNow drains to its fixed sealed target. For a
small smoke test use a recent, verified available cursor; for historical recovery
retain the required restart cursor and budget for the full catch-up. Configure
a job timeout and monitor the update so an unexpectedly large catch-up can be
stopped without deleting its checkpoint.
Use the normal pipeline deployment flow; do not write into a managed streaming
table directly.

Run one controlled update. Confirm that the new table advances beyond the
restart cursor, inspect its minimum and maximum `seq`, and compare the
`(after, through]` ranges with the recorded committed and pending ranges. Keep
the old rows. Record any overlap and every interval that the archive can no
longer provide. Do not describe sequence holes as an exact event count or as
proof of lost records: filtering and archive compaction can also create holes.

## Build a deduplicated view

After validating the new target, expose a view that prefers rows in the new
target when the two tables contain the same `seq`, while retaining old rows
that are absent from the new target:

```sql
CREATE OR REPLACE VIEW `<catalog>`.`<schema>`.`<recovered_view>` AS
WITH candidates AS (
  SELECT 0 AS recovery_precedence, * FROM `<catalog>`.`<schema>`.`<new_events>`
  UNION ALL
  SELECT 1 AS recovery_precedence, * FROM `<catalog>`.`<schema>`.`<old_events>`
)
SELECT * EXCEPT (recovery_precedence)
FROM candidates
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY seq
  ORDER BY recovery_precedence ASC
) = 1;
```

Confirm that the view contains no duplicate `seq` values and that the new
pipeline has not removed or replaced the old table. The view preserves
available old rows, but it cannot recover event content that Jetstream has
already compacted away. If the old and new tables use different filters or
payload settings, do not combine them with this template; reconcile the
intended schema and semantics first.

## Evidence required before calling recovery validated

Record the workspace and runtime, package versions, sanitized endpoint origin,
restart cursor, committed and pending ranges, bounded trial range, old/new row
counts, overlaps, sequence gaps, and a check that the old rows remain present.
Also record which history could not be recovered. This repository has not yet
run this procedure in an authorized Databricks test workspace; the runbook is
prepared for maintainer review and environment validation.
