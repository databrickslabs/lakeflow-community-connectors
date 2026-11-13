## Summary

- Add Google Campaign Manager 360 Reporting API v5 as a Lakeflow community source.
- Expose campaign performance, daily reach, global placement performance and native campaign-period reach snapshots.
- Add opt-in `reach_report_period_incremental`, which checkpoints native period reach so one cutoff can load across several pipeline updates, with optional `refresh_ended_within_days` to skip reports for long-ended campaigns.
- Share one native REACH export across campaigns with identical periods, bound the period snapshot with a 45-minute cooperative deadline, and classify HTTP 401 without retry.
- Use reporting-only OAuth, fixed schemas/keys, bounded daily reads and replay, and native reach values without summation.
- Include synthetic simulator fixtures, source-specific tests, connection specification, generated deployment source and user/API documentation.
- Reject footerless daily exports before returning a batch or checkpoint.

## Validation

- Current offline source suite on Python 3.12.13: 331 passed, one unsupported-capability skip, including period batching, deadline, diagnostics and resumable-reach checkpoint tests.
- Generated-source consistency, framework/source wheel builds, and source/package credential and customer-identifier scans.
- Standalone connector wheel and source distribution include the upstream LICENSE and NOTICE; their contents are verified against the repository originals.
- Existing managed Databricks runs cover campaign performance, daily reach, placement performance and period reach using earlier connector builds. These are supporting live evidence; they do not establish an exact-artifact managed-runtime test of this PR. `reach_report_period_incremental` ran in managed development pipeline updates, including resume across updates with values matching native reach for the campaigns checked; a complete native-value comparison at full scale has not been established.
- A current record-mode validation report and an exact-public-artifact managed-runtime run have not been established. Retain `needs-live-testing` until maintainers accept the mapped existing evidence or specify a remaining validation requirement. Maintainer-authorized CI remains outstanding.

## Support Boundaries

- English CSV headers; ISO Date for daily reach and DD/MM/YYYY period metadata with exact timezone validation.
- Explicit single-country filters support US/DE/CH. Unfiltered daily campaign reads may include other countries. Placement remains global.
- Daily upserts and `reach_report_period_incremental` have no delete feed. `reach_report_period` is a complete snapshot; spans over 92 days and unknown campaign starts carry explicit coverage status and NULL reach.
- Costs are unchanged and not currency-normalized. Reach is non-additive.

## Review

Please review the connector contract, source permissions/field compatibility, documentation, packaging, simulator behavior and live-validation evidence. Maintainer authorization is required for fork CI. Re-run the self-review workflow after the final push; this description is not a self-review label or approval.

AI-assisted implementation and review using OpenAI Codex and Claude Code.
