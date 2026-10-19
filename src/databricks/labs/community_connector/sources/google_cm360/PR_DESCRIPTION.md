## Summary

- Add Google Campaign Manager 360 Reporting API v5 as a Lakeflow community source.
- Expose campaign performance, daily reach, global placement performance and native campaign-period reach snapshots.
- Use reporting-only OAuth, fixed schemas/keys, bounded daily reads and replay, and native reach values without summation.
- Include synthetic simulator fixtures, source-specific tests, connection specification, generated deployment source and user/API documentation.
- Reject footerless daily exports before returning a batch or checkpoint.

## Validation

- Offline Python 3.10 source suite and upstream Pylint flags.
- Generated-source consistency and framework/source wheel builds.
- Source/package credential and customer-identifier scans.
- Fresh authenticated source validation and E2E Databricks ingestion are still outstanding. Please apply `needs-live-testing` while that evidence is missing.

## Support Boundaries

- English CSV headers; ISO Date for daily reach and DD/MM/YYYY period metadata with exact timezone validation.
- Explicit single-country filters support US/DE/CH. Unfiltered daily campaign reads may include other countries. Placement remains global.
- Daily upserts have no delete feed. Period reach is a complete snapshot; spans over 92 days and unknown campaign starts carry explicit coverage status and NULL reach.
- Costs are unchanged and not currency-normalized. Reach is non-additive.

## Review

Please review the connector contract, source permissions/field compatibility, documentation, packaging, simulator behavior and live-validation evidence. Maintainer authorization is required for fork CI. Re-run the self-review workflow after the final push; this description is not a self-review label or approval.

AI-assisted implementation and review using OpenAI Codex.
