## Summary

- Add Google Campaign Manager 360 Reporting API v5 as a Lakeflow community source.
- Expose campaign performance, daily reach, global placement performance and native campaign-period reach snapshots.
- Use reporting-only OAuth, fixed schemas/keys, bounded daily reads and replay, and native reach values without summation.
- Include synthetic simulator fixtures, source-specific tests, connection specification, generated deployment source and user/API documentation.
- Reject footerless daily exports before returning a batch or checkpoint.

## Validation

- Current offline source suite on Python 3.12.13: 256 passed, two unsupported-capability skips. Existing Python 3.10 source and Spark-reader-wrapper review evidence is retained for the unchanged implementation.
- Generated-source consistency, framework/source wheel builds, and source/package credential and customer-identifier scans.
- Standalone connector wheel and source distribution include the upstream LICENSE and NOTICE; their contents are verified against the repository originals.
- Existing managed Databricks runs cover campaign performance, daily reach, placement performance and period reach using earlier connector builds. These are supporting live evidence; they do not establish an exact-artifact managed-runtime test of this PR.
- A current record-mode validation report and an exact-public-artifact managed-runtime run have not been established. Retain `needs-live-testing` until maintainers accept the mapped existing evidence or specify a remaining validation requirement. Maintainer-authorized CI remains outstanding.

## Support Boundaries

- English CSV headers; ISO Date for daily reach and DD/MM/YYYY period metadata with exact timezone validation.
- Explicit single-country filters support US/DE/CH. Unfiltered daily campaign reads may include other countries. Placement remains global.
- Daily upserts have no delete feed. Period reach is a complete snapshot; spans over 92 days and unknown campaign starts carry explicit coverage status and NULL reach.
- Costs are unchanged and not currency-normalized. Reach is non-additive.

## Review

Please review the connector contract, source permissions/field compatibility, documentation, packaging, simulator behavior and live-validation evidence. Maintainer authorization is required for fork CI. Re-run the self-review workflow after the final push; this description is not a self-review label or approval.

AI-assisted implementation and review using OpenAI Codex.
