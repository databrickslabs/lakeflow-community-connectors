---
name: validate-incremental-sync
description: Validate that a connector's CDC/incremental sync implementation correctly tracks offsets and filters records.
---

# Validate Incremental Sync

## Description

This skill helps you validate that a connector's CDC (Change Data Capture) implementation is correct by:
1. Understanding how the offset structure works from the code
2. Verifying the offset matches the cursor field of the last record
3. Testing that passing an offset filters records correctly

This is useful for:
- **Development**: Verifying incremental sync during connector implementation
- **Quality Review**: Validating existing connectors' CDC behavior
- **Debugging**: Investigating issues with offset tracking or filtering

## Instructions

Read and follow **[prompts/validate_incremental_sync.md](../../../prompts/validate_incremental_sync.md)**

### Quick Summary

1. **Run the test suite first** to get table metadata and ingestion types
2. **Identify CDC tables** (skip snapshot/append tables)
3. **Read the connector code** to understand offset structure
4. **Test filtering** by reading with a midpoint cursor value
5. **Document findings** using the provided template

