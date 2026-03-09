# Google Sheets & Docs — Pipeline Specs

Use these JSON pipeline specs with the Lakeflow ingestion pipeline (or CLI) for the **google_sheets_docs** connector.

## Files

### All-in-one and per-table (default configs)

| File | Use case |
|------|----------|
| **pipeline_spec_all_tables.json** | One pipeline that ingests all three tables (spreadsheets, sheet_values, documents). |
| **pipeline_spec_spreadsheets.json** | Pipeline for **spreadsheets** only (Drive file list; no table_configuration). |
| **pipeline_spec_sheet_values.json** | **sheet_values** with default options (first row as header, Sheet1, A:Z). Replace `YOUR_SPREADSHEET_ID`. |
| **pipeline_spec_documents.json** | **documents** only (metadata only; no document body). |
| **pipeline_spec_documents_with_content.json** | **documents** with plain-text body via `include_content: "true"`. |

### Special configurations (from connector docs)

| File | Special config | Description |
|------|----------------|-------------|
| **pipeline_spec_sheet_values_raw_rows.json** | `use_first_row_as_header: "false"` | Returns raw `row_index` + `values` (array of strings) instead of named columns. Use when the sheet has no header row or you want positional columns. |
| **pipeline_spec_sheet_values_custom_sheet_and_range.json** | `sheet_name` + `range` | Custom tab name (e.g. `"Data"`) and A1 range (e.g. `"A1:D100"`). Defaults are `Sheet1` and `A:Z`. |
| **pipeline_spec_sheet_values_spreadsheetId.json** | `spreadsheetId` | Same as `spreadsheet_id`; alternative parameter name (both are in the allowlist). |
| **pipeline_spec_example_common_options.json** | `scd_type`, `sequence_by` | Example using common pipeline options: SCD Type 2 and `sequence_by` for sheet_values; SCD Type 1 for documents with content. |

## Table config reference

### spreadsheets

- No `table_configuration` (Drive file list for spreadsheets; paginated automatically).

### sheet_values

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `spreadsheet_id` or `spreadsheetId` | Yes | — | Google Spreadsheet ID (from URL or from a spreadsheets sync). |
| `sheet_name` | No | `"Sheet1"` | Sheet (tab) name. |
| `range` | No | `"A:Z"` | A1 notation range (e.g. `A:Z`, `A1:D100`). If no `!` is present, combined with `sheet_name`. |
| `use_first_row_as_header` | No | `"true"` | `"true"`: first row = column names, named columns + `row_index`; use first column as primary key for SCD Type 2. `"false"`: raw `row_index` + `values` array. |

### documents

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `include_content` | No | not set | `"true"`, `"1"`, or `"yes"` to fetch document body as plain text (Drive export; 10 MB limit per doc). Omit for metadata only. |

### Common options (all tables)

Set inside `table_configuration` alongside source-specific options:

| Option | Description |
|--------|-------------|
| `scd_type` | `SCD_TYPE_1` (default) or `SCD_TYPE_2`. |
| `primary_keys` | List of column names to override the connector’s default primary keys. |
| `sequence_by` | Column used to order records for SCD Type 2 change tracking. |

## Before running

1. Set `connection_name` in each JSON to your Unity Catalog connection (e.g. `google_sheets_docs_connection`).
2. For any **sheet_values** pipeline, set `spreadsheet_id` (or `spreadsheetId`) to the target spreadsheet ID (from the sheet URL or from a **spreadsheets** sync).
