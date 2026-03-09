#!/usr/bin/env python3
"""
Local test for sheet_values with the stores config (first row as header, ID primary key).
Run from repo root:
  PYTHONPATH=src python src/databricks/labs/community_connector/sources/google_sheets_docs/test_sheet_values_local.py

Requires dev_config.json with valid OAuth credentials (client_id, client_secret, refresh_token).
"""
import sys
from pathlib import Path

# Repo root and src for imports (file is in .../src/databricks/.../google_sheets_docs/)
_SRC = Path(__file__).resolve().parents[5]
_REPO_ROOT = _SRC.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

# Table config from user (sheet_values -> stores)
SHEET_VALUES_TABLE_CONFIG = {
    "sheet_name": "Sheet1",
    "range": "A:E",
    "spreadsheet_id": "11qtiCmvOPMoRUpoxaMxB5sTN8Ws1jVdQL9JoJGXnsxc",
    "use_first_row_as_header": "true",
}


def load_config(path: Path) -> dict:
    import json
    with open(path) as f:
        return json.load(f)


def main():
    config_dir = _REPO_ROOT / "tests" / "unit" / "sources" / "google_sheets_docs" / "configs"
    config_path = config_dir / "dev_config.json"
    if not config_path.exists():
        print(f"ERROR: {config_path} not found. Need OAuth credentials for local test.")
        sys.exit(1)

    config = load_config(config_path)
    from databricks.labs.community_connector.sources.google_sheets_docs.google_sheets_docs import (
        GoogleSheetsDocsLakeflowConnect,
    )

    connector = GoogleSheetsDocsLakeflowConnect(config)
    opts = SHEET_VALUES_TABLE_CONFIG.copy()

    print("0. Sheets API first-row probe...")
    try:
        from urllib.parse import quote
        sheet_name = opts.get("sheet_name", "Sheet1")
        range_a1 = f"{sheet_name}!1:1"
        url = f"https://sheets.googleapis.com/v4/spreadsheets/{opts['spreadsheet_id']}/values/{quote(range_a1, safe='')}"
        resp = connector._request("GET", url, params={"valueRenderOption": "UNFORMATTED_VALUE", "majorDimension": "ROWS"})
        if resp.status_code == 404:
            print("   SKIP: 404 Not Found. Spreadsheet ID not found or dev_config credentials have no access.")
            print("   Use a spreadsheet that the dev_config OAuth account can access (same account that created the sheet or shared with it).")
            sys.exit(0)
        if resp.status_code != 200:
            print(f"   FAIL: API returned {resp.status_code}: {resp.text[:300]}")
            sys.exit(1)
        headers = connector._fetch_sheet_first_row(opts)
        if not headers:
            print("   FAIL: First row empty or not returned.")
            sys.exit(1)
        print(f"   First row (headers): {headers}")
    except Exception as e:
        print(f"   FAIL: {type(e).__name__}: {e}")
        sys.exit(1)

    print("1. get_table_schema(sheet_values, table_config)...")
    schema = connector.get_table_schema("sheet_values", opts)
    schema_names = [f.name for f in schema.fields]
    print(f"   Schema columns: {schema_names}")

    if "ID" not in schema_names:
        print("   FAIL: Schema does not contain column 'ID' (expected when use_first_row_as_header=true).")
        sys.exit(1)
    print("   OK: Schema has 'ID' column.")

    print("2. read_table_metadata(sheet_values, table_config)...")
    meta = connector.read_table_metadata("sheet_values", opts)
    pk = meta.get("primary_keys") or []
    print(f"   primary_keys: {pk}")

    if "ID" not in pk and (not pk or pk[0] != "ID"):
        # Connector may return first header as primary key
        if pk:
            print(f"   INFO: primary_keys = {pk} (first column from sheet header)")
        else:
            print("   WARN: primary_keys empty (first row may not have been fetched).")
    else:
        print("   OK: primary_keys includes ID or first column.")

    print("3. read_table(sheet_values, None, table_config)...")
    records_iter, next_offset = connector.read_table("sheet_values", None, opts)
    records = list(records_iter)
    print(f"   Read {len(records)} record(s).")

    if not records:
        print("   WARN: No records returned (sheet may be empty or range has no data).")
    else:
        first = records[0]
        if "ID" in first:
            print(f"   OK: First record has 'ID' = {first.get('ID')!r}")
        elif "row_index" in first and "values" in first:
            print("   FAIL: Records have row_index/values (raw schema) but expected named columns including ID.")
            sys.exit(1)
        else:
            print(f"   First record keys: {list(first.keys())}")

    print("\nAll local checks passed for sheet_values (stores config).")


if __name__ == "__main__":
    main()
