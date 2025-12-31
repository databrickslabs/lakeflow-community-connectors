# Test and Fix the Connector

## Goal
Validate the generated connector for **{{source_name}}** by executing the provided test suite, diagnosing failures, and applying minimal, targeted fixes until all tests pass.

## Instructions

1. Create a `test_{source_name}_lakeflow_connect.py` under `tests/unit/sources/{source_name}/` directory.
2. Use `tests/unit/sources/test_suite.py` to run test and follow `tests/unit/sources/example/test_example_lakeflow_connect.py` or other sources as an example.
3. Use the configuration file `tests/unit/sources/{source_name}/configs/dev_config.json` and `tests/unit/sources/{source_name}/configs/dev_table_config.json` to initialize your tests.
   - example:
```json
{
  "user": "YOUR_USER_NAME",
  "password": "YOUR_PASSWORD",
  "token": "YOUR_TOKEN"
}
```
   - If `dev_config.json` does not exist, create it and ask the developers to provide the required parameters to connect to a test instance of the source.
   - If needed, create `dev_table_config.json` and ask developers to supply the necessary table_options parameters for testing different cases.
   - Be sure to remove these config files after testing is complete and before committing any changes.
4. Run the tests using: `pytest tests/unit/sources/{source_name}/test_{source_name}_lakeflow_connect.py -v`
5. Based on test failures, update the implementation under `src/databricks/labs/community_connector/sources/{source_name}` as needed. Use both the test results and the source API documentation, as well as any relevant libraries and test code, to guide your corrections.

## Part 2: Validate Schemas Against Live API

**After all tests pass**, validate that your connector schemas accurately match actual API responses. This catches discrepancies that tests might miss.

### Why Schema Validation Matters

- **API docs can be wrong**: Fields documented but not actually returned
- **Field names vary**: `sendDate` vs `sentDate`, `id` vs `contactId`
- **Nested structures differ**: API may return richer nested data than documented
- **Prevents production issues**: Catches schema problems before deployment

### Instructions

1. **Copy the validation script** from example:
   ```bash
   cp sources/example/test/validate_example_schemas.py sources/{source_name}/test/validate_{source_name}_schemas.py
   ```

2. **Update imports** in the copied file:
   ```python
   # Change:
   from sources.example.example import LakeflowConnect

   # To:
   from sources.{source_name}.{source_name} import LakeflowConnect
   ```

3. **Run validation**:
   ```bash
   python sources/{source_name}/test/validate_{source_name}_schemas.py
   ```

4. **Review results**:
   - **✅ PERFECT MATCH**: All fields match - no action needed
   - **✅ Nested fields covered**: MapType/StructType fields properly handled - expected
   - **⚠️ DISCREPANCIES**: Schema mismatches found - fix required

5. **Fix discrepancies** (if any):
   ```python
   # Add missing field to schema:
   StructField("missingFieldName", StringType(), True),

   # Remove field not in API or document why it's missing
   ```

6. **Update documentation** - Add validation section to `{source_name}_api_doc.md`:
   ```markdown
   ## Schema Validation Against Live API

   **Validation Date**: YYYY-MM-DD

   | Table | Status | Notes |
   |-------|--------|-------|
   | table1 | ✅ Perfect Match | All fields validated |
   ```

7. **Re-run tests** and **regenerate merged file**:
   ```bash
   pytest sources/{source_name}/test/ -v
   python scripts/merge_python_source.py {source_name}
   ```

### Understanding Nested Fields

**MapType and StructType nested fields are EXPECTED** - the validator understands these patterns:

- **MapType** (dynamic keys): `values.QID1.choiceId` ✅ Covered
- **StructType** (fixed structure): `stats.sent` ✅ Covered

The validator shows: `✅ X nested fields correctly covered by MapType/StructType`

## Notes

- This step is interactive. Make adjustments based on test and validation results.
- Schema validation takes 2-5 minutes but prevents production issues.
- Remove `dev_config.json` after this step before committing.
- Avoid mocking data in tests. Config files will be supplied to enable connections to an actual instance.

