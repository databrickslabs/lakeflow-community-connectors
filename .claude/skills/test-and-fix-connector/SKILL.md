---
name: test-and-fix-connector
description: Validate a connector by running the test suite, diagnosing failures, and applying fixes until all tests pass.
---

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
4. Run the tests using the project virtual environment (Python 3.10+ required):
```bash
python3.10 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest tests/unit/sources/{source_name}/test_{source_name}_lakeflow_connect.py -v
```
5. Based on test failures, update the implementation under `src/databricks/labs/community_connector/sources/{source_name}` as needed. Use both the test results and the source API documentation, as well as any relevant libraries and test code, to guide your corrections.

## Notes

- This step is more interactive. Based on testing results, we need to make various adjustments.
- Remove the `dev_config.json` after this step.
- Avoid mocking data in tests. Config files will be supplied to enable connections to an actual instance.

## Git Commit on Completion

After all tests pass, commit any changes to the connector implementation and test files before returning:

```bash
git add src/databricks/labs/community_connector/sources/{source_name}/
git add tests/unit/sources/{source_name}/
git commit -m "Fix {source_name} connector and add tests"
```

Only commit files that were actually modified or created. Use the exact source name in the commit message. Do not push — only commit locally.
