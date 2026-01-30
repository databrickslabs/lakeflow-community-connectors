# Testing Guide for Redshift Connector

This guide is for **local testing only**. For production deployment on Databricks, see [DATABRICKS_SETUP.md](./DATABRICKS_SETUP.md).

## ⚠️ Security Notice

**This testing approach uses AWS credentials for local development ONLY.**

For production:
- ✅ Use IAM Instance Profiles (no credentials needed)
- ✅ Use AWS Secrets Manager
- ❌ Never commit credentials to version control

## Quick Start

### Step 1: Get Your AWS Credentials

You need AWS credentials to call the Redshift Data API:

**Option A: Permanent Credentials (Recommended for Testing)**
1. Go to AWS Console → IAM → Users
2. Select your user → Security credentials
3. Click "Create access key" → Choose "Command Line Interface (CLI)"
4. Save your:
   - Access Key ID (starts with `AKIA`)
   - Secret Access Key

**Option B: Temporary Credentials (More Secure)**
1. Get temporary credentials from your AWS admin
2. You'll need:
   - Access Key ID (starts with `ASIA`)
   - Secret Access Key
   - Session Token (expires in 1-12 hours)

### Step 2: Update Configuration Files

**File:** `sources/redshift/configs/dev_config.json`

Replace these placeholder values with your actual credentials:
```json
{
    "region": "us-east-1",
    "database": "dev",
    "cluster_identifier": "rg-redshift-connector",
    "secret_arn": "arn:aws:secretsmanager:us-east-1:332745928618:secret:redshift!rg-redshift-connector-rg-awsuser-S9ZSTM",
    "access_key_id": "YOUR_AWS_ACCESS_KEY_ID",
    "secret_access_key": "YOUR_AWS_SECRET_ACCESS_KEY",
    "session_token": "YOUR_AWS_SESSION_TOKEN",
    "schema_filter": "public",
    "poll_interval": "2",
    "max_poll_attempts": "300"
}
```

**Notes:**
- If using permanent credentials (AKIA), you can **remove** the `session_token` line
- If using temporary credentials (ASIA), **keep** the `session_token` line
- The `secret_arn` contains your database credentials (username/password)

**File:** `sources/redshift/configs/dev_table_config.json`

Already configured to test with the `public.users` table (TICKIT sample schema):
```json
{
    "test_table": "public.users",
    "table_options": {
        "limit": "100"
    }
}
```

### Step 3: Run Tests

#### Using the Test Script (Recommended)

```bash
cd /Users/ram.goli/lakeflow-community-connectors

# Set your AWS credentials as environment variables
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_SESSION_TOKEN="your-session-token"  # Only if using temporary credentials

# Run the test script
./test_local.sh
```

#### Manual Testing

```bash
cd /Users/ram.goli/lakeflow-community-connectors

# Activate virtual environment
source .venv/bin/activate

# Run tests
pytest sources/redshift/test/test_redshift_lakeflow_connect.py -v

# Clean up config files after testing
rm sources/redshift/configs/dev_config.json
rm sources/redshift/configs/dev_table_config.json
```

## What the Tests Do

The test suite validates:

1. ✅ **Initialization** - Connector initializes with config
2. ✅ **List Tables** - Can discover tables in Redshift
3. ✅ **Get Table Schema** - Can retrieve column metadata
4. ✅ **Read Table Metadata** - Can get primary keys and ingestion type
5. ✅ **Read Table** - Can read data from Redshift
6. ✅ **Read Table Deletes** - Confirms snapshot-only ingestion

## Expected Output

```
============================= test session starts ==============================
sources/redshift/test/test_redshift_lakeflow_connect.py::test_redshift_connector PASSED [100%]

==================================================
LAKEFLOW CONNECT TEST REPORT
==================================================
Connector Class: LakeflowConnect
Timestamp: 2026-01-09T...

SUMMARY:
  Total Tests: 6
  Passed: 6
  Failed: 0
  Errors: 0
  Success Rate: 100.0%

TEST RESULTS:
--------------------------------------------------
✅ test_initialization
✅ test_list_tables
✅ test_get_table_schema
✅ test_read_table_metadata
✅ test_read_table
✅ test_read_table_deletes

======================== 1 passed in XXXs =========================
```

## Troubleshooting

### Error: "The security token included in the request is invalid"

**Cause:** Session token expired or incorrect credentials

**Solution:** 
- Get fresh temporary credentials
- Update all three values: `access_key_id`, `secret_access_key`, `session_token`

### Error: "Redshift endpoint doesn't exist in this region"

**Cause:** Wrong region specified

**Solution:** Check your cluster's actual region in AWS Console and update `region` in config

### Error: "Access Denied" to Secrets Manager

**Cause:** IAM user/role lacks permission to access the secret

**Solution:** Add this policy to your IAM user/role:
```json
{
    "Effect": "Allow",
    "Action": "secretsmanager:GetSecretValue",
    "Resource": "arn:aws:secretsmanager:us-east-1:332745928618:secret:redshift!rg-redshift-connector-rg-awsuser-*"
}
```

### Error: "aws: command not found" in test_local.sh

**Cause:** AWS CLI not installed (optional, script still works)

**Solution:** Just ignore this message or install AWS CLI with `brew install awscli`

## Security Reminders

After testing:
- 🧹 **Clean up config files** (they contain credentials)
- 🔐 **Rotate credentials** if they were exposed
- 📝 **Never commit credentials** to git

The test script automatically cleans up config files. If running manually, remember to delete them:

```bash
rm sources/redshift/configs/dev_config.json
rm sources/redshift/configs/dev_table_config.json
```

## Next Steps

Once local testing passes:
1. See [DATABRICKS_SETUP.md](./DATABRICKS_SETUP.md) for production deployment
2. Configure instance profiles for secure credential-free authentication
3. Test on Databricks cluster with Unity Catalog connection
