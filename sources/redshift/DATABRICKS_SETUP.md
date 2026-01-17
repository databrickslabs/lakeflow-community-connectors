# Databricks Setup Guide for Redshift Connector

This guide walks you through setting up the Redshift connector in Databricks using instance profiles (no hardcoded credentials needed!).

## Prerequisites

- AWS Account with permissions to create instance profiles
- Databricks workspace on AWS
- Redshift cluster with Secrets Manager configured

## Step 1: Create Instance Profile (AWS Console)

### 1.1 Create the Instance Profile

```bash
# Using AWS CLI
aws iam create-instance-profile \
  --instance-profile-name AmazonRedshift-CommandsAccessRole-20260109T114712

# Add the existing role to the instance profile
aws iam add-role-to-instance-profile \
  --instance-profile-name AmazonRedshift-CommandsAccessRole-20260109T114712 \
  --role-name AmazonRedshift-CommandsAccessRole-20260109T114712
```

Or via AWS Console:
1. Go to **IAM** → **Roles**
2. Select your role: `AmazonRedshift-CommandsAccessRole-20260109T114712`
3. Note: AWS automatically creates an instance profile with the same name when you create a role via console

### 1.2 Verify Instance Profile ARN

Your instance profile ARN should be:
```
arn:aws:iam::332745928618:instance-profile/AmazonRedshift-CommandsAccessRole-20260109T114712
```

### 1.3 Update IAM Trust Policy (if needed)

The role must trust the Databricks AWS account. Your role should have a trust relationship that includes:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "redshift.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    },
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "YOUR_DATABRICKS_EXTERNAL_ID"
        }
      }
    }
  ]
}
```

## Step 2: Add Instance Profile to Databricks

### 2.1 In Databricks Workspace

1. Go to **Workspace settings** → **Security** → **Instance profiles**
2. Click **Add instance profile**
3. Fill in:
   - **Instance profile ARN**: `arn:aws:iam::332745928618:instance-profile/AmazonRedshift-CommandsAccessRole-20260109T114712`
   - **IAM role ARN**: `arn:aws:iam::332745928618:role/service-role/AmazonRedshift-CommandsAccessRole-20260109T114712`
4. Click **Add**

## Step 3: Create Unity Catalog Connection

### 3.1 Connection Configuration

```sql
CREATE CONNECTION redshift_connection
TYPE redshift
OPTIONS (
  region 'us-east-1',
  database 'dev',
  cluster_identifier 'rg-redshift-connector',
  secret_arn 'arn:aws:secretsmanager:us-east-1:332745928618:secret:redshift!rg-redshift-connector-rg-awsuser-S9ZSTM',
  schema_filter 'public'
);
```

### 3.2 Key Points

- ✅ **No AWS credentials needed** - Uses instance profile automatically
- ✅ **Database credentials** - Stored securely in Secrets Manager
- ✅ **Secrets Manager access** - IAM role has permission to access the secret

## Step 4: Test the Connection

### 4.1 Create a Cluster with Instance Profile

1. Create a new cluster
2. In **Advanced options** → **Instance Profile**, select:
   `AmazonRedshift-CommandsAccessRole-20260109T114712`
3. Start the cluster

### 4.2 Test Query

```python
# Read from Redshift table
df = spark.read.table("redshift_connection.public.users")
df.show()
```

## Troubleshooting

### Issue: "Access Denied" when accessing Secrets Manager

**Solution**: Ensure the IAM role has the `AmazonRedshiftAllCommandsFullAccess` policy attached, or add custom policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "redshift-data:ExecuteStatement",
        "redshift-data:DescribeStatement",
        "redshift-data:GetStatementResult",
        "redshift:GetClusterCredentials"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "secretsmanager:GetSecretValue"
      ],
      "Resource": "arn:aws:secretsmanager:us-east-1:332745928618:secret:redshift!rg-redshift-connector-rg-awsuser-*"
    }
  ]
}
```

### Issue: "Instance profile not found"

**Solution**: Make sure you added the instance profile ARN (not role ARN) to Databricks workspace settings.

## Required IAM Permissions

Your IAM role needs these permissions:
- ✅ `redshift-data:ExecuteStatement`
- ✅ `redshift-data:DescribeStatement`
- ✅ `redshift-data:GetStatementResult`
- ✅ `redshift:GetClusterCredentials`
- ✅ `secretsmanager:GetSecretValue`

## Security Best Practices

1. ✅ Use Secrets Manager for database credentials
2. ✅ Use instance profiles (no hardcoded AWS credentials)
3. ✅ Limit IAM role permissions to only what's needed
4. ✅ Use schema_filter to limit access to specific schemas
5. ✅ Enable CloudTrail logging for audit trails

---

For more information, see:
- [Databricks Instance Profiles Documentation](https://docs.databricks.com/administration-guide/cloud-configurations/aws/instance-profiles.html)
- [AWS Redshift Data API Documentation](https://docs.aws.amazon.com/redshift/latest/mgmt/data-api.html)
