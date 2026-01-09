#!/bin/bash
# Local testing script for Redshift connector
# Usage: ./test_local.sh

echo "🔧 Setting up environment for local testing..."

# Set your AWS credentials here (or export them before running this script)
# export AWS_ACCESS_KEY_ID="your-access-key"
# export AWS_SECRET_ACCESS_KEY="your-secret-key"
# export AWS_SESSION_TOKEN="your-token"  # Only if using temporary credentials

# Check if credentials are set
if [ -z "$AWS_ACCESS_KEY_ID" ]; then
    echo "❌ AWS_ACCESS_KEY_ID not set!"
    echo "Please export your AWS credentials first:"
    echo "  export AWS_ACCESS_KEY_ID='your-key'"
    echo "  export AWS_SECRET_ACCESS_KEY='your-secret'"
    exit 1
fi

echo "✅ AWS credentials detected"
echo "📍 Region: us-east-1"
echo "🗄️  Cluster: rg-redshift-connector"
echo "🔐 Using Secrets Manager for database auth"
echo ""

# Activate virtual environment
source .venv/bin/activate

# Run tests
echo "🧪 Running Redshift connector tests..."
pytest sources/redshift/test/test_redshift_lakeflow_connect.py -v

# Cleanup
echo ""
echo "🧹 Cleaning up test config files..."
rm -f sources/redshift/configs/dev_config.json
rm -f sources/redshift/configs/dev_table_config.json

echo "✅ Testing complete!"
