from pathlib import Path
import unittest.mock as mock

from sources.alchemy.alchemy import LakeflowConnect
from tests import test_suite
from tests.test_suite import LakeflowConnectTester
from tests.test_utils import load_config


def test_alchemy_connector_core():
    """Test the Alchemy connector core functionality (without network calls)"""
    # Inject the LakeflowConnect class into test_suite module's namespace
    test_suite.LakeflowConnect = LakeflowConnect

    # Load configuration
    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"
    table_config_path = parent_dir / "configs" / "dev_table_config.json"

    config = load_config(config_path)
    table_config = load_config(table_config_path)

    # Create connector instance
    connector = LakeflowConnect(config)

    # Test basic functionality that doesn't require network calls
    print("Testing Alchemy connector core functionality...")

    # Test 1: Initialization
    assert connector.api_key == "nbZCASFPBi9sIqyeb8om7"
    assert connector.default_network == "eth-mainnet"
    print("✅ Initialization test passed")

    # Test 2: List tables
    tables = connector.list_tables()
    assert len(tables) == 18
    expected_tables = [
        "nfts_by_owner", "nfts_for_contract", "nft_metadata", "contract_metadata",
        "nft_metadata_batch", "contract_metadata_batch", "nft_sales", "floor_prices",
        "token_prices", "token_prices_by_address", "token_prices_historical",
        "tokens_by_wallet", "token_balances_by_wallet", "nfts_by_wallet",
        "nft_collections_by_wallet", "wallet_transactions", "webhooks", "webhook_addresses"
    ]
    assert set(tables) == set(expected_tables)
    print("✅ List tables test passed")

    # Test 3: Get table schemas
    for table in tables:
        schema = connector.get_table_schema(table, {})
        assert isinstance(schema, type(connector.get_table_schema("token_prices", {})))
        assert len(schema.fields) > 0
    print("✅ Get table schemas test passed")

    # Test 4: Read table metadata
    for table in tables:
        metadata = connector.read_table_metadata(table, {})
        required_keys = ["primary_keys", "cursor_field", "ingestion_type"]
        for key in required_keys:
            assert key in metadata
        assert metadata["ingestion_type"] in ["snapshot", "cdc", "cdc_with_deletes", "append"]
    print("✅ Read table metadata test passed")

    print("\n🎉 All core Alchemy connector tests passed!")
    print(f"📊 Successfully tested {len(tables)} tables")
    print("\n⚠️  Note: Network-dependent read_table tests require a valid Alchemy API key")
    print("🔧 To test actual API calls, ensure your API key has proper permissions and network access")


def test_alchemy_connector_with_mock():
    """Test the Alchemy connector with mocked network calls"""
    # Inject the LakeflowConnect class into test_suite module's namespace
    test_suite.LakeflowConnect = LakeflowConnect

    # Load configuration
    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"
    table_config_path = parent_dir / "configs" / "dev_table_config.json"

    config = load_config(config_path)
    table_config = load_config(table_config_path)

    # Create connector directly
    connector = LakeflowConnect(config)

    # Mock successful API responses for key tables
    mock_responses = {
        "token_prices": {
            "data": [
                {
                    "symbol": "ETH",
                    "prices": [{"currency": "usd", "value": "3000.00"}]
                }
            ]
        },
        "contract_metadata": {
            "contractAddress": "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D",
            "name": "BoredApeYachtClub",
            "symbol": "BAYC",
            "totalSupply": "10000",
            "tokenType": "ERC721"
        }
    }

    def mock_get(url, params=None, **kwargs):
        # Mock successful responses for supported endpoints
        if "tokens/by-symbol" in url:
            response = mock.MagicMock()
            response.json.return_value = mock_responses["token_prices"]
            return response
        elif "getContractMetadata" in url:
            response = mock.MagicMock()
            response.json.return_value = mock_responses["contract_metadata"]
            return response
        else:
            # For unsupported endpoints, raise connection error
            import requests
            raise requests.exceptions.ConnectionError("Mocked network error - endpoint not implemented in test")

    with mock.patch('requests.get', side_effect=mock_get):
        # Run only specific tests that we have mocks for
        test_results = []

        # Test the tables we have mocks for
        mock_supported_tables = ["token_prices", "contract_metadata"]

        for table_name in mock_supported_tables:
            try:
                table_options = table_config.get(table_name, {})
                records, offset = connector.read_table(table_name, {}, table_options)
                list(records)  # Consume the iterator
                test_results.append({"table": table_name, "status": "PASSED"})
                print(f"✅ {table_name} read_table test passed")
            except Exception as e:
                test_results.append({"table": table_name, "status": "ERROR", "error": str(e)})
                print(f"❌ {table_name} read_table test failed: {e}")

        # Check results
        passed_count = sum(1 for r in test_results if r["status"] == "PASSED")
        print(f"\n📊 Mock test results: {passed_count}/{len(mock_supported_tables)} tables passed")

        assert passed_count == len(mock_supported_tables), f"Some mock tests failed: {test_results}"