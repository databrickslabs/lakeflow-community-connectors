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


# =============================================================================
# NFT API ENDPOINT TESTS
# =============================================================================

def test_nfts_by_owner():
    """Test the nfts_by_owner endpoint (GET /getNFTsForOwner)"""
    parent_dir = Path(__file__).parent.parent
    config = load_config(parent_dir / "configs" / "dev_config.json")
    connector = LakeflowConnect(config)
    
    captured_params = {}
    
    def mock_get(url, params=None, **kwargs):
        nonlocal captured_params
        captured_params = params or {}
        
        assert "getNFTsForOwner" in url
        response = mock.MagicMock()
        response.json.return_value = {
            "ownedNfts": [
                {
                    "contract": {"address": "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D"},
                    "tokenId": "1234",
                    "tokenType": "ERC721",
                    "title": "Bored Ape #1234",
                    "description": "A Bored Ape",
                    "rawMetadata": {"name": "Bored Ape #1234"},
                    "tokenUri": {"raw": "ipfs://..."},
                    "media": []
                }
            ],
            "pageKey": None
        }
        response.raise_for_status = mock.MagicMock()
        return response
    
    with mock.patch('requests.get', side_effect=mock_get):
        table_options = {"owner_address": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045", "page_size": "10"}
        records, offset = connector.read_table("nfts_by_owner", {}, table_options)
        records_list = list(records)
        
        assert "owner" in captured_params
        assert captured_params["owner"] == "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
        assert len(records_list) == 1
        print("✅ nfts_by_owner test passed")


def test_nfts_for_contract():
    """Test the nfts_for_contract endpoint (GET /getNFTsForContract)"""
    parent_dir = Path(__file__).parent.parent
    config = load_config(parent_dir / "configs" / "dev_config.json")
    connector = LakeflowConnect(config)
    
    captured_params = {}
    
    def mock_get(url, params=None, **kwargs):
        nonlocal captured_params
        captured_params = params or {}
        
        assert "getNFTsForContract" in url
        response = mock.MagicMock()
        response.json.return_value = {
            "nfts": [
                {
                    "contract": {"address": "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D"},
                    "tokenId": "1",
                    "tokenType": "ERC721"
                }
            ]
        }
        response.raise_for_status = mock.MagicMock()
        return response
    
    with mock.patch('requests.get', side_effect=mock_get):
        table_options = {"contract_address": "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D"}
        records, offset = connector.read_table("nfts_for_contract", {}, table_options)
        records_list = list(records)
        
        assert "contractAddress" in captured_params
        assert len(records_list) == 1
        print("✅ nfts_for_contract test passed")


def test_nft_metadata():
    """Test the nft_metadata endpoint (GET /getNFTMetadata)"""
    parent_dir = Path(__file__).parent.parent
    config = load_config(parent_dir / "configs" / "dev_config.json")
    connector = LakeflowConnect(config)
    
    captured_params = {}
    
    def mock_get(url, params=None, **kwargs):
        nonlocal captured_params
        captured_params = params or {}
        
        assert "getNFTMetadata" in url
        response = mock.MagicMock()
        response.json.return_value = {
            "contract": {"address": "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D", "name": "BAYC"},
            "tokenId": "1",
            "tokenType": "ERC721",
            "title": "Bored Ape #1",
            "description": "A Bored Ape",
            "rawMetadata": {"name": "Bored Ape #1", "attributes": []},
            "tokenUri": {"raw": "ipfs://...", "gateway": "https://..."},
            "media": []
        }
        response.raise_for_status = mock.MagicMock()
        return response
    
    with mock.patch('requests.get', side_effect=mock_get):
        table_options = {"contract_address": "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D", "token_id": "1"}
        records, offset = connector.read_table("nft_metadata", {}, table_options)
        records_list = list(records)
        
        assert captured_params["contractAddress"] == "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D"
        assert captured_params["tokenId"] == "1"
        assert len(records_list) == 1
        print("✅ nft_metadata test passed")


def test_contract_metadata():
    """Test the contract_metadata endpoint (GET /getContractMetadata)"""
    parent_dir = Path(__file__).parent.parent
    config = load_config(parent_dir / "configs" / "dev_config.json")
    connector = LakeflowConnect(config)
    
    captured_params = {}
    
    def mock_get(url, params=None, **kwargs):
        nonlocal captured_params
        captured_params = params or {}
        
        assert "getContractMetadata" in url
        response = mock.MagicMock()
        response.json.return_value = {
            "address": "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D",
            "name": "BoredApeYachtClub",
            "symbol": "BAYC",
            "totalSupply": "10000",
            "tokenType": "ERC721"
        }
        response.raise_for_status = mock.MagicMock()
        return response
    
    with mock.patch('requests.get', side_effect=mock_get):
        table_options = {"contract_address": "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D"}
        records, offset = connector.read_table("contract_metadata", {}, table_options)
        records_list = list(records)
        
        assert captured_params["contractAddress"] == "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D"
        assert len(records_list) == 1
        assert records_list[0]["contractAddress"] == "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D"
        print("✅ contract_metadata test passed")


def test_nft_metadata_batch():
    """Test the nft_metadata_batch endpoint (POST /getNFTMetadataBatch)"""
    parent_dir = Path(__file__).parent.parent
    config = load_config(parent_dir / "configs" / "dev_config.json")
    connector = LakeflowConnect(config)
    
    captured_body = {}
    
    def mock_post(url, json=None, **kwargs):
        nonlocal captured_body
        captured_body = json or {}
        
        assert "getNFTMetadataBatch" in url
        response = mock.MagicMock()
        response.json.return_value = [
            {
                "contract": {"address": "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D"},
                "tokenId": "1",
                "tokenType": "ERC721"
            },
            {
                "contract": {"address": "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D"},
                "tokenId": "2",
                "tokenType": "ERC721"
            }
        ]
        response.raise_for_status = mock.MagicMock()
        return response
    
    with mock.patch('requests.post', side_effect=mock_post):
        table_options = {"tokens": "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D:1,0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D:2"}
        records, offset = connector.read_table("nft_metadata_batch", {}, table_options)
        records_list = list(records)
        
        assert "tokens" in captured_body
        assert len(captured_body["tokens"]) == 2
        assert len(records_list) == 2
        print("✅ nft_metadata_batch test passed")


def test_contract_metadata_batch():
    """Test the contract_metadata_batch endpoint (POST /getContractMetadataBatch)"""
    parent_dir = Path(__file__).parent.parent
    config = load_config(parent_dir / "configs" / "dev_config.json")
    connector = LakeflowConnect(config)
    
    captured_body = {}
    
    def mock_post(url, json=None, **kwargs):
        nonlocal captured_body
        captured_body = json or {}
        
        assert "getContractMetadataBatch" in url
        response = mock.MagicMock()
        response.json.return_value = [
            {"address": "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D", "name": "BAYC"},
            {"address": "0x60E4d786628Fea6478F785A6d7e704777c86a7c6", "name": "MAYC"}
        ]
        response.raise_for_status = mock.MagicMock()
        return response
    
    with mock.patch('requests.post', side_effect=mock_post):
        table_options = {"contract_addresses": "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D,0x60E4d786628Fea6478F785A6d7e704777c86a7c6"}
        records, offset = connector.read_table("contract_metadata_batch", {}, table_options)
        records_list = list(records)
        
        assert "contractAddresses" in captured_body
        assert len(captured_body["contractAddresses"]) == 2
        assert len(records_list) == 2
        print("✅ contract_metadata_batch test passed")


def test_nft_sales():
    """Test the nft_sales endpoint (GET /getNFTSales)"""
    parent_dir = Path(__file__).parent.parent
    config = load_config(parent_dir / "configs" / "dev_config.json")
    connector = LakeflowConnect(config)
    
    captured_params = {}
    
    def mock_get(url, params=None, **kwargs):
        nonlocal captured_params
        captured_params = params or {}
        
        assert "getNFTSales" in url
        response = mock.MagicMock()
        response.json.return_value = {
            "nftSales": [
                {
                    "contractAddress": "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D",
                    "tokenId": "1234",
                    "transactionHash": "0x123...",
                    "logIndex": 0,
                    "blockNumber": 15000000,
                    "sellerAddress": "0x111...",
                    "buyerAddress": "0x222...",
                    "sellerFee": {"amount": "1000000000000000000"},
                    "protocolFee": {"amount": "25000000000000000"},
                    "royaltyFee": {"amount": "25000000000000000"},
                    "marketplace": "opensea",
                    "quantity": "1",
                    "taker": "BUYER"
                }
            ],
            "pageKey": None
        }
        response.raise_for_status = mock.MagicMock()
        return response
    
    with mock.patch('requests.get', side_effect=mock_get):
        table_options = {"contract_address": "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D", "order": "desc"}
        records, offset = connector.read_table("nft_sales", {}, table_options)
        records_list = list(records)
        
        assert captured_params["order"] == "desc"
        assert len(records_list) == 1
        print("✅ nft_sales test passed")


def test_floor_prices():
    """Test the floor_prices endpoint (GET /getFloorPrice)"""
    parent_dir = Path(__file__).parent.parent
    config = load_config(parent_dir / "configs" / "dev_config.json")
    connector = LakeflowConnect(config)
    
    captured_params = {}
    
    def mock_get(url, params=None, **kwargs):
        nonlocal captured_params
        captured_params = params or {}
        
        assert "getFloorPrice" in url
        response = mock.MagicMock()
        response.json.return_value = {
            "openSea": {"floorPrice": 25.5, "priceCurrency": "ETH"},
            "looksRare": {"floorPrice": 24.8, "priceCurrency": "ETH"}
        }
        response.raise_for_status = mock.MagicMock()
        return response
    
    with mock.patch('requests.get', side_effect=mock_get):
        table_options = {"contract_address": "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D"}
        records, offset = connector.read_table("floor_prices", {}, table_options)
        records_list = list(records)
        
        assert captured_params["contractAddress"] == "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D"
        assert len(records_list) == 1
        assert records_list[0]["contractAddress"] == "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D"
        print("✅ floor_prices test passed")


# =============================================================================
# TOKEN/PRICE API ENDPOINT TESTS
# =============================================================================

def test_token_prices():
    """Test the token_prices endpoint (GET /tokens/by-symbol)"""
    parent_dir = Path(__file__).parent.parent
    config = load_config(parent_dir / "configs" / "dev_config.json")
    connector = LakeflowConnect(config)
    
    captured_params = {}
    
    def mock_get(url, params=None, **kwargs):
        nonlocal captured_params
        captured_params = params or {}
        
        assert "tokens/by-symbol" in url
        response = mock.MagicMock()
        response.json.return_value = {
            "data": [
                {"symbol": "ETH", "prices": [{"currency": "usd", "value": "3000.00"}]},
                {"symbol": "BTC", "prices": [{"currency": "usd", "value": "60000.00"}]}
            ]
        }
        response.raise_for_status = mock.MagicMock()
        return response
    
    with mock.patch('requests.get', side_effect=mock_get):
        table_options = {"symbols": "ETH,BTC"}
        records, offset = connector.read_table("token_prices", {}, table_options)
        records_list = list(records)
        
        assert "symbols" in captured_params
        assert isinstance(captured_params["symbols"], list)
        assert captured_params["symbols"] == ["ETH", "BTC"]
        assert len(records_list) == 2
        print("✅ token_prices test passed")


def test_token_prices_by_address():
    """Test the token_prices_by_address endpoint (POST /tokens/by-address)"""
    parent_dir = Path(__file__).parent.parent
    config = load_config(parent_dir / "configs" / "dev_config.json")
    connector = LakeflowConnect(config)
    
    captured_body = {}
    
    def mock_post(url, json=None, **kwargs):
        nonlocal captured_body
        captured_body = json or {}
        
        assert "tokens/by-address" in url
        response = mock.MagicMock()
        response.json.return_value = {
            "data": [
                {
                    "network": "eth-mainnet",
                    "address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
                    "prices": [{"currency": "usd", "value": "1.00"}]
                }
            ]
        }
        response.raise_for_status = mock.MagicMock()
        return response
    
    with mock.patch('requests.post', side_effect=mock_post):
        table_options = {"addresses": "eth-mainnet:0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"}
        records, offset = connector.read_table("token_prices_by_address", {}, table_options)
        records_list = list(records)
        
        assert "addresses" in captured_body
        assert len(records_list) == 1
        print("✅ token_prices_by_address test passed")


def test_token_prices_historical():
    """Test the token_prices_historical endpoint (POST /tokens/historical)"""
    parent_dir = Path(__file__).parent.parent
    config = load_config(parent_dir / "configs" / "dev_config.json")
    connector = LakeflowConnect(config)
    
    captured_body = {}
    
    def mock_post(url, json=None, **kwargs):
        nonlocal captured_body
        captured_body = json or {}
        
        assert "tokens/historical" in url
        response = mock.MagicMock()
        response.json.return_value = {
            "data": [
                {"symbol": "ETH", "timestamp": "2024-01-01T00:00:00Z", "value": "2500.00"},
                {"symbol": "ETH", "timestamp": "2024-01-02T00:00:00Z", "value": "2600.00"}
            ]
        }
        response.raise_for_status = mock.MagicMock()
        return response
    
    with mock.patch('requests.post', side_effect=mock_post):
        table_options = {
            "symbol": "ETH",
            "start_time": "2024-01-01T00:00:00Z",
            "end_time": "2024-01-02T00:00:00Z"
        }
        records, offset = connector.read_table("token_prices_historical", {}, table_options)
        records_list = list(records)
        
        assert captured_body["symbol"] == "ETH"
        assert captured_body["startTime"] == "2024-01-01T00:00:00Z"
        assert captured_body["endTime"] == "2024-01-02T00:00:00Z"
        assert len(records_list) == 2
        print("✅ token_prices_historical test passed")


# =============================================================================
# PORTFOLIO API ENDPOINT TESTS
# =============================================================================

def test_tokens_by_wallet():
    """Test the tokens_by_wallet endpoint (POST /getTokensByWallet)"""
    parent_dir = Path(__file__).parent.parent
    config = load_config(parent_dir / "configs" / "dev_config.json")
    connector = LakeflowConnect(config)
    
    captured_body = {}
    
    def mock_post(url, json=None, **kwargs):
        nonlocal captured_body
        captured_body = json or {}
        
        assert "getTokensByWallet" in url
        response = mock.MagicMock()
        response.json.return_value = [
            {
                "address": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",
                "network": "ETH_MAINNET",
                "tokenBalances": [
                    {"contractAddress": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", "tokenBalance": "1000000000"}
                ]
            }
        ]
        response.raise_for_status = mock.MagicMock()
        return response
    
    with mock.patch('requests.post', side_effect=mock_post):
        table_options = {"addresses": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"}
        records, offset = connector.read_table("tokens_by_wallet", {}, table_options)
        records_list = list(records)
        
        assert "addresses" in captured_body
        assert len(captured_body["addresses"]) == 1
        assert captured_body["addresses"][0]["network"] == "ETH_MAINNET"
        assert len(records_list) == 1
        print("✅ tokens_by_wallet test passed")


def test_token_balances_by_wallet():
    """Test the token_balances_by_wallet endpoint (POST /getTokenBalancesByWallet)"""
    parent_dir = Path(__file__).parent.parent
    config = load_config(parent_dir / "configs" / "dev_config.json")
    connector = LakeflowConnect(config)
    
    captured_body = {}
    
    def mock_post(url, json=None, **kwargs):
        nonlocal captured_body
        captured_body = json or {}
        
        assert "getTokenBalancesByWallet" in url
        response = mock.MagicMock()
        response.json.return_value = [
            {
                "address": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",
                "network": "ETH_MAINNET",
                "tokenBalances": [
                    {"contractAddress": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", "tokenBalance": "1000000000"}
                ]
            }
        ]
        response.raise_for_status = mock.MagicMock()
        return response
    
    with mock.patch('requests.post', side_effect=mock_post):
        table_options = {"addresses": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"}
        records, offset = connector.read_table("token_balances_by_wallet", {}, table_options)
        records_list = list(records)
        
        assert "addresses" in captured_body
        assert captured_body["addresses"][0]["network"] == "ETH_MAINNET"
        assert len(records_list) == 1
        print("✅ token_balances_by_wallet test passed")


def test_nfts_by_wallet():
    """Test the nfts_by_wallet endpoint (POST /getNftsByWallet)"""
    parent_dir = Path(__file__).parent.parent
    config = load_config(parent_dir / "configs" / "dev_config.json")
    connector = LakeflowConnect(config)
    
    captured_body = {}
    
    def mock_post(url, json=None, **kwargs):
        nonlocal captured_body
        captured_body = json or {}
        
        assert "getNftsByWallet" in url
        response = mock.MagicMock()
        response.json.return_value = [
            {
                "address": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",
                "network": "ETH_MAINNET",
                "nfts": [
                    {"contractAddress": "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D", "tokenId": "1234", "balance": "1"}
                ]
            }
        ]
        response.raise_for_status = mock.MagicMock()
        return response
    
    with mock.patch('requests.post', side_effect=mock_post):
        table_options = {"addresses": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045", "page_size": "10"}
        records, offset = connector.read_table("nfts_by_wallet", {}, table_options)
        records_list = list(records)
        
        assert "addresses" in captured_body
        assert captured_body["addresses"][0]["network"] == "ETH_MAINNET"
        assert len(records_list) == 1
        print("✅ nfts_by_wallet test passed")


def test_nft_collections_by_wallet():
    """Test the nft_collections_by_wallet endpoint (POST /getNftCollectionsByWallet)"""
    parent_dir = Path(__file__).parent.parent
    config = load_config(parent_dir / "configs" / "dev_config.json")
    connector = LakeflowConnect(config)
    
    captured_body = {}
    
    def mock_post(url, json=None, **kwargs):
        nonlocal captured_body
        captured_body = json or {}
        
        assert "getNftCollectionsByWallet" in url
        response = mock.MagicMock()
        response.json.return_value = [
            {
                "address": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",
                "network": "ETH_MAINNET",
                "collections": [
                    {"contractAddress": "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D", "name": "BAYC", "count": 2}
                ]
            }
        ]
        response.raise_for_status = mock.MagicMock()
        return response
    
    with mock.patch('requests.post', side_effect=mock_post):
        table_options = {"addresses": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"}
        records, offset = connector.read_table("nft_collections_by_wallet", {}, table_options)
        records_list = list(records)
        
        assert "addresses" in captured_body
        assert captured_body["addresses"][0]["network"] == "ETH_MAINNET"
        assert len(records_list) == 1
        print("✅ nft_collections_by_wallet test passed")


def test_wallet_transactions():
    """Test the wallet_transactions endpoint (POST /getTransactionsByWallet)"""
    parent_dir = Path(__file__).parent.parent
    config = load_config(parent_dir / "configs" / "dev_config.json")
    connector = LakeflowConnect(config)
    
    captured_body = {}
    
    def mock_post(url, json=None, **kwargs):
        nonlocal captured_body
        captured_body = json or {}
        
        assert "getTransactionsByWallet" in url
        response = mock.MagicMock()
        response.json.return_value = [
            {
                "address": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",
                "network": "ETH_MAINNET",
                "transactions": [
                    {
                        "hash": "0x123...",
                        "blockNum": "15000000",
                        "from": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",
                        "to": "0x222...",
                        "value": "1000000000000000000"
                    }
                ]
            }
        ]
        response.raise_for_status = mock.MagicMock()
        return response
    
    with mock.patch('requests.post', side_effect=mock_post):
        table_options = {"addresses": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045", "order": "desc"}
        records, offset = connector.read_table("wallet_transactions", {}, table_options)
        records_list = list(records)
        
        assert "addresses" in captured_body
        assert captured_body["addresses"][0]["network"] == "ETH_MAINNET"
        assert len(records_list) == 1
        print("✅ wallet_transactions test passed")


# =============================================================================
# WEBHOOK API ENDPOINT TESTS
# =============================================================================

def test_webhooks():
    """Test the webhooks endpoint (GET /team-webhooks)"""
    parent_dir = Path(__file__).parent.parent
    config = load_config(parent_dir / "configs" / "dev_config.json")
    config["webhook_auth_token"] = "test_token"  # Add webhook auth token
    connector = LakeflowConnect(config)
    
    def mock_get(url, params=None, **kwargs):
        assert "team-webhooks" in url
        # Check for auth header
        headers = kwargs.get("headers", {})
        assert headers.get("X-Alchemy-Token") == "test_token"
        
        response = mock.MagicMock()
        response.json.return_value = {
            "data": [
                {"id": "wh_123", "network": "ETH_MAINNET", "webhook_type": "ADDRESS_ACTIVITY", "is_active": True}
            ]
        }
        response.raise_for_status = mock.MagicMock()
        return response
    
    with mock.patch('requests.get', side_effect=mock_get):
        records, offset = connector.read_table("webhooks", {}, {})
        records_list = list(records)
        
        assert len(records_list) == 1
        assert records_list[0]["id"] == "wh_123"
        print("✅ webhooks test passed")


def test_webhook_addresses():
    """Test the webhook_addresses endpoint (GET /webhook-addresses)"""
    parent_dir = Path(__file__).parent.parent
    config = load_config(parent_dir / "configs" / "dev_config.json")
    config["webhook_auth_token"] = "test_token"
    connector = LakeflowConnect(config)
    
    captured_params = {}
    
    def mock_get(url, params=None, **kwargs):
        nonlocal captured_params
        captured_params = params or {}
        
        assert "webhook-addresses" in url
        headers = kwargs.get("headers", {})
        assert headers.get("X-Alchemy-Token") == "test_token"
        
        response = mock.MagicMock()
        response.json.return_value = {
            "addresses": ["0x111...", "0x222...", "0x333..."],
            "totalCount": 3
        }
        response.raise_for_status = mock.MagicMock()
        return response
    
    with mock.patch('requests.get', side_effect=mock_get):
        table_options = {"webhook_id": "wh_123"}
        records, offset = connector.read_table("webhook_addresses", {}, table_options)
        records_list = list(records)
        
        assert captured_params["webhook_id"] == "wh_123"
        assert len(records_list) == 3
        print("✅ webhook_addresses test passed")


# =============================================================================
# SPECIAL CASE TESTS
# =============================================================================

def test_token_prices_multiple_symbols():
    """
    Test that token_prices correctly sends multiple symbols as array-style query params.
    
    The Alchemy API expects: ?symbols=ETH&symbols=BTC&symbols=SOL
    Not: ?symbols=ETH,BTC,SOL
    """
    parent_dir = Path(__file__).parent.parent
    config = load_config(parent_dir / "configs" / "dev_config.json")
    connector = LakeflowConnect(config)
    
    captured_params = {}
    
    def mock_get(url, params=None, **kwargs):
        nonlocal captured_params
        captured_params = params or {}
        
        response = mock.MagicMock()
        response.json.return_value = {
            "data": [
                {"symbol": "ETH", "prices": [{"currency": "usd", "value": "3000.00"}]},
                {"symbol": "BTC", "prices": [{"currency": "usd", "value": "60000.00"}]},
                {"symbol": "SOL", "prices": [{"currency": "usd", "value": "150.00"}]}
            ]
        }
        response.raise_for_status = mock.MagicMock()
        return response
    
    with mock.patch('requests.get', side_effect=mock_get):
        table_options = {"symbols": "ETH,BTC,SOL"}
        records, offset = connector.read_table("token_prices", {}, table_options)
        records_list = list(records)
        
        assert isinstance(captured_params["symbols"], list)
        assert captured_params["symbols"] == ["ETH", "BTC", "SOL"]
        assert len(records_list) == 3
        print("✅ Multiple symbols test passed!")


def test_token_prices_single_symbol():
    """Test that token_prices also works correctly with a single symbol."""
    parent_dir = Path(__file__).parent.parent
    config = load_config(parent_dir / "configs" / "dev_config.json")
    connector = LakeflowConnect(config)
    
    captured_params = {}
    
    def mock_get(url, params=None, **kwargs):
        nonlocal captured_params
        captured_params = params or {}
        
        response = mock.MagicMock()
        response.json.return_value = {
            "data": [{"symbol": "ETH", "prices": [{"currency": "usd", "value": "3000.00"}]}]
        }
        response.raise_for_status = mock.MagicMock()
        return response
    
    with mock.patch('requests.get', side_effect=mock_get):
        table_options = {"symbols": "ETH"}
        records, offset = connector.read_table("token_prices", {}, table_options)
        records_list = list(records)
        
        assert isinstance(captured_params["symbols"], list)
        assert captured_params["symbols"] == ["ETH"]
        assert len(records_list) == 1
        print("✅ Single symbol test passed!")


def test_portfolio_with_network_override():
    """Test that portfolio endpoints correctly handle network override with @ syntax."""
    parent_dir = Path(__file__).parent.parent
    config = load_config(parent_dir / "configs" / "dev_config.json")
    connector = LakeflowConnect(config)
    
    captured_body = {}
    
    def mock_post(url, json=None, **kwargs):
        nonlocal captured_body
        captured_body = json or {}
        
        response = mock.MagicMock()
        response.json.return_value = [
            {
                "address": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",
                "network": "POLYGON_MAINNET",
                "tokenBalances": []
            }
        ]
        response.raise_for_status = mock.MagicMock()
        return response
    
    with mock.patch('requests.post', side_effect=mock_post):
        # Test with explicit network override using @ syntax
        table_options = {"addresses": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045@polygon-mainnet"}
        records, offset = connector.read_table("tokens_by_wallet", {}, table_options)
        list(records)
        
        # Should use the overridden network, not the default
        assert captured_body["addresses"][0]["network"] == "POLYGON_MAINNET"
        print("✅ Network override test passed!")


def test_nft_metadata_string_normalization():
    """Test that nft_metadata correctly normalizes string fields to objects."""
    parent_dir = Path(__file__).parent.parent
    config = load_config(parent_dir / "configs" / "dev_config.json")
    connector = LakeflowConnect(config)
    
    def mock_get(url, params=None, **kwargs):
        response = mock.MagicMock()
        # Simulate API returning string values instead of objects
        response.json.return_value = {
            "contract": "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D",  # String instead of object
            "tokenId": "1",
            "tokenType": "ERC721",
            "rawMetadata": '{"name": "Test"}',  # JSON string instead of object
            "tokenUri": "ipfs://test",  # String instead of object
            "media": "[]"  # JSON string instead of array
        }
        response.raise_for_status = mock.MagicMock()
        return response
    
    with mock.patch('requests.get', side_effect=mock_get):
        table_options = {"contract_address": "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D", "token_id": "1"}
        records, offset = connector.read_table("nft_metadata", {}, table_options)
        records_list = list(records)
        
        record = records_list[0]
        # Check that string fields were normalized to proper objects
        assert isinstance(record["contract"], dict)
        assert record["contract"]["address"] == "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D"
        assert isinstance(record["tokenUri"], dict)
        assert record["tokenUri"]["raw"] == "ipfs://test"
        assert isinstance(record["rawMetadata"], dict)
        print("✅ NFT metadata string normalization test passed!")
