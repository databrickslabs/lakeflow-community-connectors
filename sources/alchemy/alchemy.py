import requests
import json
from typing import Dict, List, Iterator, Any, Optional
from datetime import datetime, timezone
from urllib.parse import urlencode
from pyspark.sql.types import *

# Supported blockchain networks
SUPPORTED_NETWORKS = {
    # Ethereum & L2s
    'eth-mainnet', 'eth-sepolia', 'eth-holesky',
    'arb-mainnet', 'arb-sepolia', 'opt-mainnet', 'opt-sepolia',
    'base-mainnet', 'base-sepolia',
    # Polygon
    'polygon-mainnet', 'polygon-amoy', 'polygonzkevm-mainnet',
    # Other EVM
    'bnb-mainnet', 'avax-mainnet', 'linea-mainnet',
    'zksync-mainnet', 'scroll-mainnet', 'blast-mainnet',
    # Solana
    'solana-mainnet', 'solana-devnet'
}

class LakeflowConnect:
    def __init__(self, options: Dict[str, str]) -> None:
        """
        Initialize the Alchemy connector with API credentials and configuration.
        """
        self.api_key = options.get("api_key")
        if not self.api_key:
            raise ValueError("API key is required")

        self.default_network = options.get("network", "eth-mainnet")
        if self.default_network not in SUPPORTED_NETWORKS:
            raise ValueError(f"Unsupported network: {self.default_network}")

        # Base URLs for different APIs
        self.nft_base_url = "https://{network}.g.alchemy.com/nft/v3/{api_key}"
        self.prices_base_url = "https://api.g.alchemy.com/prices/v1/{api_key}"
        self.portfolio_base_url = "https://api.g.alchemy.com/data/v1/{api_key}"
        self.webhooks_base_url = "https://dashboard.alchemy.com/api"

        # Auth token for webhooks (optional, only needed for webhook endpoints)
        self.webhook_auth_token = options.get("webhook_auth_token")

        # Table configurations
        self._table_configs = {
            # NFT Tables
            "nfts_by_owner": {
                "endpoint": "/getNFTsForOwner",
                "api_type": "nft",
                "primary_keys": ["owner", "contract", "tokenId"],
                "cursor_field": "updatedAt",
                "ingestion_type": "append",
                "supports_pagination": True,
                "required_params": ["owner_address"],
                "optional_params": ["contract_addresses", "with_metadata", "page_size"]
            },
            "nfts_for_contract": {
                "endpoint": "/getNFTsForContract",
                "api_type": "nft",
                "primary_keys": ["contract", "tokenId"],
                "cursor_field": None,
                "ingestion_type": "snapshot",
                "supports_pagination": False,
                "required_params": ["contract_address"],
                "optional_params": ["with_metadata", "start_token", "limit"]
            },
            "nft_metadata": {
                "endpoint": "/getNFTMetadata",
                "api_type": "nft",
                "primary_keys": ["contract", "tokenId"],
                "cursor_field": None,
                "ingestion_type": "snapshot",
                "supports_pagination": False,
                "required_params": ["contract_address", "token_id"],
                "optional_params": ["token_type", "refresh_cache"]
            },
            "contract_metadata": {
                "endpoint": "/getContractMetadata",
                "api_type": "nft",
                "primary_keys": ["contractAddress"],
                "cursor_field": None,
                "ingestion_type": "snapshot",
                "supports_pagination": False,
                "required_params": ["contract_address"],
                "optional_params": []
            },
            "nft_metadata_batch": {
                "endpoint": "/getNFTMetadataBatch",
                "api_type": "nft",
                "method": "POST",
                "primary_keys": ["contract", "tokenId"],
                "cursor_field": None,
                "ingestion_type": "snapshot",
                "supports_pagination": False,
                "required_params": ["tokens"],
                "optional_params": ["token_uri_timeout_in_ms", "refresh_cache"]
            },
            "contract_metadata_batch": {
                "endpoint": "/getContractMetadataBatch",
                "api_type": "nft",
                "method": "POST",
                "primary_keys": ["contractAddress"],
                "cursor_field": None,
                "ingestion_type": "snapshot",
                "supports_pagination": False,
                "required_params": ["contract_addresses"],
                "optional_params": []
            },
            "nft_sales": {
                "endpoint": "/getNFTSales",
                "api_type": "nft",
                "primary_keys": ["contractAddress", "tokenId", "transactionHash", "logIndex"],
                "cursor_field": "blockNumber",
                "ingestion_type": "cdc",
                "supports_pagination": True,
                "required_params": [],
                "optional_params": ["contract_address", "token_id", "from_block", "to_block", "order", "page_key"]
            },
            "floor_prices": {
                "endpoint": "/getFloorPrice",
                "api_type": "nft",
                "primary_keys": ["contractAddress"],
                "cursor_field": None,
                "ingestion_type": "snapshot",
                "supports_pagination": False,
                "required_params": ["contract_address"],
                "optional_params": []
            },
            # Token/Price Tables
            "token_prices": {
                "endpoint": "/tokens/by-symbol",
                "api_type": "prices",
                "primary_keys": ["symbol"],
                "cursor_field": None,
                "ingestion_type": "snapshot",
                "supports_pagination": False,
                "required_params": ["symbols"],
                "optional_params": []
            },
            "token_prices_by_address": {
                "endpoint": "/tokens/by-address",
                "api_type": "prices",
                "method": "POST",
                "primary_keys": ["network", "address"],
                "cursor_field": None,
                "ingestion_type": "snapshot",
                "supports_pagination": False,
                "required_params": ["addresses"],
                "optional_params": []
            },
            "token_prices_historical": {
                "endpoint": "/tokens/historical",
                "api_type": "prices",
                "method": "POST",
                "primary_keys": ["symbol", "timestamp"],
                "cursor_field": "timestamp",
                "ingestion_type": "cdc",
                "supports_pagination": False,
                "required_params": ["symbol", "start_time", "end_time"],
                "optional_params": ["interval"]
            },
            # Portfolio Tables
            "tokens_by_wallet": {
                "endpoint": "/assets/tokens/by-address",
                "api_type": "portfolio",
                "method": "POST",
                "primary_keys": ["wallet_address", "network", "contractAddress"],
                "cursor_field": None,
                "ingestion_type": "snapshot",
                "supports_pagination": False,
                "required_params": ["addresses"],
                "optional_params": ["with_metadata", "with_prices", "include_native_tokens"]
            },
            "token_balances_by_wallet": {
                "endpoint": "/assets/tokens/balances/by-address",
                "api_type": "portfolio",
                "method": "POST",
                "primary_keys": ["wallet_address", "network", "contractAddress"],
                "cursor_field": None,
                "ingestion_type": "snapshot",
                "supports_pagination": False,
                "required_params": ["addresses"],
                "optional_params": []
            },
            "nfts_by_wallet": {
                "endpoint": "/assets/nfts/by-address",
                "api_type": "portfolio",
                "method": "POST",
                "primary_keys": ["wallet_address", "network", "contractAddress", "tokenId"],
                "cursor_field": None,
                "ingestion_type": "snapshot",
                "supports_pagination": True,
                "required_params": ["addresses"],
                "optional_params": ["with_metadata", "page_size"]
            },
            "nft_collections_by_wallet": {
                "endpoint": "/getNftCollectionsByWallet",
                "api_type": "portfolio",
                "method": "POST",
                "primary_keys": ["wallet_address", "network", "contractAddress"],
                "cursor_field": None,
                "ingestion_type": "snapshot",
                "supports_pagination": False,
                "required_params": ["addresses"],
                "optional_params": ["with_metadata"]
            },
            "wallet_transactions": {
                "endpoint": "/transactions/history/by-address",
                "api_type": "portfolio",
                "method": "POST",
                "primary_keys": ["hash", "network"],
                "cursor_field": "blockNum",
                "ingestion_type": "cdc",
                "supports_pagination": True,
                "required_params": ["addresses"],
                "optional_params": ["page_size", "page_key"]
            },
            # Webhook Tables
            "webhooks": {
                "endpoint": "/team-webhooks",
                "api_type": "webhooks",
                "primary_keys": ["id"],
                "cursor_field": None,
                "ingestion_type": "snapshot",
                "supports_pagination": False,
                "required_params": [],
                "optional_params": []
            },
            "webhook_addresses": {
                "endpoint": "/webhook-addresses",
                "api_type": "webhooks",
                "primary_keys": ["webhook_id", "address"],
                "cursor_field": None,
                "ingestion_type": "snapshot",
                "supports_pagination": False,
                "required_params": ["webhook_id"],
                "optional_params": ["limit", "after"]
            }
        }

    def list_tables(self) -> List[str]:
        """
        List all supported tables.
        """
        return list(self._table_configs.keys())

    def get_table_schema(self, table_name: str, table_options: Dict[str, str]) -> StructType:
        """
        Get the schema for a table.
        """
        if table_name not in self._table_configs:
            raise ValueError(f"Unknown table: {table_name}")

        config = self._table_configs[table_name]

        if table_name == "nfts_by_owner":
            return StructType([
                StructField("owner", StringType(), False),
                StructField("contract", StructType([
                    StructField("address", StringType(), False),
                    StructField("name", StringType(), True),
                    StructField("symbol", StringType(), True),
                    StructField("totalSupply", StringType(), True),
                    StructField("tokenType", StringType(), True),
                    StructField("contractDeployer", StringType(), True),
                    StructField("deployedBlockNumber", LongType(), True),
                    StructField("openSea", StructType([
                        StructField("floorPrice", DoubleType(), True),
                        StructField("collectionName", StringType(), True),
                        StructField("safelistRequestStatus", StringType(), True),
                        StructField("imageUrl", StringType(), True),
                        StructField("description", StringType(), True),
                        StructField("externalUrl", StringType(), True),
                        StructField("twitterUsername", StringType(), True),
                        StructField("discordUrl", StringType(), True),
                        StructField("lastIngestedAt", StringType(), True),
                    ]), True),
                ]), False),
                StructField("tokenId", StringType(), False),
                StructField("tokenType", StringType(), True),
                StructField("title", StringType(), True),
                StructField("description", StringType(), True),
                StructField("timeLastUpdated", StringType(), True),
                StructField("metadataError", StringType(), True),
                StructField("rawMetadata", StructType([
                    StructField("name", StringType(), True),
                    StructField("description", StringType(), True),
                    StructField("image", StringType(), True),
                    StructField("external_url", StringType(), True),
                    StructField("attributes", ArrayType(StructType([
                        StructField("value", StringType(), True),
                        StructField("trait_type", StringType(), True),
                        StructField("display_type", StringType(), True),
                    ])), True),
                ]), True),
                StructField("tokenUri", StructType([
                    StructField("raw", StringType(), True),
                    StructField("gateway", StringType(), True),
                ]), True),
                StructField("media", ArrayType(StructType([
                    StructField("raw", StringType(), True),
                    StructField("gateway", StringType(), True),
                    StructField("thumbnail", StringType(), True),
                    StructField("format", StringType(), True),
                    StructField("bytes", LongType(), True),
                ])), True),
                StructField("balance", StringType(), True),
                StructField("acquiredAt", StructType([
                    StructField("blockTimestamp", StringType(), True),
                    StructField("blockNumber", LongType(), True),
                ]), True),
            ])

        elif table_name == "nfts_for_contract":
            return StructType([
                StructField("contract", StructType([
                    StructField("address", StringType(), False),
                    StructField("name", StringType(), True),
                    StructField("symbol", StringType(), True),
                    StructField("totalSupply", StringType(), True),
                    StructField("tokenType", StringType(), True),
                ]), False),
                StructField("tokenId", StringType(), False),
                StructField("tokenType", StringType(), True),
                StructField("title", StringType(), True),
                StructField("description", StringType(), True),
                StructField("timeLastUpdated", StringType(), True),
                StructField("metadataError", StringType(), True),
                StructField("rawMetadata", StructType([
                    StructField("name", StringType(), True),
                    StructField("description", StringType(), True),
                    StructField("image", StringType(), True),
                    StructField("external_url", StringType(), True),
                    StructField("attributes", ArrayType(StructType([
                        StructField("value", StringType(), True),
                        StructField("trait_type", StringType(), True),
                    ])), True),
                ]), True),
                StructField("tokenUri", StructType([
                    StructField("raw", StringType(), True),
                    StructField("gateway", StringType(), True),
                ]), True),
                StructField("media", ArrayType(StructType([
                    StructField("raw", StringType(), True),
                    StructField("gateway", StringType(), True),
                    StructField("thumbnail", StringType(), True),
                    StructField("format", StringType(), True),
                    StructField("bytes", LongType(), True),
                ])), True),
            ])

        elif table_name == "nft_metadata":
            return StructType([
                StructField("contract", StructType([
                    StructField("address", StringType(), False),
                    StructField("name", StringType(), True),
                    StructField("symbol", StringType(), True),
                    StructField("totalSupply", StringType(), True),
                    StructField("tokenType", StringType(), True),
                ]), False),
                StructField("tokenId", StringType(), False),
                StructField("tokenType", StringType(), True),
                StructField("title", StringType(), True),
                StructField("description", StringType(), True),
                StructField("timeLastUpdated", StringType(), True),
                StructField("metadataError", StringType(), True),
                StructField("rawMetadata", StructType([
                    StructField("name", StringType(), True),
                    StructField("description", StringType(), True),
                    StructField("image", StringType(), True),
                    StructField("external_url", StringType(), True),
                    StructField("attributes", ArrayType(StructType([
                        StructField("value", StringType(), True),
                        StructField("trait_type", StringType(), True),
                        StructField("display_type", StringType(), True),
                    ])), True),
                ]), True),
                StructField("tokenUri", StructType([
                    StructField("raw", StringType(), True),
                    StructField("gateway", StringType(), True),
                ]), True),
                StructField("media", ArrayType(StructType([
                    StructField("raw", StringType(), True),
                    StructField("gateway", StringType(), True),
                    StructField("thumbnail", StringType(), True),
                    StructField("format", StringType(), True),
                    StructField("bytes", LongType(), True),
                ])), True),
            ])

        elif table_name == "contract_metadata":
            return StructType([
                StructField("contractAddress", StringType(), False),
                StructField("name", StringType(), True),
                StructField("symbol", StringType(), True),
                StructField("totalSupply", StringType(), True),
                StructField("tokenType", StringType(), True),
                StructField("contractDeployer", StringType(), True),
                StructField("deployedBlockNumber", LongType(), True),
                StructField("openSea", StructType([
                    StructField("floorPrice", DoubleType(), True),
                    StructField("collectionName", StringType(), True),
                    StructField("safelistRequestStatus", StringType(), True),
                    StructField("imageUrl", StringType(), True),
                    StructField("description", StringType(), True),
                    StructField("externalUrl", StringType(), True),
                    StructField("twitterUsername", StringType(), True),
                    StructField("discordUrl", StringType(), True),
                    StructField("lastIngestedAt", StringType(), True),
                ]), True),
            ])

        elif table_name == "nft_sales":
            return StructType([
                StructField("contractAddress", StringType(), True),
                StructField("tokenId", StringType(), True),
                StructField("quantity", StringType(), True),
                StructField("transactionHash", StringType(), False),
                StructField("blockNumber", LongType(), False),
                StructField("logIndex", LongType(), False),
                StructField("bundleIndex", LongType(), True),
                StructField("transactionIndex", LongType(), True),
                StructField("buyerAddress", StringType(), True),
                StructField("sellerAddress", StringType(), True),
                StructField("marketplace", StringType(), True),
                StructField("price", StructType([
                    StructField("amount", StringType(), True),
                    StructField("currency", StructType([
                        StructField("contractAddress", StringType(), True),
                        StructField("symbol", StringType(), True),
                        StructField("decimals", LongType(), True),
                    ]), True),
                    StructField("marketplaceFee", StringType(), True),
                    StructField("creatorFee", StringType(), True),
                    StructField("royaltyFee", StringType(), True),
                ]), True),
            ])

        elif table_name == "floor_prices":
            return StructType([
                StructField("contractAddress", StringType(), False),
                StructField("openSea", StructType([
                    StructField("floorPrice", DoubleType(), True),
                    StructField("priceCurrency", StringType(), True),
                    StructField("retrievedAt", StringType(), True),
                ]), True),
                StructField("looksRare", StructType([
                    StructField("floorPrice", DoubleType(), True),
                    StructField("priceCurrency", StringType(), True),
                    StructField("retrievedAt", StringType(), True),
                ]), True),
            ])

        elif table_name == "token_prices":
            return StructType([
                StructField("symbol", StringType(), False),
                StructField("prices", ArrayType(StructType([
                    StructField("currency", StringType(), False),
                    StructField("value", StringType(), False),
                ])), False),
            ])

        elif table_name == "token_prices_historical":
            return StructType([
                StructField("symbol", StringType(), False),
                StructField("timestamp", StringType(), False),  # API returns ISO timestamp string
                StructField("value", DoubleType(), True),
                StructField("marketCap", DoubleType(), True),
                StructField("volume24h", DoubleType(), True),
            ])

        elif table_name == "tokens_by_wallet":
            return StructType([
                StructField("wallet_address", StringType(), False),
                StructField("network", StringType(), False),
                StructField("contractAddress", StringType(), True),
                StructField("tokenBalance", StringType(), True),
                StructField("decimals", LongType(), True),
                StructField("name", StringType(), True),
                StructField("symbol", StringType(), True),
                StructField("logo", StringType(), True),
                StructField("price", DoubleType(), True),
            ])

        elif table_name == "token_balances_by_wallet":
            return StructType([
                StructField("wallet_address", StringType(), False),
                StructField("network", StringType(), False),
                StructField("contractAddress", StringType(), True),
                StructField("tokenBalance", StringType(), True),
                StructField("error", StringType(), True),
            ])

        elif table_name == "nfts_by_wallet":
            return StructType([
                StructField("wallet_address", StringType(), False),
                StructField("network", StringType(), False),
                StructField("contractAddress", StringType(), False),
                StructField("tokenId", StringType(), False),
                StructField("balance", StringType(), True),
            ])

        elif table_name == "wallet_transactions":
            return StructType([
                StructField("hash", StringType(), False),
                StructField("network", StringType(), False),
                StructField("blockNum", StringType(), False),
                StructField("from", StringType(), True),
                StructField("to", StringType(), True),
                StructField("value", StringType(), True),
                StructField("gasPrice", StringType(), True),
                StructField("gasUsed", StringType(), True),
                StructField("timestamp", LongType(), True),
                StructField("category", StringType(), True),
                StructField("asset", StringType(), True),
                StructField("contractAddress", StringType(), True),
                StructField("erc721TokenId", StringType(), True),
                StructField("erc1155Metadata", ArrayType(StructType([
                    StructField("tokenId", StringType(), True),
                    StructField("value", StringType(), True),
                ])), True),
                StructField("logEvents", ArrayType(StructType([
                    StructField("contractAddress", StringType(), True),
                    StructField("topics", ArrayType(StringType()), True),
                    StructField("data", StringType(), True),
                ])), True),
            ])

        elif table_name == "webhooks":
            return StructType([
                StructField("id", StringType(), False),
                StructField("network", StringType(), True),
                StructField("webhook_type", StringType(), True),
                StructField("webhook_url", StringType(), True),
                StructField("is_active", BooleanType(), True),
                StructField("time_created", LongType(), True),
                StructField("addresses", ArrayType(StringType()), True),
            ])

        elif table_name == "webhook_addresses":
            return StructType([
                StructField("webhook_id", StringType(), False),
                StructField("address", StringType(), False),
            ])

        elif table_name == "nft_metadata_batch":
            # Same schema as individual NFT metadata
            return StructType([
                StructField("contract", StructType([
                    StructField("address", StringType(), False),
                    StructField("name", StringType(), True),
                    StructField("symbol", StringType(), True),
                    StructField("totalSupply", StringType(), True),
                    StructField("tokenType", StringType(), True),
                ]), False),
                StructField("tokenId", StringType(), False),
                StructField("tokenType", StringType(), True),
                StructField("title", StringType(), True),
                StructField("description", StringType(), True),
                StructField("timeLastUpdated", StringType(), True),
                StructField("metadataError", StringType(), True),
                StructField("rawMetadata", StructType([
                    StructField("name", StringType(), True),
                    StructField("description", StringType(), True),
                    StructField("image", StringType(), True),
                    StructField("external_url", StringType(), True),
                    StructField("attributes", ArrayType(StructType([
                        StructField("value", StringType(), True),
                        StructField("trait_type", StringType(), True),
                        StructField("display_type", StringType(), True),
                    ])), True),
                ]), True),
                StructField("tokenUri", StructType([
                    StructField("raw", StringType(), True),
                    StructField("gateway", StringType(), True),
                ]), True),
                StructField("media", ArrayType(StructType([
                    StructField("raw", StringType(), True),
                    StructField("gateway", StringType(), True),
                    StructField("thumbnail", StringType(), True),
                    StructField("format", StringType(), True),
                    StructField("bytes", LongType(), True),
                ])), True),
            ])

        elif table_name == "contract_metadata_batch":
            # Same schema as individual contract metadata
            return StructType([
                StructField("contractAddress", StringType(), False),
                StructField("name", StringType(), True),
                StructField("symbol", StringType(), True),
                StructField("totalSupply", StringType(), True),
                StructField("tokenType", StringType(), True),
                StructField("contractDeployer", StringType(), True),
                StructField("deployedBlockNumber", LongType(), True),
                StructField("openSea", StructType([
                    StructField("floorPrice", DoubleType(), True),
                    StructField("collectionName", StringType(), True),
                    StructField("safelistRequestStatus", StringType(), True),
                    StructField("imageUrl", StringType(), True),
                    StructField("description", StringType(), True),
                    StructField("externalUrl", StringType(), True),
                    StructField("twitterUsername", StringType(), True),
                    StructField("discordUrl", StringType(), True),
                    StructField("lastIngestedAt", StringType(), True),
                ]), True),
            ])

        elif table_name == "token_prices_by_address":
            # Similar to token_prices but with network/address instead of symbol
            return StructType([
                StructField("network", StringType(), False),
                StructField("address", StringType(), False),
                StructField("prices", ArrayType(StructType([
                    StructField("currency", StringType(), False),
                    StructField("value", StringType(), False),
                ])), False),
            ])

        elif table_name == "nft_collections_by_wallet":
            return StructType([
                StructField("wallet_address", StringType(), False),
                StructField("network", StringType(), False),
                StructField("contractAddress", StringType(), False),
                StructField("name", StringType(), True),
                StructField("symbol", StringType(), True),
                StructField("tokenType", StringType(), True),
                StructField("openSea", StructType([
                    StructField("floorPrice", DoubleType(), True),
                    StructField("collectionName", StringType(), True),
                    StructField("safelistRequestStatus", StringType(), True),
                    StructField("imageUrl", StringType(), True),
                    StructField("description", StringType(), True),
                    StructField("externalUrl", StringType(), True),
                ]), True),
                StructField("totalBalance", StringType(), True),
                StructField("distinctNftsOwned", LongType(), True),
                StructField("distinctTokensOwned", LongType(), True),
            ])

        else:
            raise ValueError(f"Schema not implemented for table: {table_name}")

    def read_table_metadata(self, table_name: str, table_options: Dict[str, str]) -> Dict:
        """
        Get metadata for a table.
        """
        if table_name not in self._table_configs:
            raise ValueError(f"Unknown table: {table_name}")

        config = self._table_configs[table_name]
        return {
            "primary_keys": config["primary_keys"],
            "cursor_field": config["cursor_field"],
            "ingestion_type": config["ingestion_type"],
        }

    def read_table(
        self, table_name: str, start_offset: Dict, table_options: Dict[str, str]
    ) -> (Iterator[Dict], Dict):
        """
        Read data from a table.
        """
        if table_name not in self._table_configs:
            raise ValueError(f"Unknown table: {table_name}")

        config = self._table_configs[table_name]

        # Validate required parameters
        for param in config["required_params"]:
            if param not in table_options and param not in start_offset:
                raise ValueError(f"Required parameter '{param}' not provided for table '{table_name}'")

        # Build API URL and request data
        url, request_data = self._build_api_request(table_name, table_options, start_offset)

        # Make API request (GET or POST)
        method = config.get("method", "GET")
        headers = {"Content-Type": "application/json"}
        
        # Portfolio and Prices APIs require Bearer token authentication
        if config["api_type"] in ["portfolio", "prices"]:
            headers["Authorization"] = f"Bearer {self.api_key}"
        
        # Webhook APIs require X-Alchemy-Token header
        if config["api_type"] == "webhooks" and self.webhook_auth_token:
            headers["X-Alchemy-Token"] = self.webhook_auth_token
        
        if method == "POST":
            response = requests.post(url, json=request_data, headers=headers)
        else:
            response = requests.get(url, params=request_data, headers=headers)

        response.raise_for_status()
        data = response.json()

        # Process response based on table type
        records = self._process_api_response(table_name, data, table_options)

        # Calculate next offset
        next_offset = self._calculate_next_offset(table_name, data, start_offset)

        return iter(records), next_offset

    def _build_api_request(self, table_name: str, table_options: Dict[str, str], start_offset: Dict) -> (str, Dict):
        """
        Build API request URL and request data (query params for GET, JSON body for POST).
        """
        config = self._table_configs[table_name]
        network = table_options.get("network", self.default_network)
        method = config.get("method", "GET")

        if config["api_type"] == "nft":
            base_url = self.nft_base_url.format(network=network, api_key=self.api_key)
            url = f"{base_url}{config['endpoint']}"
        elif config["api_type"] == "prices":
            base_url = self.prices_base_url.format(api_key=self.api_key)
            url = f"{base_url}{config['endpoint']}"
        elif config["api_type"] == "portfolio":
            base_url = self.portfolio_base_url.format(api_key=self.api_key)
            url = f"{base_url}{config['endpoint']}"
        elif config["api_type"] == "webhooks":
            url = f"{self.webhooks_base_url}{config['endpoint']}"
        else:
            raise ValueError(f"Unknown API type: {config['api_type']}")

        # For GET requests, build query parameters
        if method == "GET":
            params = {}
            self._build_get_params(table_name, table_options, start_offset, params)
            return url, params

        # For POST requests, build JSON body
        else:
            body = {}
            self._build_post_body(table_name, table_options, start_offset, body)
            return url, body

    def _build_get_params(self, table_name: str, table_options: Dict[str, str], start_offset: Dict, params: Dict):
        """Build query parameters for GET requests."""
        if table_name == "nfts_by_owner":
            params["owner"] = table_options["owner_address"]
            if "contract_addresses" in table_options:
                params["contractAddresses"] = table_options["contract_addresses"].split(",")
            params["withMetadata"] = table_options.get("with_metadata", "true").lower() == "true"
            params["pageSize"] = int(table_options.get("page_size", "100"))
            if start_offset and "pageKey" in start_offset:
                params["pageKey"] = start_offset["pageKey"]

        elif table_name == "nfts_for_contract":
            params["contractAddress"] = table_options["contract_address"]
            params["withMetadata"] = table_options.get("with_metadata", "true").lower() == "true"
            if "start_token" in table_options:
                params["startToken"] = table_options["start_token"]
            if "limit" in table_options:
                params["limit"] = int(table_options["limit"])

        elif table_name == "nft_metadata":
            params["contractAddress"] = table_options["contract_address"]
            params["tokenId"] = table_options["token_id"]
            if "token_type" in table_options:
                params["tokenType"] = table_options["token_type"]
            if "refresh_cache" in table_options:
                params["refreshCache"] = table_options["refresh_cache"].lower() == "true"

        elif table_name == "contract_metadata":
            params["contractAddress"] = table_options["contract_address"]

        elif table_name == "nft_sales":
            if "contract_address" in table_options:
                params["contractAddress"] = table_options["contract_address"]
            if "token_id" in table_options:
                params["tokenId"] = table_options["token_id"]
            if "from_block" in table_options:
                params["fromBlock"] = table_options["from_block"]
            if "to_block" in table_options:
                params["toBlock"] = table_options["to_block"]
            params["order"] = table_options.get("order", "desc")
            if start_offset and "pageKey" in start_offset:
                params["pageKey"] = start_offset["pageKey"]

        elif table_name == "floor_prices":
            params["contractAddress"] = table_options["contract_address"]

        elif table_name == "token_prices":
            # Alchemy expects symbols as array-style query params: ?symbols=ETH&symbols=BTC
            symbols_str = table_options["symbols"]
            params["symbols"] = [s.strip() for s in symbols_str.split(",")]

        elif table_name == "webhook_addresses":
            params["webhook_id"] = table_options["webhook_id"]
            if "limit" in table_options:
                params["limit"] = int(table_options["limit"])
            if "after" in table_options:
                params["after"] = table_options["after"]

    def _build_post_body(self, table_name: str, table_options: Dict[str, str], start_offset: Dict, body: Dict):
        """Build JSON body for POST requests."""
        if table_name == "nft_metadata_batch":
            body["tokens"] = []
            # Parse tokens from table_options["tokens"] - expected format: "contract1:token1,contract2:token2"
            tokens_str = table_options["tokens"]
            for token_spec in tokens_str.split(","):
                if ":" in token_spec:
                    contract, token_id = token_spec.split(":", 1)
                    body["tokens"].append({
                        "contractAddress": contract.strip(),
                        "tokenId": token_id.strip()
                    })

            if "token_uri_timeout_in_ms" in table_options:
                body["tokenUriTimeoutInMs"] = int(table_options["token_uri_timeout_in_ms"])
            if "refresh_cache" in table_options:
                body["refreshCache"] = table_options["refresh_cache"].lower() == "true"

        elif table_name == "contract_metadata_batch":
            body["contractAddresses"] = table_options["contract_addresses"].split(",")

        elif table_name == "token_prices_by_address":
            body["addresses"] = []
            # Parse addresses from table_options["addresses"] - expected format: "network1:address1,network2:address2"
            addresses_str = table_options["addresses"]
            for addr_spec in addresses_str.split(","):
                if ":" in addr_spec:
                    network, address = addr_spec.split(":", 1)
                    body["addresses"].append({
                        "network": network.strip(),
                        "address": address.strip()
                    })

        elif table_name == "token_prices_historical":
            # symbol is required per Alchemy API docs
            body["symbol"] = table_options["symbol"]
            body["startTime"] = table_options["start_time"]
            body["endTime"] = table_options["end_time"]
            if "interval" in table_options:
                body["interval"] = table_options["interval"]

        elif table_name == "tokens_by_wallet":
            # This endpoint uses networks (plural, array) per address
            body["addresses"] = []
            addresses_str = table_options["addresses"]
            default_network = table_options.get("network", self.default_network)
            for addr_spec in addresses_str.split(","):
                addr_spec = addr_spec.strip()
                if "@" in addr_spec:
                    address, networks_str = addr_spec.split("@", 1)
                    # Support multiple networks with | separator
                    networks = [n.strip() for n in networks_str.split("|")]
                else:
                    address = addr_spec
                    networks = [default_network]
                body["addresses"].append({
                    "address": address.strip(),
                    "networks": networks  # Note: plural, array format
                })

            body["withMetadata"] = table_options.get("with_metadata", "true").lower() == "true"
            body["withPrices"] = table_options.get("with_prices", "true").lower() == "true"
            body["includeNativeTokens"] = table_options.get("include_native_tokens", "true").lower() == "true"
            if "include_erc20_tokens" in table_options:
                body["includeErc20Tokens"] = table_options["include_erc20_tokens"].lower() == "true"

        elif table_name == "token_balances_by_wallet":
            # This endpoint uses networks (plural, array) per address
            body["addresses"] = []
            addresses_str = table_options["addresses"]
            default_network = table_options.get("network", self.default_network)
            for addr_spec in addresses_str.split(","):
                addr_spec = addr_spec.strip()
                if "@" in addr_spec:
                    address, networks_str = addr_spec.split("@", 1)
                    # Support multiple networks with | separator
                    networks = [n.strip() for n in networks_str.split("|")]
                else:
                    address = addr_spec
                    networks = [default_network]
                body["addresses"].append({
                    "address": address.strip(),
                    "networks": networks  # Note: plural, array format
                })

        elif table_name == "nfts_by_wallet":
            # This endpoint uses networks (plural, array) per address
            body["addresses"] = []
            addresses_str = table_options["addresses"]
            default_network = table_options.get("network", self.default_network)
            for addr_spec in addresses_str.split(","):
                addr_spec = addr_spec.strip()
                if "@" in addr_spec:
                    address, networks_str = addr_spec.split("@", 1)
                    # Support multiple networks with | separator
                    networks = [n.strip() for n in networks_str.split("|")]
                else:
                    address = addr_spec
                    networks = [default_network]
                body["addresses"].append({
                    "address": address.strip(),
                    "networks": networks  # Note: plural, array format
                })

            body["withMetadata"] = table_options.get("with_metadata", "true").lower() == "true"
            if "page_size" in table_options:
                body["pageSize"] = int(table_options["page_size"])

        elif table_name == "nft_collections_by_wallet":
            body["addresses"] = []
            addresses_str = table_options["addresses"]
            default_network = self._convert_network_format(table_options.get("network", self.default_network))
            for addr_spec in addresses_str.split(","):
                addr_spec = addr_spec.strip()
                if "@" in addr_spec:
                    address, network_str = addr_spec.split("@", 1)
                    network = self._convert_network_format(network_str.strip())
                else:
                    address = addr_spec
                    network = default_network
                body["addresses"].append({
                    "address": address.strip(),
                    "network": network
                })

            body["withMetadata"] = table_options.get("with_metadata", "true").lower() == "true"

        elif table_name == "wallet_transactions":
            # This endpoint uses networks (plural, array) per address
            body["addresses"] = []
            addresses_str = table_options["addresses"]
            default_network = table_options.get("network", self.default_network)
            for addr_spec in addresses_str.split(","):
                addr_spec = addr_spec.strip()
                if "@" in addr_spec:
                    address, networks_str = addr_spec.split("@", 1)
                    # Support multiple networks with | separator: address@eth-mainnet|base-mainnet
                    networks = [n.strip() for n in networks_str.split("|")]
                else:
                    address = addr_spec
                    networks = [default_network]
                body["addresses"].append({
                    "address": address.strip(),
                    "networks": networks  # Note: plural, array format
                })

            if "page_size" in table_options:
                body["pageSize"] = int(table_options["page_size"])
            if "page_key" in table_options:
                body["pageKey"] = table_options["page_key"]

    def _process_api_response(self, table_name: str, data: Dict, table_options: Dict[str, str]) -> List[Dict]:
        """
        Process API response into records.
        """
        if table_name == "nfts_by_owner":
            records = data.get("ownedNfts", []) if "ownedNfts" in data else data.get("nfts", [])
            # API response doesn't include owner, add it from table_options
            owner = table_options.get("owner_address")
            normalized = [self._normalize_nft_record(r) for r in records]
            return [{**record, "owner": owner} for record in normalized]

        elif table_name == "nfts_for_contract":
            records = data.get("ownedNfts", []) if "ownedNfts" in data else data.get("nfts", [])
            # Normalize nested fields that might be strings instead of dicts
            return [self._normalize_nft_record(r) for r in records]

        elif table_name == "nft_metadata":
            return [self._normalize_nft_record(data)]

        elif table_name == "contract_metadata":
            # Ensure contractAddress is in the response (may be returned as 'address' or missing)
            record = dict(data)
            if "contractAddress" not in record:
                record["contractAddress"] = record.get("address") or table_options.get("contract_address")
            return [record]

        elif table_name == "nft_sales":
            return data.get("nftSales", [])

        elif table_name == "floor_prices":
            # API response doesn't include contractAddress, add it from table_options
            record = dict(data)
            record["contractAddress"] = table_options.get("contract_address")
            return [record]

        elif table_name == "token_prices":
            return data.get("data", [])

        elif table_name == "token_prices_historical":
            # API response doesn't include symbol, add it from table_options
            records = data.get("data", [])
            symbol = table_options.get("symbol")
            return [{**record, "symbol": symbol} for record in records]

        elif table_name == "tokens_by_wallet":
            # Response has data.tokens array with token data including network
            inner_data = data.get("data", data)
            tokens = inner_data.get("tokens", [])
            records = []
            default_network = table_options.get("network", self.default_network)
            for token in tokens:
                if "network" not in token:
                    token["network"] = default_network
                # Rename 'address' to 'wallet_address' for schema compatibility
                if "address" in token and "wallet_address" not in token:
                    token["wallet_address"] = token.pop("address")
                # Rename 'tokenAddress' to 'contractAddress' for schema compatibility
                if "tokenAddress" in token and "contractAddress" not in token:
                    token["contractAddress"] = token.pop("tokenAddress")
                # Extract nested tokenMetadata fields
                if "tokenMetadata" in token:
                    metadata = token.get("tokenMetadata", {})
                    if isinstance(metadata, dict):
                        if "decimals" not in token:
                            token["decimals"] = metadata.get("decimals")
                        if "name" not in token:
                            token["name"] = metadata.get("name")
                        if "symbol" not in token:
                            token["symbol"] = metadata.get("symbol")
                        if "logo" not in token:
                            token["logo"] = metadata.get("logo")
                # Extract price from tokenPrices array (first USD price)
                if "tokenPrices" in token and "price" not in token:
                    prices = token.get("tokenPrices", [])
                    if prices and isinstance(prices, list):
                        for p in prices:
                            if p.get("currency") == "usd":
                                try:
                                    token["price"] = float(p.get("value", 0))
                                except (ValueError, TypeError):
                                    token["price"] = None
                                break
                records.append(token)
            return records

        elif table_name == "token_balances_by_wallet":
            # Response has data.tokens array
            inner_data = data.get("data", data)
            tokens = inner_data.get("tokens", inner_data.get("balances", []))
            records = []
            default_network = table_options.get("network", self.default_network)
            for token in tokens:
                if "network" not in token:
                    token["network"] = default_network
                # Rename 'address' to 'wallet_address' for schema compatibility
                if "address" in token and "wallet_address" not in token:
                    token["wallet_address"] = token.pop("address")
                records.append(token)
            return records

        elif table_name == "nfts_by_wallet":
            # Response has data.ownedNfts array
            inner_data = data.get("data", data)
            nfts = inner_data.get("ownedNfts", inner_data.get("nfts", []))
            records = []
            default_network = table_options.get("network", self.default_network)
            for nft in nfts:
                # Ensure network field exists
                if "network" not in nft:
                    nft["network"] = default_network
                # Rename 'address' to 'wallet_address' for schema compatibility
                if "address" in nft and "wallet_address" not in nft:
                    nft["wallet_address"] = nft.pop("address")
                # Extract contractAddress from nested contract object
                if "contractAddress" not in nft and "contract" in nft:
                    contract = nft.get("contract", {})
                    if isinstance(contract, dict):
                        nft["contractAddress"] = contract.get("address")
                records.append(nft)
            return records

        elif table_name == "wallet_transactions":
            # Response is {"transactions": [...]} - transactions include network info
            transactions = data.get("transactions", [])
            records = []
            for tx in transactions:
                # Ensure network field exists (may come from tx or default)
                if "network" not in tx:
                    tx["network"] = table_options.get("network", self.default_network)
                records.append(tx)
            return records

        elif table_name == "webhooks":
            return data.get("data", [])

        elif table_name == "webhook_addresses":
            records = []
            webhook_id = table_options["webhook_id"]
            for address in data.get("addresses", []):
                records.append({
                    "webhook_id": webhook_id,
                    "address": address
                })
            return records

        elif table_name == "nft_metadata_batch":
            # Response contains an array of NFT metadata objects
            records = data if isinstance(data, list) else []
            return [self._normalize_nft_record(r) for r in records]

        elif table_name == "contract_metadata_batch":
            # Response contains an array of contract metadata objects
            return data if isinstance(data, list) else []

        elif table_name == "token_prices_by_address":
            # Similar structure to token_prices but keyed by address/network
            records = []
            for item in data.get("data", []):
                records.append(item)
            return records

        elif table_name == "nft_collections_by_wallet":
            records = []
            for wallet_data in data:
                wallet_address = wallet_data.get("address")
                network = wallet_data.get("network", table_options.get("network", self.default_network))
                for collection in wallet_data.get("collections", []):
                    record = {
                        "wallet_address": wallet_address,
                        "network": network,
                        **collection
                    }
                    records.append(record)
            return records

        return []

    def _convert_network_format(self, network: str) -> str:
        """Convert network from connector format (eth-mainnet) to Alchemy API format (ETH_MAINNET)."""
        # Replace hyphens with underscores and uppercase
        return network.replace("-", "_").upper()

    def _normalize_nft_record(self, record: Dict) -> Dict:
        """Normalize NFT record fields that might be strings instead of dicts."""
        record = dict(record)
        if "rawMetadata" in record and isinstance(record["rawMetadata"], str):
            try:
                record["rawMetadata"] = json.loads(record["rawMetadata"])
            except (json.JSONDecodeError, TypeError):
                record["rawMetadata"] = None
        if "tokenUri" in record and isinstance(record["tokenUri"], str):
            record["tokenUri"] = {"raw": record["tokenUri"], "gateway": record["tokenUri"]}
        # Handle contract field - might be string, dict, or missing (use contractAddress)
        if "contract" not in record:
            # Try to build contract from contractAddress if available
            contract_addr = record.get("contractAddress") or record.get("contract_address")
            if contract_addr:
                record["contract"] = {"address": contract_addr, "name": None, "symbol": None, "totalSupply": None, "tokenType": None}
            else:
                record["contract"] = {"address": "", "name": None, "symbol": None, "totalSupply": None, "tokenType": None}
        elif isinstance(record["contract"], str):
            record["contract"] = {"address": record["contract"], "name": None, "symbol": None, "totalSupply": None, "tokenType": None}
        if "media" in record and isinstance(record["media"], str):
            try:
                record["media"] = json.loads(record["media"])
            except (json.JSONDecodeError, TypeError):
                record["media"] = None
        if record.get("rawMetadata") and isinstance(record["rawMetadata"], dict):
            if "attributes" in record["rawMetadata"] and isinstance(record["rawMetadata"]["attributes"], str):
                try:
                    record["rawMetadata"]["attributes"] = json.loads(record["rawMetadata"]["attributes"])
                except (json.JSONDecodeError, TypeError):
                    record["rawMetadata"]["attributes"] = None
        return record

    def _calculate_next_offset(self, table_name: str, data: Dict, current_offset: Dict) -> Dict:
        """
        Calculate next offset for pagination.
        """
        config = self._table_configs[table_name]

        if not config["supports_pagination"]:
            return None

        if table_name in ["nfts_by_owner", "nft_sales"] and "pageKey" in data:
            return {"pageKey": data["pageKey"]}

        return None