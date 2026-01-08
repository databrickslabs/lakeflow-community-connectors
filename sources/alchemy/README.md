# Alchemy Community Connector

This connector provides access to Alchemy's comprehensive blockchain data APIs, including NFTs, token prices, wallet balances, and transaction history across multiple networks.

## Features

- **NFT Data**: Access NFT ownership, metadata, sales history, and floor prices
- **Token Data**: Real-time and historical token prices
- **Wallet Data**: Token balances, NFT holdings, and transaction history
- **Multi-Network Support**: Ethereum, Polygon, Arbitrum, Optimism, and more
- **Webhook Management**: Monitor webhooks and tracked addresses

## Supported Tables

### NFT Tables
- `nfts_by_owner` - NFTs owned by a wallet address
- `nfts_for_contract` - All NFTs in a collection
- `nft_metadata` - Metadata for specific NFTs
- `nft_metadata_batch` - **POST** - Batch metadata for multiple NFTs (up to 100)
- `contract_metadata` - Metadata for NFT contracts
- `contract_metadata_batch` - **POST** - Batch metadata for multiple contracts
- `nft_sales` - NFT sales history
- `floor_prices` - Floor prices by marketplace

### Token Tables
- `token_prices` - Current token prices by symbol
- `token_prices_by_address` - **POST** - Current token prices by contract address
- `token_prices_historical` - **POST** - Historical price data

### Portfolio Tables (All POST)
- `tokens_by_wallet` - Token balances with metadata and prices
- `token_balances_by_wallet` - Token balances only
- `nfts_by_wallet` - NFTs owned by wallets
- `nft_collections_by_wallet` - NFT collections owned by wallets
- `wallet_transactions` - Transaction history

### Webhook Tables
- `webhooks` - Team webhooks
- `webhook_addresses` - Addresses tracked by webhooks

## Configuration

### Connection Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `api_key` | string | Yes | Alchemy API key from dashboard |
| `network` | string | No | Default network (default: eth-mainnet) |
| `webhook_auth_token` | string | No | Auth token for webhook endpoints |

### Table-Specific Options

| Option | Tables | Description |
|--------|---------|-------------|
| `network` | All NFT tables | Override default network |
| `owner_address` | nfts_by_owner | Wallet address to query |
| `contract_address` | Various NFT tables | NFT contract address |
| `token_id` | Various NFT tables | Specific token ID |
| `with_metadata` | NFT tables | Include metadata (default: true) |
| `page_size` | Paginated tables | Results per page (max 100) |
| `symbols` | token_prices | Comma-separated token symbols |
| `addresses` | Wallet tables | Wallet addresses with networks |

## Example Usage

### NFTs by Owner
```python
from sources.alchemy.alchemy import LakeflowConnect

connector = LakeflowConnect({
    "api_key": "your_api_key",
    "network": "eth-mainnet"
})

# Get NFTs owned by an address
schema = connector.get_table_schema("nfts_by_owner", {})
metadata = connector.read_table_metadata("nfts_by_owner", {})
records, offset = connector.read_table("nfts_by_owner", None, {
    "owner_address": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",
    "with_metadata": "true",
    "page_size": "50"
})
```

### Token Prices
```python
records, offset = connector.read_table("token_prices", None, {
    "symbols": "ETH,BTC,USDC"
})
```

### Wallet Tokens
```python
records, offset = connector.read_table("tokens_by_wallet", None, {
    "addresses": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045@eth-mainnet",
    "with_metadata": "true",
    "with_prices": "true"
})
```

## Supported Networks

- **Ethereum**: eth-mainnet, eth-sepolia, eth-holesky
- **Layer 2**: arb-mainnet, opt-mainnet, base-mainnet, polygon-mainnet
- **Other**: bnb-mainnet, avax-mainnet, linea-mainnet, solana-mainnet

## Rate Limits

Alchemy uses Compute Units (CU) for billing:
- **Free Tier**: 300M monthly CU
- **Growth Tier**: Pay-as-you-go
- **Scale/Enterprise**: Higher limits and support

## Data Types

- **Snapshot**: One-time data extraction
- **CDC**: Incremental updates with cursor-based pagination
- **Append**: Continuous data addition

## Authentication

1. Get an API key from [Alchemy Dashboard](https://dashboard.alchemy.com)
2. For webhook endpoints, obtain auth token from Webhooks dashboard
3. Configure connection with your credentials

## Development

### Testing
```bash
# Update dev_config.json with your API key
# Run tests
python -m pytest sources/alchemy/test/
```

### Configuration Files
- `configs/dev_config.json` - Connection parameters
- `configs/dev_table_config.json` - Table-specific options for testing

## API Documentation

See `alchemy_api_doc.md` for detailed API endpoint documentation.

## Support

- [Alchemy Documentation](https://docs.alchemy.com)
- [Alchemy Support](https://support.alchemy.com)
- [Community Discord](https://discord.gg/alchemy)