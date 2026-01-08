# Alchemy REST API - GET Endpoints Reference

## Overview

This document covers Alchemy's REST API GET endpoints only, excluding JSON-RPC methods. These endpoints provide structured, indexed blockchain data optimized for high-volume reads, analytics, and dashboards.

## Authentication

All requests require an API key:

- **API Key**: Obtained from the Alchemy Dashboard
- **Base URL**: `https://api.g.alchemy.com` (for multi-chain endpoints)
- **Chain-Specific URL**: `https://{network}.g.alchemy.com/nft/v3/{apiKey}/` (for NFT API)
- **Auth Token**: Required separately for Webhook management endpoints (found in Webhooks dashboard)

---

## NFT API Endpoints

Base URL: `https://{network}.g.alchemy.com/nft/v3/{apiKey}`

### Ownership Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/getNFTsForOwner` | GET | Retrieve all NFTs owned by a wallet address |
| `/getOwnersForNFT` | GET | Get all owners of a specific NFT token |
| `/getOwnersForContract` | GET | Get all owners for an NFT contract/collection |
| `/isHolderOfContract` | GET | Check if a wallet owns any NFT in a collection |
| `/getContractsForOwner` | GET | Get all NFT contracts an address owns tokens from |
| `/getCollectionsForOwner` | GET | Get all NFT collections held by an owner |

#### getNFTsForOwner

```
GET https://{network}.g.alchemy.com/nft/v3/{apiKey}/getNFTsForOwner
```

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `owner` | string | Yes | Wallet address (supports ENS on Ethereum) |
| `contractAddresses[]` | array | No | Filter by contract addresses (max 45) |
| `withMetadata` | boolean | No | Include NFT metadata (default: true) |
| `pageKey` | string | No | Pagination cursor |
| `pageSize` | integer | No | Results per page (max 100) |
| `orderBy` | enum | No | `transferTime` for newest first |
| `excludeFilters[]` | array | No | Exclude spam, airdrops, etc. |

**Example Request:**
```bash
curl "https://eth-mainnet.g.alchemy.com/nft/v3/{apiKey}/getNFTsForOwner?owner=0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045&withMetadata=true&pageSize=100"
```

#### getOwnersForNFT

```
GET https://{network}.g.alchemy.com/nft/v3/{apiKey}/getOwnersForNFT
```

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `contractAddress` | string | Yes | NFT contract address |
| `tokenId` | string | Yes | Token ID |

#### getOwnersForContract

```
GET https://{network}.g.alchemy.com/nft/v3/{apiKey}/getOwnersForContract
```

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `contractAddress` | string | Yes | NFT contract address |
| `withTokenBalances` | boolean | No | Include token counts per owner |
| `block` | string | No | Block number for snapshot |
| `pageKey` | string | No | Pagination cursor |

#### isHolderOfContract

```
GET https://{network}.g.alchemy.com/nft/v3/{apiKey}/isHolderOfContract
```

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `wallet` | string | Yes | Wallet address to check |
| `contractAddress` | string | Yes | NFT contract address |

---

### Metadata Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/getNFTsForContract` | GET | Get all NFTs for a contract/collection |
| `/getNFTMetadata` | GET | Get metadata for a specific NFT |
| `/getContractMetadata` | GET | Get metadata for an NFT contract |
| `/computeRarity` | GET | Compute rarity of NFT attributes |
| `/refreshNFTMetadata` | GET | Trigger metadata refresh for an NFT |
| `/summarizeNFTAttributes` | GET | Get attribute summary for a collection |
| `/searchContractMetadata` | GET | Search contracts by keyword |
| `/invalidateContract` | GET | Trigger collection metadata refresh |

#### getNFTMetadata

```
GET https://{network}.g.alchemy.com/nft/v3/{apiKey}/getNFTMetadata
```

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `contractAddress` | string | Yes | NFT contract address |
| `tokenId` | string | Yes | Token ID |
| `tokenType` | enum | No | `ERC721` or `ERC1155` |
| `refreshCache` | boolean | No | Force metadata refresh |

#### getNFTsForContract

```
GET https://{network}.g.alchemy.com/nft/v3/{apiKey}/getNFTsForContract
```

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `contractAddress` | string | Yes | NFT contract address |
| `withMetadata` | boolean | No | Include metadata (default: true) |
| `startToken` | string | No | Start from token ID |
| `limit` | integer | No | Max results (default: 100) |

#### getContractMetadata

```
GET https://{network}.g.alchemy.com/nft/v3/{apiKey}/getContractMetadata
```

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `contractAddress` | string | Yes | NFT contract address |

#### computeRarity

```
GET https://{network}.g.alchemy.com/nft/v3/{apiKey}/computeRarity
```

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `contractAddress` | string | Yes | NFT contract address |
| `tokenId` | string | Yes | Token ID |

---

### Spam Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/getSpamContracts` | GET | Get list of all spam contracts |
| `/isSpamContract` | GET | Check if a contract is marked as spam |
| `/isAirdropNFT` | GET | Check if an NFT is marked as airdrop |
| `/reportSpam` | GET | Report a contract as spam |

#### isSpamContract

```
GET https://{network}.g.alchemy.com/nft/v3/{apiKey}/isSpamContract
```

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `contractAddress` | string | Yes | Contract address to check |

#### isAirdropNFT

```
GET https://{network}.g.alchemy.com/nft/v3/{apiKey}/isAirdropNFT
```

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `contractAddress` | string | Yes | NFT contract address |
| `tokenId` | string | Yes | Token ID |

---

### Sales Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/getFloorPrice` | GET | Get floor price by marketplace |
| `/getNFTSales` | GET | Get NFT sales history |

#### getFloorPrice

```
GET https://{network}.g.alchemy.com/nft/v3/{apiKey}/getFloorPrice
```

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `contractAddress` | string | Yes | NFT contract address |

#### getNFTSales

```
GET https://{network}.g.alchemy.com/nft/v3/{apiKey}/getNFTSales
```

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `contractAddress` | string | No | Filter by contract |
| `tokenId` | string | No | Filter by token ID |
| `fromBlock` | integer | No | Start block |
| `toBlock` | integer | No | End block |
| `order` | enum | No | `asc` or `desc` |
| `pageKey` | string | No | Pagination cursor |

---

## Prices API Endpoints

Base URL: `https://api.g.alchemy.com/prices/v1/{apiKey}`

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/tokens/by-symbol` | GET | Get token prices by symbol |
| `/tokens/historical` | GET | Get historical price data |

#### Get Token Prices by Symbol

```
GET https://api.g.alchemy.com/prices/v1/{apiKey}/tokens/by-symbol
```

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `symbols` | array | Yes | Token symbols (max 25). Example: `symbols=ETH,BTC` |

**Example Request:**
```bash
curl "https://api.g.alchemy.com/prices/v1/{apiKey}/tokens/by-symbol?symbols=ETH,BTC,USDC"
```

**Example Response:**
```json
{
  "data": [
    {
      "symbol": "ETH",
      "prices": [
        { "currency": "usd", "value": "3245.67" }
      ]
    }
  ]
}
```

#### Get Historical Token Prices

```
GET https://api.g.alchemy.com/prices/v1/{apiKey}/tokens/historical
```

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `symbol` | string | Conditional | Token symbol (or use network + address) |
| `network` | string | Conditional | Network name |
| `address` | string | Conditional | Contract address |
| `startTime` | string | Yes | ISO timestamp start |
| `endTime` | string | Yes | ISO timestamp end |
| `interval` | string | No | Data interval |

---

## Portfolio API Endpoints

Base URL: `https://api.g.alchemy.com/data/v1/{apiKey}`

Multi-chain endpoints for comprehensive wallet data.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/getTokensByWallet` | GET | Get tokens with balances, prices, metadata |
| `/getTokenBalancesByWallet` | GET | Get token balances only |
| `/getNftsByWallet` | GET | Get NFTs across networks |
| `/getNftCollectionsByWallet` | GET | Get NFT collections owned |
| `/getTransactionsByWallet` | GET | Get transaction history |

#### Get Tokens by Wallet

```
GET https://api.g.alchemy.com/data/v1/{apiKey}/getTokensByWallet
```

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `addresses` | array | Yes | Wallet addresses with networks (max 2 addresses, max 5 networks each) |
| `withMetadata` | boolean | No | Include token metadata (default: true) |
| `withPrices` | boolean | No | Include token prices (default: true) |
| `includeNativeTokens` | boolean | No | Include native tokens like ETH (default: true) |

#### Get Token Balances by Wallet

```
GET https://api.g.alchemy.com/data/v1/{apiKey}/getTokenBalancesByWallet
```

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `addresses` | array | Yes | Address/network pairs (max 3 pairs, max 20 networks) |
| `includeNativeTokens` | boolean | No | Include native tokens (default: true) |
| `includeErc20Tokens` | boolean | No | Include ERC-20 tokens (default: true) |

---

## Webhooks (Notify) API - GET Endpoints

Base URL: `https://dashboard.alchemy.com/api`

**Authentication**: Requires `X-Alchemy-Token` header with Auth Token

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/team-webhooks` | GET | Get all webhooks for your team |
| `/webhook-addresses` | GET | Get addresses tracked by a webhook |

#### Get All Webhooks

```
GET https://dashboard.alchemy.com/api/team-webhooks
```

**Headers:**

| Header | Required | Description |
|--------|----------|-------------|
| `X-Alchemy-Token` | Yes | Auth token from Webhooks dashboard |

**Example Request:**
```bash
curl "https://dashboard.alchemy.com/api/team-webhooks" \
  -H "X-Alchemy-Token: YOUR_AUTH_TOKEN"
```

**Example Response:**
```json
{
  "data": [
    {
      "id": "wh_abc123",
      "network": "ETH_MAINNET",
      "webhook_type": "ADDRESS_ACTIVITY",
      "webhook_url": "https://your-server.com/webhook",
      "is_active": true,
      "time_created": 1234567890,
      "addresses": ["0x..."]
    }
  ]
}
```

#### Get Webhook Addresses

```
GET https://dashboard.alchemy.com/api/webhook-addresses
```

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `webhook_id` | string | Yes | Webhook ID |
| `limit` | integer | No | Results per page |
| `after` | string | No | Pagination cursor |

---

## Supported Networks

The following networks support the REST APIs (availability varies by endpoint):

| Category | Networks |
|----------|----------|
| **Ethereum & L2s** | `eth-mainnet`, `eth-sepolia`, `eth-holesky`, `arb-mainnet`, `arb-sepolia`, `opt-mainnet`, `opt-sepolia`, `base-mainnet`, `base-sepolia` |
| **Polygon** | `polygon-mainnet`, `polygon-amoy`, `polygonzkevm-mainnet` |
| **Other EVM** | `bnb-mainnet`, `avax-mainnet`, `linea-mainnet`, `zksync-mainnet`, `scroll-mainnet`, `blast-mainnet` |
| **Solana** | `solana-mainnet`, `solana-devnet` |

*Note: Not all endpoints are available on all networks. Check the Alchemy documentation for specific chain support.*

---

## Pagination

Most list endpoints support pagination:

| Parameter | Description |
|-----------|-------------|
| `pageKey` | Cursor returned from previous response |
| `pageSize` | Number of results per page (varies by endpoint) |
| `limit` | Alternative to pageSize on some endpoints |

**Example Paginated Request:**
```bash
# First request
curl "https://eth-mainnet.g.alchemy.com/nft/v3/{apiKey}/getNFTsForOwner?owner=0x...&pageSize=100"

# Subsequent request with pageKey
curl "https://eth-mainnet.g.alchemy.com/nft/v3/{apiKey}/getNFTsForOwner?owner=0x...&pageSize=100&pageKey=abc123..."
```

---

## Rate Limits & Pricing

Alchemy uses Compute Units (CUs) for billing:

| Tier | Monthly CUs | Notes |
|------|-------------|-------|
| **Free** | 300M | Rate-limited |
| **Growth** | Pay-as-you-go | Higher limits |
| **Scale** | Custom | Higher throughput |
| **Enterprise** | Unlimited | Dedicated support |

---

## Error Handling

Common HTTP status codes:

| Code | Description |
|------|-------------|
| `200` | Success |
| `400` | Bad request (invalid parameters) |
| `401` | Unauthorized (invalid API key) |
| `429` | Rate limit exceeded |
| `500` | Internal server error |

**Error Response Format:**
```json
{
  "error": {
    "code": 400,
    "message": "Invalid contract address format"
  }
}
```

---

## Resources

- **Documentation**: https://docs.alchemy.com
- **API Reference**: https://docs.alchemy.com/reference/api-overview
- **Dashboard**: https://dashboard.alchemy.com
- **Sandbox**: https://dashboard.alchemy.com/sandbox
- **Support**: https://support.alchemy.com

---

*Last updated: January 2026*
