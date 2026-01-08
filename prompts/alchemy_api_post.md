# Alchemy REST API Reference

## Overview

This document covers Alchemy's REST API endpoints (GET and POST), excluding JSON-RPC methods and Webhooks. These endpoints provide structured, indexed blockchain data optimized for high-volume reads, analytics, and dashboards.

## Authentication

All requests require an API key:

- **API Key**: Obtained from the Alchemy Dashboard
- **Base URL**: `https://api.g.alchemy.com` (for multi-chain endpoints)
- **Chain-Specific URL**: `https://{network}.g.alchemy.com/nft/v3/{apiKey}/` (for NFT API)

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

#### getContractsForOwner

```
GET https://{network}.g.alchemy.com/nft/v3/{apiKey}/getContractsForOwner
```

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `owner` | string | Yes | Wallet address |
| `pageKey` | string | No | Pagination cursor |
| `pageSize` | integer | No | Results per page |

#### getCollectionsForOwner

```
GET https://{network}.g.alchemy.com/nft/v3/{apiKey}/getCollectionsForOwner
```

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `owner` | string | Yes | Wallet address |
| `pageKey` | string | No | Pagination cursor |

---

### Metadata Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/getNFTsForContract` | GET | Get all NFTs for a contract/collection |
| `/getNFTMetadata` | GET | Get metadata for a specific NFT |
| `/getNFTMetadataBatch` | **POST** | Get metadata for multiple NFTs (up to 100) |
| `/getContractMetadata` | GET | Get metadata for an NFT contract |
| `/getContractMetadataBatch` | **POST** | Get metadata for multiple contracts |
| `/computeRarity` | GET | Compute rarity of NFT attributes |
| `/refreshNFTMetadata` | GET | Trigger metadata refresh for an NFT |
| `/summarizeNFTAttributes` | GET | Get attribute summary for a collection |
| `/searchContractMetadata` | GET | Search contracts by keyword |
| `/invalidateContract` | GET | Trigger collection metadata refresh |

#### getNFTMetadata (GET)

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

#### getNFTMetadataBatch (POST)

```
POST https://{network}.g.alchemy.com/nft/v3/{apiKey}/getNFTMetadataBatch
```

**Request Body:**

```json
{
  "tokens": [
    {
      "contractAddress": "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D",
      "tokenId": "3",
      "tokenType": "ERC721"
    },
    {
      "contractAddress": "0x8a90CAb2b38dba80c64b7734e58Ee1dB38B8992e",
      "tokenId": "4",
      "tokenType": "ERC721"
    }
  ],
  "tokenUriTimeoutInMs": 5000,
  "refreshCache": false
}
```

**Body Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `tokens` | array | Yes | Array of token objects (max 100) |
| `tokens[].contractAddress` | string | Yes | NFT contract address |
| `tokens[].tokenId` | string | Yes | Token ID |
| `tokens[].tokenType` | enum | No | `ERC721` or `ERC1155` |
| `tokenUriTimeoutInMs` | integer | No | Timeout for metadata fetch (ms) |
| `refreshCache` | boolean | No | Force cache refresh (default: false) |

**Example Request:**
```bash
curl -X POST "https://eth-mainnet.g.alchemy.com/nft/v3/{apiKey}/getNFTMetadataBatch" \
  -H "Content-Type: application/json" \
  -d '{
    "tokens": [
      {"contractAddress": "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D", "tokenId": "3"},
      {"contractAddress": "0x8a90CAb2b38dba80c64b7734e58Ee1dB38B8992e", "tokenId": "4"}
    ]
  }'
```

#### getContractMetadataBatch (POST)

```
POST https://{network}.g.alchemy.com/nft/v3/{apiKey}/getContractMetadataBatch
```

**Request Body:**

```json
{
  "contractAddresses": [
    "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D",
    "0x8a90CAb2b38dba80c64b7734e58Ee1dB38B8992e"
  ]
}
```

**Body Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `contractAddresses` | array | Yes | Array of contract addresses |

#### getNFTsForContract (GET)

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

#### getContractMetadata (GET)

```
GET https://{network}.g.alchemy.com/nft/v3/{apiKey}/getContractMetadata
```

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `contractAddress` | string | Yes | NFT contract address |

#### computeRarity (GET)

```
GET https://{network}.g.alchemy.com/nft/v3/{apiKey}/computeRarity
```

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `contractAddress` | string | Yes | NFT contract address |
| `tokenId` | string | Yes | Token ID |

#### searchContractMetadata (GET)

```
GET https://{network}.g.alchemy.com/nft/v3/{apiKey}/searchContractMetadata
```

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `query` | string | Yes | Search keyword |

---

### Spam Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/getSpamContracts` | GET | Get list of all spam contracts |
| `/isSpamContract` | GET | Check if a contract is marked as spam |
| `/isAirdropNFT` | GET | Check if an NFT is marked as airdrop |
| `/reportSpam` | GET | Report a contract as spam |

#### isSpamContract (GET)

```
GET https://{network}.g.alchemy.com/nft/v3/{apiKey}/isSpamContract
```

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `contractAddress` | string | Yes | Contract address to check |

#### isAirdropNFT (GET)

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

#### getFloorPrice (GET)

```
GET https://{network}.g.alchemy.com/nft/v3/{apiKey}/getFloorPrice
```

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `contractAddress` | string | Yes | NFT contract address |

#### getNFTSales (GET)

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
| `/tokens/by-address` | **POST** | Get token prices by contract address |
| `/tokens/historical` | **POST** | Get historical price data |

### Get Token Prices by Symbol (GET)

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

### Get Token Prices by Address (POST)

```
POST https://api.g.alchemy.com/prices/v1/{apiKey}/tokens/by-address
```

**Request Body:**

```json
{
  "addresses": [
    {
      "network": "eth-mainnet",
      "address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
    },
    {
      "network": "eth-mainnet",
      "address": "0x6B175474E89094C44Da98b954EessdcD16C26EB"
    }
  ]
}
```

**Body Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `addresses` | array | Yes | Array of network/address pairs (max 25 addresses, max 3 networks) |
| `addresses[].network` | string | Yes | Network identifier (e.g., `eth-mainnet`) |
| `addresses[].address` | string | Yes | Token contract address |

**Example Request:**
```bash
curl -X POST "https://api.g.alchemy.com/prices/v1/{apiKey}/tokens/by-address" \
  -H "Content-Type: application/json" \
  -d '{
    "addresses": [
      {"network": "eth-mainnet", "address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"}
    ]
  }'
```

### Get Historical Token Prices (POST)

```
POST https://api.g.alchemy.com/prices/v1/{apiKey}/tokens/historical
```

**Request Body (by symbol):**

```json
{
  "symbol": "ETH",
  "startTime": "2024-01-01T00:00:00Z",
  "endTime": "2024-01-31T23:59:59Z",
  "interval": "1d"
}
```

**Request Body (by address):**

```json
{
  "network": "eth-mainnet",
  "address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
  "startTime": "2024-01-01T00:00:00Z",
  "endTime": "2024-01-31T23:59:59Z",
  "interval": "1d"
}
```

**Body Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `symbol` | string | Conditional | Token symbol (use this OR network+address) |
| `network` | string | Conditional | Network identifier |
| `address` | string | Conditional | Contract address |
| `startTime` | string | Yes | ISO timestamp start |
| `endTime` | string | Yes | ISO timestamp end |
| `interval` | string | No | Data interval (e.g., `1h`, `1d`) |

---

## Portfolio API Endpoints

Base URL: `https://api.g.alchemy.com/data/v1/{apiKey}`

Multi-chain endpoints for comprehensive wallet data. All Portfolio API endpoints use **POST** method.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/getTokensByWallet` | **POST** | Get tokens with balances, prices, metadata |
| `/getTokenBalancesByWallet` | **POST** | Get token balances only |
| `/getNftsByWallet` | **POST** | Get NFTs across networks |
| `/getNftCollectionsByWallet` | **POST** | Get NFT collections owned |
| `/getTransactionsByWallet` | **POST** | Get transaction history |

### Get Tokens by Wallet (POST)

```
POST https://api.g.alchemy.com/data/v1/{apiKey}/getTokensByWallet
```

**Request Body:**

```json
{
  "addresses": [
    {
      "address": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",
      "networks": ["eth-mainnet", "polygon-mainnet", "arb-mainnet"]
    }
  ],
  "withMetadata": true,
  "withPrices": true,
  "includeNativeTokens": true,
  "includeErc20Tokens": true
}
```

**Body Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `addresses` | array | Yes | Wallet addresses with networks (max 2 addresses, max 5 networks each) |
| `addresses[].address` | string | Yes | Wallet address |
| `addresses[].networks` | array | Yes | Networks to query |
| `withMetadata` | boolean | No | Include token metadata (default: true) |
| `withPrices` | boolean | No | Include token prices (default: true) |
| `includeNativeTokens` | boolean | No | Include native tokens like ETH (default: true) |
| `includeErc20Tokens` | boolean | No | Include ERC-20 tokens (default: true) |

**Example Request:**
```bash
curl -X POST "https://api.g.alchemy.com/data/v1/{apiKey}/getTokensByWallet" \
  -H "Content-Type: application/json" \
  -d '{
    "addresses": [
      {
        "address": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",
        "networks": ["eth-mainnet", "polygon-mainnet"]
      }
    ]
  }'
```

### Get Token Balances by Wallet (POST)

```
POST https://api.g.alchemy.com/data/v1/{apiKey}/getTokenBalancesByWallet
```

**Request Body:**

```json
{
  "addresses": [
    {
      "address": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",
      "networks": ["eth-mainnet", "polygon-mainnet"]
    }
  ],
  "includeNativeTokens": true,
  "includeErc20Tokens": true
}
```

**Body Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `addresses` | array | Yes | Address/network pairs (max 3 pairs, max 20 networks) |
| `includeNativeTokens` | boolean | No | Include native tokens (default: true) |
| `includeErc20Tokens` | boolean | No | Include ERC-20 tokens (default: true) |

### Get NFTs by Wallet (POST)

```
POST https://api.g.alchemy.com/data/v1/{apiKey}/getNftsByWallet
```

**Request Body:**

```json
{
  "addresses": [
    {
      "address": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",
      "networks": ["eth-mainnet", "polygon-mainnet"]
    }
  ],
  "withMetadata": true
}
```

**Body Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `addresses` | array | Yes | Address/network pairs (max 2 pairs, max 15 networks) |
| `withMetadata` | boolean | No | Include NFT metadata (default: true) |

### Get NFT Collections by Wallet (POST)

```
POST https://api.g.alchemy.com/data/v1/{apiKey}/getNftCollectionsByWallet
```

**Request Body:**

```json
{
  "addresses": [
    {
      "address": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",
      "networks": ["eth-mainnet", "polygon-mainnet"]
    }
  ],
  "withMetadata": true
}
```

**Body Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `addresses` | array | Yes | Address/network pairs (max 2 pairs, max 15 networks) |
| `withMetadata` | boolean | No | Include collection metadata (default: true) |

### Get Transactions by Wallet (POST)

```
POST https://api.g.alchemy.com/data/v1/{apiKey}/getTransactionsByWallet
```

**Request Body:**

```json
{
  "addresses": [
    {
      "address": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",
      "networks": ["eth-mainnet"]
    }
  ],
  "pageSize": 100
}
```

**Body Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `addresses` | array | Yes | Address/network pairs |
| `pageSize` | integer | No | Results per page |
| `pageKey` | string | No | Pagination cursor |

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

## Endpoint Summary Table

### GET Endpoints

| API | Endpoint | Description |
|-----|----------|-------------|
| NFT | `/getNFTsForOwner` | Get NFTs for wallet |
| NFT | `/getOwnersForNFT` | Get owners of NFT |
| NFT | `/getOwnersForContract` | Get all owners for collection |
| NFT | `/isHolderOfContract` | Check if wallet holds NFT |
| NFT | `/getContractsForOwner` | Get NFT contracts for wallet |
| NFT | `/getCollectionsForOwner` | Get collections for wallet |
| NFT | `/getNFTsForContract` | Get all NFTs in collection |
| NFT | `/getNFTMetadata` | Get single NFT metadata |
| NFT | `/getContractMetadata` | Get collection metadata |
| NFT | `/computeRarity` | Get NFT rarity |
| NFT | `/refreshNFTMetadata` | Refresh NFT metadata |
| NFT | `/summarizeNFTAttributes` | Get attribute summary |
| NFT | `/searchContractMetadata` | Search collections |
| NFT | `/invalidateContract` | Refresh collection metadata |
| NFT | `/getSpamContracts` | Get spam contract list |
| NFT | `/isSpamContract` | Check if spam |
| NFT | `/isAirdropNFT` | Check if airdrop |
| NFT | `/reportSpam` | Report spam |
| NFT | `/getFloorPrice` | Get floor price |
| NFT | `/getNFTSales` | Get sales history |
| Prices | `/tokens/by-symbol` | Get prices by symbol |

### POST Endpoints

| API | Endpoint | Description |
|-----|----------|-------------|
| NFT | `/getNFTMetadataBatch` | Batch get NFT metadata (100 max) |
| NFT | `/getContractMetadataBatch` | Batch get collection metadata |
| Prices | `/tokens/by-address` | Get prices by contract address |
| Prices | `/tokens/historical` | Get historical prices |
| Portfolio | `/getTokensByWallet` | Get tokens with prices/metadata |
| Portfolio | `/getTokenBalancesByWallet` | Get token balances |
| Portfolio | `/getNftsByWallet` | Get NFTs multi-chain |
| Portfolio | `/getNftCollectionsByWallet` | Get NFT collections multi-chain |
| Portfolio | `/getTransactionsByWallet` | Get transaction history |

---

## Resources

- **Documentation**: https://docs.alchemy.com
- **API Reference**: https://docs.alchemy.com/reference/api-overview
- **Dashboard**: https://dashboard.alchemy.com
- **Sandbox**: https://dashboard.alchemy.com/sandbox
- **Support**: https://support.alchemy.com

---

*Last updated: January 2026*
