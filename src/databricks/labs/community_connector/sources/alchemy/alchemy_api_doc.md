# Alchemy API Documentation

## Table Selection Rationale

The Alchemy product surface is large (JSON-RPC node APIs, Webhook APIs, Account Abstraction APIs, etc.). This document focuses on the **data-plane APIs** that are useful for analytics and portfolio-style connectors: the **Portfolio API**, **Prices API**, and **NFT API v3**. These three families share a common authentication pattern and are the APIs covered by Airbyte and Fivetran Web3 connectors.

**Tables documented in this file:**

| # | Table Name | API Family | Ingestion Type |
|---|------------|-----------|----------------|
| 1 | `tokens_by_wallet` | Portfolio | snapshot |
| 2 | `token_balances_by_wallet` | Portfolio | snapshot |
| 3 | `nft_collections_by_wallet` | Portfolio | snapshot |
| 4 | `wallet_transactions` | Portfolio (Beta) | append |
| 5 | `token_prices_by_symbol` | Prices | snapshot |
| 6 | `token_prices_by_address` | Prices | snapshot |
| 7 | `token_prices_historical` | Prices | append |
| 8 | `nfts_by_wallet` | NFT v3 | snapshot |
| 9 | `nft_metadata` | NFT v3 | snapshot |
| 10 | `nft_contract_metadata` | NFT v3 | snapshot |
| 11 | `nft_floor_prices` | NFT v3 | snapshot |

**Design choices:**
- `wallet_transactions` is included despite beta status (ETH+Base only) because transaction history is core analytics data. It is flagged as deferred to production once the endpoint graduates from beta.
- `nfts_by_wallet` (NFT v3 `getNFTsForOwner`) is kept alongside the Portfolio-family `nft_collections_by_wallet` because the two have different granularity: one returns per-token NFTs, the other returns contract-level collections.
- Pure JSON-RPC node endpoints (e.g., `eth_getBalance`, `eth_getLogs`) are intentionally excluded — they are low-level primitives, not analytics tables.

**Deferred tables** (documented at the bottom of this file): `nfts_for_contract` (useful for full collection enumeration but very high-volume / high-CU), NFT sales/transfers (`getNFTSales`, `alchemy_getAssetTransfers`).

---

## Authorization

**Method:** API key embedded in the URL path (no `Authorization` header required).

All Alchemy API families embed the API key directly in the URL path, not in a header or query parameter. There is no OAuth flow.

**Portfolio API and Prices API:**
```
https://api.g.alchemy.com/{product}/v1/{apiKey}/...
```

**NFT API v3:**
```
https://{network}.g.alchemy.com/nft/v3/{apiKey}/...
```

**Configuration required:**
- `api_key` (string) — Alchemy API key, obtained from the Alchemy Dashboard (https://dashboard.alchemy.com).

**Obtaining a key:**
1. Create a free account at https://www.alchemy.com.
2. Create an App in the Dashboard. The API key is shown on the App detail page.
3. Use the same key for all three API families (Portfolio, Prices, NFT).

**Example request (Portfolio API):**
```
POST https://api.g.alchemy.com/data/v1/YOUR_API_KEY/assets/tokens/by-address
Content-Type: application/json

{
  "addresses": [
    {
      "address": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",
      "networks": ["eth-mainnet"]
    }
  ]
}
```

**Example request (NFT API v3):**
```
GET https://eth-mainnet.g.alchemy.com/nft/v3/YOUR_API_KEY/getNFTsForOwner?owner=0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045
```

**Security note:** Because the API key is in the URL, it will appear in server logs. Treat it as a secret. Alchemy recommends using allow-lists on the key (via Dashboard settings) when deploying to production.

---

## Supported Networks

Networks are part of the **URL path**, not a query parameter.

### Portfolio API and Prices API

These APIs use the global `api.g.alchemy.com` host. Networks are specified in the **request body** (not the URL) as string identifiers.

**Token endpoints** (`tokens/by-address`, `tokens/balances/by-address`): Ethereum, Solana, and 30+ EVM chains.

**NFT endpoints** (`nfts/contracts/by-address`): Ethereum and EVM L2s (Polygon, Arbitrum, Optimism, Base, World Chain, and more).

**Transactions** (`transactions/history/by-address`): **Beta** — ETH mainnet and Base mainnet only.

Common EVM network identifiers used in request bodies:

| Network | Identifier |
|---------|------------|
| Ethereum Mainnet | `eth-mainnet` |
| Polygon PoS | `polygon-mainnet` or `matic-mainnet` |
| Arbitrum One | `arb-mainnet` |
| Optimism | `opt-mainnet` |
| Base | `base-mainnet` |
| Solana Mainnet | `sol-mainnet` |

For the full chain list: https://dashboard.alchemy.com/chains

### NFT API v3

Uses the network as the **URL subdomain**: `https://{network}.g.alchemy.com/nft/v3/{apiKey}/...`

Supported networks for NFT v3: `eth-mainnet`, `polygon-mainnet`, `arb-mainnet`, `opt-mainnet`, `base-mainnet`, and other EVM chains. NFT v3 does **not** support Solana.

`getFloorPrice` is restricted to **Ethereum mainnet only** (OpenSea and LooksRare marketplaces).

---

## Rate Limits and Compute Units

### Compute Units (CU)

Every Alchemy API call consumes a number of Compute Units based on the complexity of the request. Monthly CU quotas apply.

**Plan tiers:**

| Plan | Monthly CUs | Throughput (CUPs) |
|------|-------------|-------------------|
| Free | 30,000,000 | 500 CU/s |
| Pay-as-you-go | Unlimited (metered at $0.45/M CU) | 10,000 CU/s |
| Enterprise | Custom | Custom |

**CU costs per endpoint:**

| Endpoint | CU Cost |
|----------|---------|
| `assets/tokens/by-address` (Portfolio) | 360 CU |
| `assets/tokens/balances/by-address` (Portfolio) | 200 CU |
| `assets/nfts/contracts/by-address` (Portfolio) | 600 CU |
| `assets/nfts/by-address` (Portfolio) | 1,000 CU |
| `transactions/history/by-address` (Portfolio) | 1,000 CU |
| `prices/tokens/by-symbol` | 40 CU |
| `prices/tokens/by-address` | 40 CU |
| `prices/tokens/historical` | 40 CU |
| `getNFTsForOwner` (NFT v3) | 480 CU |
| `getNFTMetadata` (NFT v3) | 80 CU |
| `getContractMetadata` (NFT v3) | 160 CU |
| `getFloorPrice` (NFT v3) | 80 CU |

**Throughput** is measured in CU/s and evaluated over a 10-second rolling window using a token-bucket algorithm. A free-tier key (500 CU/s limit) can burst up to 5,000 CUs within any 10-second window.

### Retry Semantics

**Retriable HTTP status codes:** 429 (Too Many Requests), 500 (Internal Server Error), 503 (Service Unavailable).

**Non-retriable:** 400 (Bad Request — malformed input), 401/403 (auth failure), 404 (not found).

**Recommended backoff strategy (exponential with jitter):**
1. On 429, check the `Retry-After` response header if present; honor it.
2. Otherwise use exponential backoff: wait `min(2^attempt + random_ms, 64_000)` milliseconds.
3. Random jitter: 0–1,000 ms added per retry to avoid thundering-herd.
4. Maximum retries: 5.
5. Simple alternative: random wait of 1,000–1,250 ms per retry (simpler but less efficient at scale).

---

## Object List

The connector exposes a **static** set of tables. There is no discovery API that enumerates available tables at runtime. The table list is fixed:

| Table Name | API Endpoint | Description |
|------------|-------------|-------------|
| `tokens_by_wallet` | `POST /data/v1/{key}/assets/tokens/by-address` | Fungible token balances with metadata and prices per wallet |
| `token_balances_by_wallet` | `POST /data/v1/{key}/assets/tokens/balances/by-address` | Raw on-chain fungible token balances (no metadata/prices) |
| `nft_collections_by_wallet` | `POST /data/v1/{key}/assets/nfts/contracts/by-address` | NFT collections (contracts) held per wallet |
| `wallet_transactions` | `POST /data/v1/{key}/transactions/history/by-address` | Full transaction history per wallet (Beta: ETH+Base only) |
| `token_prices_by_symbol` | `GET /prices/v1/{key}/tokens/by-symbol` | Current prices for tokens by symbol |
| `token_prices_by_address` | `POST /prices/v1/{key}/tokens/by-address` | Current prices for tokens by contract address |
| `token_prices_historical` | `POST /prices/v1/{key}/tokens/historical` | Historical OHLCV-style price series for a single token |
| `nfts_by_wallet` | `GET /{network}.g.alchemy.com/nft/v3/{key}/getNFTsForOwner` | All individual NFTs owned by a wallet |
| `nft_metadata` | `GET /{network}.g.alchemy.com/nft/v3/{key}/getNFTMetadata` | Metadata for a specific NFT token |
| `nft_contract_metadata` | `GET /{network}.g.alchemy.com/nft/v3/{key}/getContractMetadata` | Collection/contract-level metadata |
| `nft_floor_prices` | `GET /eth-mainnet.g.alchemy.com/nft/v3/{key}/getFloorPrice` | Floor prices per collection by marketplace (ETH only) |

---

## Object Schema and Read API

### Table 1: `tokens_by_wallet`

**Description:** Returns fungible token balances (native + ERC-20 + SPL), enriched with token metadata and current USD prices, for one or more wallet addresses across one or more networks.

**Endpoint:**
```
POST https://api.g.alchemy.com/data/v1/{apiKey}/assets/tokens/by-address
```

**Request body parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `addresses` | array | Yes | — | Max 2 items. Each item has `address` (string) and `networks` (array, max 5 per address) |
| `addresses[].address` | string | Yes | — | Wallet address (hex for EVM, base58 for Solana) |
| `addresses[].networks` | array | Yes | — | Network identifiers (e.g., `["eth-mainnet", "base-mainnet"]`) |
| `withMetadata` | boolean | No | `true` | Include `tokenMetadata` in response |
| `withPrices` | boolean | No | `true` | Include `tokenPrices` in response |
| `includeNativeTokens` | boolean | No | `true` | Include native chain tokens (ETH, MATIC, etc.) |
| `includeErc20Tokens` | boolean | No | `true` | Include ERC-20 tokens |
| `pageKey` | string | No | — | Cursor for the next page of results |

**Example request:**
```json
{
  "addresses": [
    {
      "address": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",
      "networks": ["eth-mainnet", "polygon-mainnet"]
    }
  ],
  "withMetadata": true,
  "withPrices": true
}
```

**Response schema (HTTP 200):**

```json
{
  "data": {
    "tokens": [
      {
        "address": "string",
        "network": "string",
        "tokenAddress": "string | null",
        "tokenBalance": "string",
        "tokenMetadata": {
          "decimals": "integer",
          "logo": "string | null",
          "name": "string",
          "symbol": "string"
        },
        "tokenPrices": [
          {
            "currency": "string",
            "value": "string",
            "lastUpdatedAt": "string (ISO 8601)"
          }
        ],
        "error": "string | null"
      }
    ],
    "pageKey": "string | null"
  }
}
```

**Response fields:**

| Field | Type | Description |
|-------|------|-------------|
| `data.tokens[].address` | string | Wallet address queried |
| `data.tokens[].network` | string | Network identifier |
| `data.tokens[].tokenAddress` | string\|null | Token contract address; `null` for native tokens |
| `data.tokens[].tokenBalance` | string | Raw token balance as integer string (no decimal applied) |
| `data.tokens[].tokenMetadata.decimals` | integer | Decimal places for the token |
| `data.tokens[].tokenMetadata.logo` | string\|null | URL to token logo |
| `data.tokens[].tokenMetadata.name` | string | Token name (e.g., "Uniswap") |
| `data.tokens[].tokenMetadata.symbol` | string | Token symbol (e.g., "UNI") |
| `data.tokens[].tokenPrices[].currency` | string | Price currency code (e.g., "usd") |
| `data.tokens[].tokenPrices[].value` | string | Price as decimal string |
| `data.tokens[].tokenPrices[].lastUpdatedAt` | string | ISO 8601 timestamp of price |
| `data.tokens[].error` | string\|null | Error for this token entry, if any |
| `data.pageKey` | string\|null | Pagination cursor; `null` when no more pages |

**Pagination:** `pageKey`-based cursor pagination. Pass the returned `pageKey` as the `pageKey` body parameter in the next request. Repeat until `pageKey` is null.

**Primary key:** `(address, network, tokenAddress)` — note `tokenAddress` is `null` for native tokens, so use a composite that treats null as a sentinel (e.g., `"NATIVE"`).

**Ingestion type:** `snapshot` — balances represent the current state; there is no cursor to fetch only changed balances. Refresh on a schedule (e.g., hourly or daily per wallet).

**CU cost:** 360 CU per request.

---

### Table 2: `token_balances_by_wallet`

**Description:** Returns raw on-chain fungible token balances for one or more wallet addresses across networks. No metadata or price enrichment — lower cost than `tokens_by_wallet`.

**Endpoint:**
```
POST https://api.g.alchemy.com/data/v1/{apiKey}/assets/tokens/balances/by-address
```

**Request body parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `addresses` | array | Yes | — | Max 3 items. Each item has `address` (string) and `networks` (array, max 20 per address) |
| `addresses[].address` | string | Yes | — | Wallet address |
| `addresses[].networks` | array | Yes | — | Network identifiers |
| `includeNativeTokens` | boolean | No | `true` | Include native chain tokens |
| `includeErc20Tokens` | boolean | No | `true` | Include ERC-20 tokens |
| `pageKey` | string | No | — | Pagination cursor |

**Example request:**
```json
{
  "addresses": [
    {
      "address": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",
      "networks": ["eth-mainnet", "base-mainnet", "polygon-mainnet"]
    }
  ]
}
```

**Response schema (HTTP 200):**

```json
{
  "data": {
    "tokens": [
      {
        "network": "string",
        "address": "string",
        "tokenAddress": "string | null",
        "tokenBalance": "string"
      }
    ],
    "pageKey": "string | null"
  }
}
```

**Response fields:**

| Field | Type | Description |
|-------|------|-------------|
| `data.tokens[].network` | string | Network identifier |
| `data.tokens[].address` | string | Wallet address |
| `data.tokens[].tokenAddress` | string\|null | Token contract address; `null` for native token |
| `data.tokens[].tokenBalance` | string | Raw balance as integer string |
| `data.pageKey` | string\|null | Pagination cursor |

**Pagination:** Same `pageKey` cursor model as `tokens_by_wallet`.

**Primary key:** `(address, network, tokenAddress)`

**Ingestion type:** `snapshot`

**CU cost:** 200 CU per request.

---

### Table 3: `nft_collections_by_wallet`

**Description:** Returns NFT collections (contracts) held by wallet addresses across networks. One row per (wallet, network, contract). Includes OpenSea metadata for collections.

**Endpoint:**
```
POST https://api.g.alchemy.com/data/v1/{apiKey}/assets/nfts/contracts/by-address
```

**Request body parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `addresses` | array | Yes | — | Max 2 items. Each item has `address` (string) and `networks` (array, max 15) |
| `addresses[].address` | string | Yes | — | Wallet address |
| `addresses[].networks` | array | Yes | — | Network identifiers |
| `addresses[].excludeFilters` | array | No | — | Filter types to exclude: `SPAM`, `AIRDROPS` |
| `addresses[].includeFilters` | array | No | — | Filter types to include: `SPAM`, `AIRDROPS` |
| `addresses[].spamConfidenceLevel` | enum | No | — | `VERY_HIGH`, `HIGH`, `MEDIUM`, `LOW` |
| `withMetadata` | boolean | No | `true` | Include collection metadata |
| `pageKey` | string | No | — | Pagination cursor |
| `pageSize` | integer | No | 100 | Results per page (default 100) |
| `orderBy` | enum | No | — | `transferTime` |
| `sortOrder` | enum | No | — | `asc` or `desc` |

**Response schema (HTTP 200):**

```json
{
  "data": {
    "contracts": [
      {
        "network": "string",
        "address": "string",
        "contract": {
          "address": "string",
          "name": "string",
          "symbol": "string",
          "totalSupply": "string",
          "tokenType": "ERC721 | ERC1155 | NO_SUPPORTED_NFT_STANDARD | NOT_A_CONTRACT",
          "contractDeployer": "string",
          "deployedBlockNumber": "number",
          "openseaMetadata": {
            "floorPrice": "number | null",
            "collectionName": "string",
            "safelistRequestStatus": "string",
            "imageUrl": "string | null",
            "description": "string | null",
            "externalUrl": "string | null",
            "twitterUsername": "string | null",
            "discordUrl": "string | null",
            "bannerImageUrl": "string | null",
            "lastIngestedAt": "string (ISO 8601)"
          },
          "isSpam": "string",
          "spamClassifications": ["string"]
        }
      }
    ],
    "totalCount": "integer",
    "pageKey": "string | null"
  }
}
```

**Response fields:**

| Field | Type | Description |
|-------|------|-------------|
| `data.contracts[].network` | string | Network identifier |
| `data.contracts[].address` | string | Wallet address |
| `data.contracts[].contract.address` | string | NFT contract address |
| `data.contracts[].contract.name` | string | Contract name |
| `data.contracts[].contract.symbol` | string | Contract symbol |
| `data.contracts[].contract.totalSupply` | string | Total NFTs in collection |
| `data.contracts[].contract.tokenType` | string | Token standard enum |
| `data.contracts[].contract.contractDeployer` | string | Deployer address |
| `data.contracts[].contract.deployedBlockNumber` | number | Block number of deployment |
| `data.contracts[].contract.openseaMetadata.floorPrice` | number\|null | Floor price in ETH |
| `data.contracts[].contract.openseaMetadata.collectionName` | string | OpenSea collection name |
| `data.contracts[].contract.openseaMetadata.safelistRequestStatus` | string | OpenSea safelist status |
| `data.contracts[].contract.isSpam` | string | Spam classification result |
| `data.contracts[].contract.spamClassifications` | array | List of spam reasons |
| `data.totalCount` | integer | Total collections found |
| `data.pageKey` | string\|null | Pagination cursor |

**Pagination:** `pageKey` cursor; also supports `pageSize`.

**Primary key:** `(address, network, contract.address)`

**Ingestion type:** `snapshot`

**CU cost:** 600 CU per request.

---

### Table 4: `wallet_transactions`

**Description:** Full transaction history (external + internal) for wallet addresses. Returns transaction metadata and internal call traces.

**Status:** **Beta** — Only ETH mainnet and Base mainnet are currently supported. Alchemy has indicated this endpoint may be superseded by `alchemy_getAssetTransfers`. Plan implementation accordingly.

**Endpoint:**
```
POST https://api.g.alchemy.com/data/v1/{apiKey}/transactions/history/by-address
```

**Request body parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `addresses` | array | Yes | — | **Max 1 item** (despite plural). Each has `address` (string) and `networks` (array, max 2) |
| `addresses[].address` | string | Yes | — | Wallet address |
| `addresses[].networks` | array | Yes | — | Only `eth-mainnet` and `base-mainnet` accepted |
| `limit` | integer | No | 25 | Results per page (max 50) |
| `before` | string | No | — | Cursor pointing to the previous result set |
| `after` | string | No | — | Cursor pointing to the end of the current result set (next page) |
| `pageKey` | string | No | — | Alternative pagination cursor |

**Response schema (HTTP 200):**

```json
{
  "before": "string | null",
  "after": "string | null",
  "totalCount": "integer",
  "transactions": [
    {
      "network": "string",
      "hash": "string",
      "timeStamp": "string (ISO 8601)",
      "blockNumber": "integer",
      "blockHash": "string",
      "nonce": "integer",
      "transactionIndex": "integer",
      "fromAddress": "string",
      "toAddress": "string | null",
      "contractAddress": "string | null",
      "value": "string",
      "cumulativeGasUsed": "string",
      "effectiveGasPrice": "string",
      "gasUsed": "string",
      "logs": [
        {
          "contractAddress": "string",
          "logIndex": "integer",
          "data": "string",
          "removed": "boolean",
          "topics": ["string"]
        }
      ],
      "internalTxns": [
        {
          "type": "string",
          "from": "string",
          "to": "string",
          "value": "string",
          "gas": "string",
          "gasUsed": "string",
          "input": "string",
          "output": "string",
          "error": "string | null"
        }
      ]
    }
  ]
}
```

**Response fields:**

| Field | Type | Description |
|-------|------|-------------|
| `before` | string\|null | Cursor pointing to the result set before the current page |
| `after` | string\|null | Cursor for the next page (pass as `after` in next request) |
| `totalCount` | integer | Total transactions in result set |
| `transactions[].hash` | string | Transaction hash (unique identifier) |
| `transactions[].timeStamp` | string | ISO 8601 block timestamp |
| `transactions[].blockNumber` | integer | Block number |
| `transactions[].blockHash` | string | Block hash |
| `transactions[].nonce` | integer | Sender nonce |
| `transactions[].transactionIndex` | integer | Position in block |
| `transactions[].fromAddress` | string | Sender address |
| `transactions[].toAddress` | string\|null | Recipient address (`null` for contract creation) |
| `transactions[].contractAddress` | string\|null | Created contract address (if contract creation tx) |
| `transactions[].value` | string | Native token value transferred (wei) |
| `transactions[].cumulativeGasUsed` | string | Cumulative gas used in block up to this tx |
| `transactions[].effectiveGasPrice` | string | Gas price paid (wei) |
| `transactions[].gasUsed` | string | Gas consumed by this transaction |
| `transactions[].logs[].contractAddress` | string | Emitting contract |
| `transactions[].logs[].logIndex` | integer | Log position in block |
| `transactions[].logs[].data` | string | Hex-encoded log data |
| `transactions[].logs[].removed` | boolean | Whether log was removed in a reorg |
| `transactions[].logs[].topics` | array | Log topics |
| `transactions[].internalTxns[].type` | string | Internal call type (CALL, DELEGATECALL, etc.) |
| `transactions[].internalTxns[].from` | string | Caller address |
| `transactions[].internalTxns[].to` | string | Callee address |
| `transactions[].internalTxns[].value` | string | ETH value of internal call |

**Pagination:** Dual-cursor model. Pass the `after` cursor from the previous response as `after` in the next request to advance. `before` can be used for reverse pagination.

**Primary key:** `(hash, network)` — transaction hashes are unique per network.

**Cursor field:** `timeStamp` or `blockNumber` — for incremental appends, track the highest `blockNumber` seen and use block-based filtering on subsequent syncs (note: the endpoint itself does not accept a `fromBlock` parameter; incremental sync requires fetching forward and deduplicating by `hash`).

**Ingestion type:** `append` — transactions are immutable once finalized. New syncs should fetch only new transactions (cursor by `blockNumber`).

**CU cost:** 1,000 CU per request.

**Caveat:** Alchemy documentation notes a planned deprecation in favor of `alchemy_getAssetTransfers`. Monitor for updates.

---

### Table 5: `token_prices_by_symbol`

**Description:** Current prices for multiple tokens identified by symbol. Returns USD (and optionally other currency) prices with last-updated timestamps.

**Endpoint:**
```
GET https://api.g.alchemy.com/prices/v1/{apiKey}/tokens/by-symbol
```

**Query parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `symbols` | string[] | Yes | Array of token symbols, max 25. Format: `symbols=ETH&symbols=BTC` or `symbols=[ETH,BTC]` |

**Example request:**
```
GET https://api.g.alchemy.com/prices/v1/YOUR_API_KEY/tokens/by-symbol?symbols=ETH&symbols=BTC&symbols=USDC
```

**Response schema (HTTP 200):**

```json
{
  "data": [
    {
      "symbol": "string",
      "prices": [
        {
          "currency": "string",
          "value": "string",
          "lastUpdatedAt": "string (ISO 8601)"
        }
      ],
      "error": "string | null"
    }
  ]
}
```

**Response fields:**

| Field | Type | Description |
|-------|------|-------------|
| `data[].symbol` | string | Token symbol queried |
| `data[].prices[].currency` | string | Currency code (e.g., "usd") |
| `data[].prices[].value` | string | Price as decimal string |
| `data[].prices[].lastUpdatedAt` | string | ISO 8601 timestamp of price feed update |
| `data[].error` | string\|null | Error if symbol not found or lookup failed |

**Pagination:** No pagination — single response for all requested symbols.

**Primary key:** `(symbol, currency)` for a point-in-time snapshot. For time-series use, add `lastUpdatedAt` to the key.

**Ingestion type:** `snapshot` — always returns the latest price. Run on a schedule to build a price history.

**CU cost:** 40 CU per request (regardless of number of symbols, up to 25).

---

### Table 6: `token_prices_by_address`

**Description:** Current prices for multiple tokens identified by network + contract address pairs. Useful when a token's symbol is ambiguous across chains.

**Endpoint:**
```
POST https://api.g.alchemy.com/prices/v1/{apiKey}/tokens/by-address
```

**Request body parameters:**

| Parameter | Type | Required | Constraints | Description |
|-----------|------|----------|-------------|-------------|
| `addresses` | array | Yes | Min 1, max 25 items, max 3 networks | Token network+address pairs |
| `addresses[].network` | string | Yes | — | Network identifier (e.g., `eth-mainnet`) |
| `addresses[].address` | string | Yes | — | Token contract address |

**Example request:**
```json
{
  "addresses": [
    { "network": "eth-mainnet", "address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48" },
    { "network": "eth-mainnet", "address": "0x6B175474E89094C44Da98b954EedeAC495271d0F" }
  ]
}
```

**Response schema (HTTP 200):**

```json
{
  "data": [
    {
      "network": "string",
      "address": "string",
      "prices": [
        {
          "currency": "string",
          "value": "string",
          "lastUpdatedAt": "string (ISO 8601)"
        }
      ],
      "error": "string | null"
    }
  ]
}
```

**Response fields:**

| Field | Type | Description |
|-------|------|-------------|
| `data[].network` | string | Network identifier |
| `data[].address` | string | Token contract address |
| `data[].prices[].currency` | string | Currency code |
| `data[].prices[].value` | string | Price as decimal string |
| `data[].prices[].lastUpdatedAt` | string | ISO 8601 price timestamp |
| `data[].error` | string\|null | Error if not found |

**Pagination:** No pagination.

**Primary key:** `(network, address, currency)`

**Ingestion type:** `snapshot`

**CU cost:** 40 CU per request.

---

### Table 7: `token_prices_historical`

**Description:** Historical price time series for a single token. Supports 5-minute, 1-hour, and 1-day intervals. Optionally includes market cap and total volume.

**Endpoint:**
```
POST https://api.g.alchemy.com/prices/v1/{apiKey}/tokens/historical
```

**Request body parameters (two mutually exclusive schemas):**

**Option A — by symbol:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `symbol` | string | Yes | Token symbol (e.g., `ETH`, `BTC`, `USDC`) |
| `startTime` | string\|integer | Yes | Start of range (ISO 8601 or Unix timestamp) |
| `endTime` | string\|integer | Yes | End of range (ISO 8601 or Unix timestamp) |
| `interval` | string | No | `"5m"`, `"1h"`, or `"1d"` (default `"1d"`) |
| `withMarketData` | boolean | No | Include `marketCap` and `totalVolume` (default `false`) |

**Option B — by network + address:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `network` | string | Yes | Network identifier |
| `address` | string | Yes | Token contract address |
| `startTime` | string\|integer | Yes | Start of range (ISO 8601 or Unix timestamp) |
| `endTime` | string\|integer | Yes | End of range (ISO 8601 or Unix timestamp) |
| `interval` | string | No | `"5m"`, `"1h"`, or `"1d"` (default `"1d"`) |
| `withMarketData` | boolean | No | Include `marketCap` and `totalVolume` (default `false`) |

**Time range constraints:**

| Interval | Maximum Range |
|----------|---------------|
| `"5m"` | 7 days |
| `"1h"` | 30 days |
| `"1d"` | 365 days (1 year) |

**Example request (by symbol, daily for 30 days):**
```json
{
  "symbol": "ETH",
  "startTime": "2025-01-01T00:00:00Z",
  "endTime": "2025-01-31T23:59:59Z",
  "interval": "1d",
  "withMarketData": true
}
```

**Response schema — symbol-based (HTTP 200):**
```json
{
  "symbol": "string",
  "currency": "string",
  "data": [
    {
      "value": "string",
      "timestamp": "string (ISO 8601)",
      "marketCap": "string | null",
      "totalVolume": "string | null"
    }
  ]
}
```

**Response schema — network/address-based (HTTP 200):**
```json
{
  "network": "string",
  "address": "string",
  "currency": "string",
  "data": [
    {
      "value": "string",
      "timestamp": "string (ISO 8601)",
      "marketCap": "string | null",
      "totalVolume": "string | null"
    }
  ]
}
```

**Response fields:**

| Field | Type | Description |
|-------|------|-------------|
| `symbol` or (`network` + `address`) | string | Token identifier echoed from request |
| `currency` | string | Currency of prices (e.g., "usd") |
| `data[].value` | string | Price at this interval |
| `data[].timestamp` | string | ISO 8601 start of interval |
| `data[].marketCap` | string\|null | Market cap (only if `withMarketData: true`) |
| `data[].totalVolume` | string\|null | 24h volume (only if `withMarketData: true`) |

**Pagination:** No pagination — all data points for the requested range returned in one response. For large historical loads, batch requests by date range.

**Primary key:** `(symbol OR (network, address), currency, timestamp)`

**Cursor field:** `timestamp` — for incremental sync, store the last `endTime` and use it as `startTime` for the next batch.

**Ingestion type:** `append` — historical price points are immutable once published (though Alchemy may backfill corrections). Sync new intervals going forward.

**CU cost:** 40 CU per request.

---

### Table 8: `nfts_by_wallet`

**Description:** All individual NFTs (ERC-721 and ERC-1155) owned by a wallet address, with full token metadata. One row per (wallet, network, contract, tokenId).

**Endpoint:**
```
GET https://{network}.g.alchemy.com/nft/v3/{apiKey}/getNFTsForOwner
```

**Note on URL shape:** Unlike Portfolio API endpoints (which use `api.g.alchemy.com`), NFT v3 endpoints use the **network as a URL subdomain**. The network is specified in the URL, not in the request body.

**Query parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `owner` | string | Yes | — | Wallet address or ENS name (ENS only on ETH mainnet) |
| `contractAddresses[]` | string[] | No | — | Filter to specific contracts (max 45) |
| `withMetadata` | boolean | No | `true` | Include NFT metadata |
| `orderBy` | enum | No | — | `transferTime` — sort by acquisition time, newest first |
| `excludeFilters[]` | enum[] | No | — | `SPAM`, `AIRDROPS` |
| `includeFilters[]` | enum[] | No | — | `SPAM`, `AIRDROPS` |
| `spamConfidenceLevel` | enum | No | — | `VERY_HIGH`, `HIGH`, `MEDIUM`, `LOW` |
| `tokenUriTimeoutInMs` | integer | No | — | Metadata fetch timeout in ms (0 = cache only) |
| `pageKey` | string | No | — | Pagination cursor |
| `pageSize` | integer | No | 100 | Results per page (max 100) |

**Example request:**
```
GET https://eth-mainnet.g.alchemy.com/nft/v3/YOUR_API_KEY/getNFTsForOwner?owner=0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045&pageSize=100&excludeFilters[]=SPAM
```

**Response schema (HTTP 200):**

```json
{
  "ownedNfts": [
    {
      "contract": {
        "address": "string",
        "name": "string",
        "symbol": "string",
        "totalSupply": "string",
        "tokenType": "ERC721 | ERC1155 | NO_SUPPORTED_NFT_STANDARD | NOT_A_CONTRACT",
        "contractDeployer": "string",
        "deployedBlockNumber": "number",
        "openSeaMetadata": {
          "floorPrice": "number | null",
          "collectionName": "string",
          "safelistRequestStatus": "string",
          "imageUrl": "string | null",
          "description": "string | null",
          "externalUrl": "string | null",
          "twitterUsername": "string | null",
          "discordUrl": "string | null",
          "bannerImageUrl": "string | null",
          "lastIngestedAt": "string (ISO 8601)"
        },
        "isSpam": "string",
        "spamClassifications": ["string"]
      },
      "tokenId": "string",
      "tokenType": "string",
      "name": "string",
      "description": "string",
      "image": {
        "cachedUrl": "string | null",
        "thumbnailUrl": "string | null",
        "pngUrl": "string | null",
        "contentType": "string | null",
        "size": "integer | null",
        "originalUrl": "string | null"
      },
      "raw": {
        "tokenUri": "string | null",
        "metadata": {
          "image": "string | null",
          "name": "string | null",
          "description": "string | null",
          "attributes": [
            { "value": "string | integer", "trait_type": "string" }
          ],
          "external_url": "string | null"
        },
        "error": "string | null"
      },
      "collection": {
        "name": "string | null",
        "slug": "string | null",
        "externalUrl": "string | null",
        "bannerImageUrl": "string | null"
      },
      "tokenUri": "string | null",
      "timeLastUpdated": "string (ISO 8601)",
      "acquiredAt": {
        "blockTimestamp": "string | null",
        "blockNumber": "integer | null"
      },
      "animation": {
        "cachedUrl": "string | null",
        "contentType": "string | null",
        "size": "integer | null",
        "originalUrl": "string | null"
      },
      "mint": {
        "mintAddress": "string | null",
        "blockNumber": "integer | null",
        "timestamp": "string | null",
        "transactionHash": "string | null"
      },
      "owners": ["string"],
      "balance": "string"
    }
  ],
  "totalCount": "integer",
  "pageKey": "string | null",
  "validAt": {
    "blockNumber": "integer",
    "blockHash": "string",
    "blockTimestamp": "string (ISO 8601)"
  }
}
```

**Response fields (key fields):**

| Field | Type | Description |
|-------|------|-------------|
| `ownedNfts[].contract.address` | string | NFT contract address |
| `ownedNfts[].contract.tokenType` | string | `ERC721`, `ERC1155`, etc. |
| `ownedNfts[].tokenId` | string | Token ID (decimal or hex string) |
| `ownedNfts[].tokenType` | string | Token standard for this specific token |
| `ownedNfts[].name` | string | NFT name |
| `ownedNfts[].description` | string | NFT description |
| `ownedNfts[].image.cachedUrl` | string\|null | Alchemy-cached image URL |
| `ownedNfts[].raw.metadata.attributes` | array | Trait list `[{trait_type, value}]` |
| `ownedNfts[].timeLastUpdated` | string | When Alchemy last updated metadata |
| `ownedNfts[].acquiredAt.blockTimestamp` | string\|null | When wallet acquired this NFT |
| `ownedNfts[].acquiredAt.blockNumber` | integer\|null | Block of acquisition |
| `ownedNfts[].mint.transactionHash` | string\|null | Mint transaction hash |
| `ownedNfts[].balance` | string | Token balance (ERC-1155 may be > 1) |
| `totalCount` | integer | Total NFTs owned |
| `pageKey` | string\|null | Pagination cursor |
| `validAt.blockNumber` | integer | Block at which this snapshot is valid |

**Pagination:** `pageKey` cursor; pass in `pageKey` query param. Max 100 per page.

**Primary key:** `(owner, network, contract.address, tokenId)` — `owner` must be tracked by the connector since it is not in the response body.

**Ingestion type:** `snapshot` — reflects current ownership. `acquiredAt.blockNumber` can serve as a loose cursor, but ownership changes (transfers out) are not reflected without a full re-snapshot.

**CU cost:** 480 CU per request.

---

### Table 9: `nft_metadata`

**Description:** Metadata for a specific NFT token (name, description, image, traits). Use when you have a contract address + token ID and need rich token details.

**Endpoint:**
```
GET https://{network}.g.alchemy.com/nft/v3/{apiKey}/getNFTMetadata
```

**Query parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `contractAddress` | string | Yes | — | NFT contract address |
| `tokenId` | string | Yes | — | Token ID (hex or decimal string) |
| `tokenType` | string | No | — | `ERC721` or `ERC1155`; improves response time if known |
| `tokenUriTimeoutInMs` | integer | No | — | Metadata fetch timeout ms; `0` for cache-only |
| `refreshCache` | boolean | No | `false` | Force a metadata cache refresh |

**Example request:**
```
GET https://eth-mainnet.g.alchemy.com/nft/v3/YOUR_API_KEY/getNFTMetadata?contractAddress=0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D&tokenId=1
```

**Response schema (HTTP 200):**

Same structure as a single item from `nfts_by_wallet.ownedNfts[]` — see that table for the full field list. Key differences: no `balance`, no `acquiredAt` (those are ownership-specific fields).

```json
{
  "contract": { "address": "string", "name": "string", "symbol": "string", "totalSupply": "string", "tokenType": "string", "contractDeployer": "string", "deployedBlockNumber": "number", "openSeaMetadata": {}, "isSpam": "string", "spamClassifications": [] },
  "tokenId": "string",
  "tokenType": "string",
  "name": "string",
  "description": "string",
  "image": { "cachedUrl": "string", "thumbnailUrl": "string", "pngUrl": "string", "contentType": "string", "size": "integer", "originalUrl": "string" },
  "raw": { "tokenUri": "string", "metadata": { "image": "string", "name": "string", "description": "string", "attributes": [], "external_url": "string" }, "error": "string | null" },
  "collection": { "name": "string", "slug": "string", "externalUrl": "string", "bannerImageUrl": "string" },
  "tokenUri": "string",
  "timeLastUpdated": "string (ISO 8601)",
  "animation": { "cachedUrl": "string", "contentType": "string", "size": "integer", "originalUrl": "string" },
  "mint": { "mintAddress": "string", "blockNumber": "integer", "timestamp": "string", "transactionHash": "string" },
  "owners": ["string"]
}
```

**Response fields (key fields):**

| Field | Type | Description |
|-------|------|-------------|
| `contract.address` | string | NFT contract address |
| `tokenId` | string | Token ID |
| `name` | string | NFT name |
| `description` | string | NFT description |
| `image.cachedUrl` | string\|null | Alchemy-cached image |
| `raw.metadata.attributes` | array | `[{trait_type: string, value: string|integer}]` |
| `timeLastUpdated` | string | Metadata cache freshness timestamp |
| `mint.transactionHash` | string\|null | Mint tx hash |
| `owners` | array | Current owner addresses |

**Pagination:** No pagination — single token per request.

**Primary key:** `(contract.address, tokenId, network)` — `network` tracked externally.

**Ingestion type:** `snapshot` — metadata can change if the NFT creator updates the URI. Recommended: re-sync periodically or on-demand. Use `refreshCache: true` only when freshness is critical (incurs extra latency).

**CU cost:** 80 CU per request.

---

### Table 10: `nft_contract_metadata`

**Description:** Collection-level metadata for an NFT contract: name, symbol, total supply, token standard, deployer, and OpenSea marketplace metadata.

**Endpoint:**
```
GET https://{network}.g.alchemy.com/nft/v3/{apiKey}/getContractMetadata
```

**Query parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `contractAddress` | string | Yes | NFT contract address |

**Example request:**
```
GET https://eth-mainnet.g.alchemy.com/nft/v3/YOUR_API_KEY/getContractMetadata?contractAddress=0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D
```

**Response schema (HTTP 200):**

```json
{
  "address": "string",
  "name": "string",
  "symbol": "string",
  "totalSupply": "string",
  "tokenType": "ERC721 | ERC1155 | NO_SUPPORTED_NFT_STANDARD | NOT_A_CONTRACT",
  "contractDeployer": "string",
  "deployedBlockNumber": "number",
  "openseaMetadata": {
    "floorPrice": "number | null",
    "collectionName": "string",
    "safelistRequestStatus": "string",
    "imageUrl": "string | null",
    "description": "string | null",
    "externalUrl": "string | null",
    "twitterUsername": "string | null",
    "discordUrl": "string | null",
    "bannerImageUrl": "string | null",
    "lastIngestedAt": "string (ISO 8601)"
  }
}
```

**Response fields:**

| Field | Type | Description |
|-------|------|-------------|
| `address` | string | Contract address |
| `name` | string | Contract/collection name |
| `symbol` | string | Symbol abbreviation |
| `totalSupply` | string | Total minted tokens |
| `tokenType` | string | `ERC721`, `ERC1155`, `NO_SUPPORTED_NFT_STANDARD`, or `NOT_A_CONTRACT` |
| `contractDeployer` | string | Deployer wallet address |
| `deployedBlockNumber` | number | Deployment block number |
| `openseaMetadata.floorPrice` | number\|null | Floor price in ETH (ETH/Polygon only) |
| `openseaMetadata.collectionName` | string | OpenSea collection name |
| `openseaMetadata.safelistRequestStatus` | string | OpenSea safelist status |
| `openseaMetadata.lastIngestedAt` | string | ISO 8601 time OpenSea data was last refreshed |

**Note:** `openseaMetadata` is only populated for Ethereum and Polygon networks.

**Pagination:** No pagination — single contract per request.

**Primary key:** `(address, network)` — `network` tracked externally.

**Ingestion type:** `snapshot` — contract metadata (name, symbol, total supply) can change via contract upgrades. Periodic re-sync recommended.

**CU cost:** 160 CU per request.

---

### Table 11: `nft_floor_prices`

**Description:** Current floor prices for an NFT collection by marketplace (OpenSea, LooksRare). Returns lowest listing price per marketplace in ETH.

**Endpoint:**
```
GET https://eth-mainnet.g.alchemy.com/nft/v3/{apiKey}/getFloorPrice
```

**Network restriction:** Only available on **Ethereum mainnet**. OpenSea and LooksRare are the only supported marketplaces.

**Query parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `contractAddress` | string | Yes | NFT contract address |
| `collectionSlug` | string | No | OpenSea slug for the collection |

**Example request:**
```
GET https://eth-mainnet.g.alchemy.com/nft/v3/YOUR_API_KEY/getFloorPrice?contractAddress=0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D
```

**Response schema (HTTP 200):**

The response is a dynamic object where each key is a marketplace name (camelCase). Each marketplace entry has the same structure:

```json
{
  "openSea": {
    "floorPrice": "number",
    "priceCurrency": "ETH",
    "collectionUrl": "string",
    "retrievedAt": "string (UTC timestamp)",
    "error": "string | null"
  },
  "looksRare": {
    "floorPrice": "number",
    "priceCurrency": "ETH",
    "collectionUrl": "string",
    "retrievedAt": "string (UTC timestamp)",
    "error": "string | null"
  }
}
```

**Response fields:**

| Field | Type | Description |
|-------|------|-------------|
| `{marketplace}.floorPrice` | number | Floor price in ETH |
| `{marketplace}.priceCurrency` | string | Currency, always `"ETH"` |
| `{marketplace}.collectionUrl` | string | URL to collection page on marketplace |
| `{marketplace}.retrievedAt` | string | UTC timestamp when price was fetched |
| `{marketplace}.error` | string\|null | `"unable to fetch floor price"` if retrieval failed |

**Caching:** Floor prices are cached for ~5 minutes. Requesting more frequently than that returns the same cached value.

**Pagination:** No pagination — single contract per request.

**Primary key:** `(contractAddress, marketplace)` for a point-in-time snapshot. Add `retrievedAt` for a time-series.

**Ingestion type:** `snapshot` — poll on schedule (no finer than 5-minute intervals due to cache). ETH-only limitation means this is not useful for Polygon/Base NFT collections.

**CU cost:** 80 CU per request.

---

## Get Object Primary Keys

| Table | Primary Key |
|-------|-------------|
| `tokens_by_wallet` | `(address, network, tokenAddress)` — use `"NATIVE"` for null `tokenAddress` |
| `token_balances_by_wallet` | `(address, network, tokenAddress)` |
| `nft_collections_by_wallet` | `(address, network, contract.address)` |
| `wallet_transactions` | `(hash, network)` |
| `token_prices_by_symbol` | `(symbol, currency, lastUpdatedAt)` |
| `token_prices_by_address` | `(network, address, currency)` |
| `token_prices_historical` | `(symbol OR (network, address), currency, timestamp)` |
| `nfts_by_wallet` | `(owner, network, contract.address, tokenId)` |
| `nft_metadata` | `(contract.address, tokenId, network)` |
| `nft_contract_metadata` | `(address, network)` |
| `nft_floor_prices` | `(contractAddress, marketplace, retrievedAt)` |

**Note:** `owner` and `network` context for NFT v3 tables must be tracked by the connector since they do not appear in response bodies.

---

## Object Ingestion Types

| Table | Ingestion Type | Cursor Field | Notes |
|-------|---------------|--------------|-------|
| `tokens_by_wallet` | `snapshot` | — | Full re-fetch per sync; balances are current state only |
| `token_balances_by_wallet` | `snapshot` | — | Full re-fetch per sync |
| `nft_collections_by_wallet` | `snapshot` | — | Full re-fetch per sync |
| `wallet_transactions` | `append` | `blockNumber` | Track max `blockNumber` seen; fetch forward on next sync |
| `token_prices_by_symbol` | `snapshot` | — | Poll on schedule; append rows with timestamp for history |
| `token_prices_by_address` | `snapshot` | — | Poll on schedule |
| `token_prices_historical` | `append` | `timestamp` | Track last synced `endTime`; request forward intervals |
| `nfts_by_wallet` | `snapshot` | — | NFT ownership is current-state; no change feed |
| `nft_metadata` | `snapshot` | — | Metadata can change; periodic re-sync |
| `nft_contract_metadata` | `snapshot` | — | Contract metadata stable; sync less frequently |
| `nft_floor_prices` | `snapshot` | — | Poll no more than every 5 minutes (cache TTL) |

**True CDC is not available** for any of these endpoints. Alchemy does not provide a change feed or webhook-style stream suitable for CDC in this API surface. The Transfers API (`alchemy_getAssetTransfers`) provides an event-stream of token transfers but is not covered in this document.

---

## Field Type Mapping

| API Type | Spark/Python Type | Notes |
|----------|------------------|-------|
| `string` (general) | `StringType` | — |
| `integer` / `number` | `LongType` | Block numbers, log indices; use `LongType` for safety |
| `number` (float) | `DoubleType` | Floor prices, price values when explicitly float |
| `string` (numeric) | `StringType` or `DecimalType(38,18)` | Token balances and price `value` fields are returned as strings to preserve precision; decode with Python `Decimal` before storing |
| `string (ISO 8601)` | `TimestampType` | Parse with `datetime.fromisoformat()` |
| `boolean` | `BooleanType` | — |
| `array` | `ArrayType(...)` | Flatten or store as JSON string depending on analytics use case |
| `object` | `StructType` or `StringType` (JSON) | Nested objects like `raw.metadata` are best stored as JSON strings |
| `null` | `NullType` / nullable column | All nullable fields should use nullable=True |

**Special cases:**
- `tokenBalance` (string): Raw on-chain integer with no decimal applied. To get human-readable balance: `Decimal(tokenBalance) / Decimal(10 ** decimals)`.
- `tokenId` (string): May be decimal or hex string depending on the NFT contract. Normalize to decimal for consistency.
- `value` (string in transactions): Wei amount. 1 ETH = 1e18 wei. Use Python `Decimal` for conversion.
- `blockNumber` (integer): Fits in 32-bit integer today but use 64-bit (`LongType`) for future-proofing.

---

## Known Quirks and Caveats

1. **API key in URL path**: The API key is in the URL, not an `Authorization` header. This is unusual and means the key is visible in logs and proxies. Use Alchemy's Dashboard allow-list feature to restrict key usage by IP or referrer when possible.

2. **Two URL shapes**: Portfolio and Prices APIs use `api.g.alchemy.com` with network in the request body. NFT v3 API uses `{network}.g.alchemy.com` with network in the subdomain. The connector must handle both shapes.

3. **`wallet_transactions` is Beta**: Currently limited to ETH mainnet and Base mainnet. Alchemy has suggested it may be deprecated in favor of `alchemy_getAssetTransfers`. Treat as unstable; add feature flag for enabling/disabling.

4. **Portfolio API address limits**: `tokens_by_wallet` accepts max 2 addresses, max 5 networks per address. `token_balances_by_wallet` accepts max 3 addresses, max 20 networks. `wallet_transactions` accepts max 1 address, max 2 networks. These limits require the connector to batch wallet lists.

5. **`getFloorPrice` is ETH-only**: Floor prices are only available for Ethereum mainnet collections and only from OpenSea and LooksRare. No floor price data for Polygon, Base, or Arbitrum NFTs.

6. **`openseaMetadata` is ETH/Polygon only**: The `openseaMetadata` field in `getContractMetadata` and NFT responses is only populated for ETH and Polygon networks.

7. **`tokenBalance` is raw**: All balance values are raw on-chain integers (no decimal applied). The `decimals` field from `tokenMetadata` must be used to compute human-readable amounts.

8. **Solana support**: Tokens and token balances endpoints support Solana (`sol-mainnet`). NFT endpoints do not. Solana response shapes may differ slightly (e.g., use of `SPL` token type vs. `ERC-20`). Test Solana paths separately.

9. **Price `value` as string**: All price values are returned as strings (e.g., `"1842.50"`) to preserve floating-point precision. Parse with `Decimal` not `float`.

10. **`pageKey` vs `before`/`after` pagination**: Most endpoints use `pageKey`. The `wallet_transactions` endpoint uses a dual-cursor model (`before`/`after`). Do not confuse them.

11. **NFT `tokenId` format inconsistency**: Some contracts return tokenId as a decimal string, others as a hex string with `0x` prefix. Normalize to decimal for consistent primary keys.

12. **`getFloorPrice` 5-minute cache**: Polling more frequently than 5 minutes wastes CUs without returning fresher data.

---

## Deferred Tables

The following tables were considered but deferred from this initial batch due to high volume, high CU cost, or atypical patterns:

| Table | Endpoint | Reason for Deferral |
|-------|----------|---------------------|
| `nfts_for_contract` | `GET /nft/v3/{key}/getNFTsForContract` | Enumerates all tokens in a collection; large collections (10k+ NFTs) are extremely expensive (80–160k+ CU). Requires careful rate-limiting strategy and is likely only useful for specific analytics use cases, not general-purpose portfolio sync. |
| `nft_sales` | `GET /nft/v3/{key}/getNFTSales` | NFT sales history across marketplaces. High complexity: requires filtering by marketplace, time range, and handling the volume of sales data. Append-only event stream — good candidate for a future batch. |
| `asset_transfers` | JSON-RPC `alchemy_getAssetTransfers` | Low-level ERC-20/ERC-721/ERC-1155 transfer events. Very high volume, different pagination model (block range + `pageKey`), and JSON-RPC (not REST). Represents a separate connector pattern. |
| `nft_owners` | `GET /nft/v3/{key}/getOwnersForNFT` | Returns all owners of a specific token (useful for ERC-1155 fractional ownership). Low-priority for initial implementation. |

---

## Research Log

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|-------------|-----|----------------|------------|-------------------|
| Official Docs | https://www.alchemy.com/docs/reference/portfolio-apis | 2026-05-11 | High | Portfolio API overview, endpoint list |
| Official Docs | https://www.alchemy.com/docs/data/portfolio-apis/portfolio-api-endpoints/portfolio-api-endpoints/get-tokens-by-address | 2026-05-11 | High | `tokens_by_wallet` URL, body params, response schema, pagination |
| Official Docs | https://www.alchemy.com/docs/data/portfolio-apis/portfolio-api-endpoints/portfolio-api-endpoints/get-token-balances-by-address | 2026-05-11 | High | `token_balances_by_wallet` URL, params, response schema |
| Official Docs | https://www.alchemy.com/docs/data/portfolio-apis/portfolio-api-endpoints/portfolio-api-endpoints/get-nft-contracts-by-address | 2026-05-11 | High | `nft_collections_by_wallet` URL, body params, full response schema |
| Official Docs | https://www.alchemy.com/docs/data/portfolio-apis/portfolio-api-endpoints/portfolio-api-endpoints/get-transaction-history-by-address | 2026-05-11 | High | `wallet_transactions` URL, params, response schema, beta status, dual-cursor pagination |
| Official Docs | https://www.alchemy.com/docs/data/prices-api/prices-api-endpoints/prices-api-endpoints/get-token-prices-by-symbol | 2026-05-11 | High | `token_prices_by_symbol` URL, query params, response schema |
| Official Docs | https://www.alchemy.com/docs/data/prices-api/prices-api-endpoints/prices-api-endpoints/get-token-prices-by-address | 2026-05-11 | High | `token_prices_by_address` URL, body params, response schema |
| Official Docs | https://www.alchemy.com/docs/data/prices-api/prices-api-endpoints/prices-api-endpoints/get-historical-token-prices | 2026-05-11 | High | `token_prices_historical` URL, params, response schema, time range constraints |
| Official Docs | https://www.alchemy.com/docs/reference/nft-api-endpoints/nft-api-endpoints/nft-ownership-endpoints/get-nf-ts-for-owner-v-3 | 2026-05-11 | High | `nfts_by_wallet` URL, all params, full response schema, pageKey pagination |
| Official Docs | https://www.alchemy.com/docs/data/nft-api/api-reference/nft-metadata-endpoints/get-nft-metadata-v-3 | 2026-05-11 | High | `nft_metadata` URL, params, full response schema |
| Official Docs | https://www.alchemy.com/docs/reference/nft-api-endpoints/nft-api-endpoints/nft-metadata-endpoints/get-contract-metadata-v-3 | 2026-05-11 | High | `nft_contract_metadata` URL, params, response schema |
| Official Docs | https://www.alchemy.com/docs/reference/nft-api-endpoints/nft-api-endpoints/nft-sales-endpoints/get-floor-price-v-3 | 2026-05-11 | High | `nft_floor_prices` URL, params, response schema, ETH-only restriction, cache TTL |
| Official Docs | https://www.alchemy.com/docs/reference/compute-unit-costs | 2026-05-11 | High | CU costs per endpoint for all three API families |
| Official Docs | https://www.alchemy.com/docs/reference/throughput | 2026-05-11 | High | CUPs limits per plan, token-bucket algorithm, 429 retry semantics |
| Official Docs | https://www.alchemy.com/docs/reference/pricing-plans | 2026-05-11 | High | Monthly CU limits: Free=30M, PAYG=metered, Enterprise=custom |
| Official Docs | https://www.alchemy.com/docs/reference/nft-api-quickstart | 2026-05-11 | High | NFT v3 base URL pattern, auth method |
| Web Search | Alchemy Portfolio API endpoints documentation 2025 | 2026-05-11 | Medium | Confirmed multi-chain nature, Solana support for token endpoints |
