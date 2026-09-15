# Lakeflow Alchemy Community Connector

Ingest blockchain data from the **Alchemy Portfolio**, **Prices**, and **NFT v3** APIs into Databricks Delta tables via Lakeflow Connect. The connector exposes 11 tables covering wallet token balances, NFT holdings, current and historical token prices, transaction history, and NFT collection metadata across Ethereum, EVM L2s (Polygon, Arbitrum, Optimism, Base), and Solana (token endpoints only).

## Prerequisites

- **Alchemy account**: a free account at [alchemy.com](https://www.alchemy.com). The free plan grants 30M monthly Compute Units (CUs) and 500 CU/s of throughput — sufficient for development and many production workloads.
- **Alchemy API key**: created in the Alchemy Dashboard under an "App". The same key works across all three API families (Portfolio, Prices, NFT v3).
- **Network access**: the environment running the connector must be able to reach `https://api.g.alchemy.com` and `https://{network}.g.alchemy.com` (e.g. `eth-mainnet.g.alchemy.com`).
- **Lakeflow / Databricks environment**: a workspace where you can register a Lakeflow community connector and run ingestion pipelines.

## Setup

### Required Connection Parameters

| Name | Type | Required | Description | Example |
|------|------|----------|-------------|---------|
| `api_key` | string (secret) | yes | Alchemy API key for authenticating requests. Obtain from the [Alchemy Dashboard](https://dashboard.alchemy.com). **The key appears in the URL path** — store it as a secret and apply per-key IP / referrer allowlists in the Dashboard. | `alch_AbC123…` |
| `network` | string | no | Network applied to every table on this connection. Defaults to `eth-mainnet`. Databricks does not permit table-level overrides of connection parameters, so **use one UC connection per network** — there is no per-table `network` override. Common values: `eth-mainnet`, `base-mainnet`, `polygon-mainnet`, `arb-mainnet`, `opt-mainnet`, `sol-mainnet`. | `eth-mainnet` |
| `externalOptionsAllowList` | string | **yes** | Comma-separated list of allowed table-specific options. Because every table in this connector requires per-table options (wallet address, contract address, symbols, etc.), this field is required. Use the full allowlist below verbatim. | see below |

The `externalOptionsAllowList` must contain (in any order) every option you intend to pass through `table_options`. The connector's full supported set is:

```
wallet_address,contract_address,token_id,symbols,addresses,start_time,end_time,interval,with_market_data,max_records_per_batch,symbol,partition_days,limit,page_size,token_type,collection_slug
```

`network` is intentionally not in this list — it's a connection-level parameter and Databricks rejects table-level overrides of connection params (`INVALID_DATASOURCE_OPTION_OVERRIDE_ATTEMPT`).

### Obtaining an Alchemy API key

1. Sign up at [alchemy.com](https://www.alchemy.com) (free).
2. Open the [Alchemy Dashboard](https://dashboard.alchemy.com) → **Apps** → **Create new app**. Pick any name; the App is just a key + usage container.
3. Open the App detail page → copy the **API Key**.
4. **Strongly recommended**: still on the App detail page, configure **Security** → set IP / referrer allowlists, or rotate the key on a schedule. Because the key is embedded in the URL path (not an `Authorization` header), it appears in any HTTP-level logs your traffic passes through. Treat it as a high-value secret.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page.
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.
3. Provide the required connection parameters (`api_key`, optionally `network`) and set `externalOptionsAllowList` to the comma-separated string shown above.

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The connector exposes **11 tables** grouped by API family. Every table requires at least one per-table option in `table_options` (see "Per-table options" below).

### Portfolio API — wallet-centric

| Table | Description | Primary key | Cursor | Ingestion |
|-------|-------------|-------------|--------|-----------|
| `tokens_by_wallet` | Fungible token balances (native + ERC-20 + SPL) enriched with metadata and current USD prices per wallet. | `(address, network, tokenAddress)` | — | snapshot |
| `token_balances_by_wallet` | Raw on-chain fungible token balances per wallet. No metadata or price enrichment — cheaper than `tokens_by_wallet`. | `(address, network, tokenAddress)` | — | snapshot |
| `nft_collections_by_wallet` | NFT collections (contracts) held per wallet, with OpenSea metadata. One row per (wallet, network, contract). | `(address, network, contract.address)` | — | snapshot |
| `wallet_transactions` | Full transaction history (external + internal) per wallet. **Beta — ETH mainnet and Base mainnet only.** | `(hash, network)` | `timeStamp` | append |

Notes:
- `tokens_by_wallet` and `token_balances_by_wallet` rewrite `tokenAddress` from `null` to the literal `"NATIVE"` so the primary key stays non-null for native tokens (ETH, MATIC, etc.).
- `tokenBalance` and price `value` fields are returned as decimal **strings** to preserve precision — convert with Python `Decimal`, not `float`.

### Prices API — token pricing

| Table | Description | Primary key | Cursor | Ingestion |
|-------|-------------|-------------|--------|-----------|
| `token_prices_by_symbol` | Current prices for one or more token symbols. Up to 25 symbols per request. | `(symbol)` | — | snapshot |
| `token_prices_by_address` | Current prices for one or more `(network, contract address)` pairs. Up to 25 pairs across 3 networks per request. | `(network, address)` | — | snapshot |
| `token_prices_historical` | OHLCV-style price time series for a single token, by symbol or `(network, address)`. Supports `5m`, `1h`, `1d` intervals. **Partitioned reads supported** — long time ranges are split across executors. | `(symbol, network, address, timestamp)` | `timestamp` | append |

### NFT v3 API — token-level NFT data

| Table | Description | Primary key | Cursor | Ingestion |
|-------|-------------|-------------|--------|-----------|
| `nfts_by_wallet` | All individual NFTs (ERC-721 + ERC-1155) owned by a wallet, with full token metadata. | `(owner, network, contract.address, tokenId)` | — | snapshot |
| `nft_metadata` | Metadata for one specific NFT — name, description, image, traits. | `(network, contract.address, tokenId)` | — | snapshot |
| `nft_contract_metadata` | Collection-level metadata for an NFT contract — name, symbol, total supply, deployer, OpenSea metadata. | `(network, address)` | — | snapshot |
| `nft_floor_prices` | Current floor prices for an NFT collection by marketplace (OpenSea, LooksRare). **`eth-mainnet` only.** | `(network, contractAddress, marketplace)` | — | snapshot |

### Schema highlights

- **`raw_metadata` on NFT records** (`nfts_by_wallet`, `nft_metadata`) is modelled as `map<string, string>` because each contract's metadata layout differs — forcing a struct would silently drop fields. Re-parse in downstream queries as needed.
- **Nested structs** (token metadata, contract metadata, OpenSea metadata, NFT image / collection / mint / acquiredAt) are preserved as Spark structs rather than JSON strings.
- **`openseaMetadata`** is only populated for ETH and Polygon networks; expect `null` elsewhere.
- **`network` and `wallet`/`owner` columns are explicitly stamped** on each row so the same record is identifiable end-to-end without joining back to `table_options`.

## Per-table options

Per-table options go in the `table_options` map for each `table` entry in the pipeline spec. All option names must already be present in the connection's `externalOptionsAllowList`.

| Option | Type | Applies to | Description |
|--------|------|------------|-------------|
| `wallet_address` | string | `tokens_by_wallet`, `token_balances_by_wallet`, `nft_collections_by_wallet`, `wallet_transactions`, `nfts_by_wallet` | **Required.** Wallet address (hex for EVM, base58 for Solana). |
| `contract_address` | string | `nft_metadata`, `nft_contract_metadata`, `nft_floor_prices`, `token_prices_historical` | **Required** for NFT-by-contract tables and for the `token_prices_historical` address selector. Token contract address (hex). Uses the connection-level `network`. |
| `token_id` | string | `nft_metadata` | **Required.** NFT token ID (decimal or hex string). |
| `token_type` | string | `nft_metadata` | Optional hint: `ERC721` or `ERC1155`. Improves Alchemy response time when known. |
| `symbols` | string | `token_prices_by_symbol` | **Required.** Comma-separated token symbols, max 25. Example: `ETH,BTC,USDC`. |
| `addresses` | string | `token_prices_by_address` | **Required.** Comma-separated `network:address` pairs. Example: `eth-mainnet:0xA0b...,polygon-mainnet:0xC02...`. Max 25 pairs across 3 networks. |
| `symbol` | string | `token_prices_historical` | One of two selectors. Provide either `symbol` (cross-chain ticker like `ETH`), or `contract_address` (uses the connection-level network). |
| `start_time` | string | `token_prices_historical` | ISO 8601 start of range. Defaults to `end_time − max_window` for the chosen interval. |
| `end_time` | string | `token_prices_historical` | ISO 8601 end of range. Defaults to "now". |
| `interval` | string | `token_prices_historical` | `5m` (max 7-day window), `1h` (max 30-day window), or `1d` (max 365-day window). Default `1d`. |
| `with_market_data` | string | `token_prices_historical` | `true` or `false`. When `true`, includes `marketCap` and `totalVolume` columns. Default `false`. |
| `partition_days` | string | `token_prices_historical` | Override the per-partition window size (default = the interval's maximum window). Use smaller values for higher executor parallelism at the cost of more API calls. |
| `limit` | string | `wallet_transactions` | Page size, max 50, default 25. |
| `page_size` | string | `nfts_by_wallet` | Page size, max 100, default 100. |
| `collection_slug` | string | `nft_floor_prices` | Optional OpenSea collection slug to refine the lookup. |
| `max_records_per_batch` | string | append tables (`wallet_transactions`, `token_prices_historical`) | Admission cap honoured per microbatch; the connector returns a resumable cursor when the cap is hit. |

## Quick start

The following example creates a Unity Catalog connection and an ingestion pipeline that pulls token balances and historical ETH prices for a wallet.

### 1. Create the UC connection

```sql
CREATE CONNECTION my_alchemy_connection
TYPE foreign
OPTIONS (
  connector_name 'alchemy',
  api_key SECRET('alchemy', 'api_key'),
  network 'eth-mainnet',
  externalOptionsAllowList
    'wallet_address,contract_address,token_id,symbols,addresses,start_time,end_time,interval,with_market_data,max_records_per_batch,symbol,partition_days,limit,page_size,token_type,collection_slug'
);
```

### 2. Configure the pipeline

```python
from databricks.labs.community_connector.pipeline import ingest
from databricks.labs.community_connector import register

spark.conf.set(
    "spark.databricks.unityCatalog.connectionDfOptionInjection.enabled", "true"
)
register(spark, "alchemy")

VITALIK = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"

pipeline_spec = {
    "connection_name": "my_alchemy_connection",
    "objects": [
        # Snapshot: current token balances on the connection's network (eth-mainnet).
        # To ingest other chains, create a separate UC connection per chain.
        {
            "table": {
                "source_table": "tokens_by_wallet",
                "table_options": {"wallet_address": VITALIK},
            }
        },
        # Snapshot: NFTs owned on the connection's network
        {
            "table": {
                "source_table": "nfts_by_wallet",
                "table_options": {"wallet_address": VITALIK},
            }
        },
        # Append: daily ETH prices for 2025, with market data
        {
            "table": {
                "source_table": "token_prices_historical",
                "table_options": {
                    "symbol": "ETH",
                    "interval": "1d",
                    "start_time": "2025-01-01T00:00:00Z",
                    "end_time": "2025-12-31T23:59:59Z",
                    "with_market_data": "true",
                },
            }
        },
    ],
}

ingest(spark, pipeline_spec)
```

### 3. Run the pipeline

Snapshot tables fully re-fetch each run. Append tables (`wallet_transactions`, `token_prices_historical`) advance their cursor each run and resume from the last checkpoint on the next.

## Authentication notes

- **API key in the URL path.** Alchemy embeds the API key directly in the request URL (`https://api.g.alchemy.com/data/v1/{apiKey}/...`). It is not an `Authorization` header. Any infrastructure that logs full URLs (load balancers, proxies, application logs) will log the key. Always:
  - Store the key as a secret (Databricks Secret Manager or equivalent).
  - Configure **per-key IP / referrer allowlists** on the Alchemy Dashboard → App → Security tab to limit blast radius.
  - Rotate the key periodically; the same key works across Portfolio / Prices / NFT v3, so rotation is a single update.
- **Two URL shapes.** Portfolio and Prices APIs use `api.g.alchemy.com` with the network in the request **body**. NFT v3 uses `{network}.g.alchemy.com` with the network in the **subdomain**. The connector handles routing automatically — you set the network once via the connection-level `network` parameter and the connector picks the right host for every table.

## Rate limits and cost (Compute Units)

Every Alchemy API call consumes a number of **Compute Units (CUs)**. Plan quotas are:

| Plan | Monthly CUs | Throughput |
|------|-------------|------------|
| Free | 30,000,000 | 500 CU/s |
| Pay-as-you-go | Unlimited (metered, ~$0.45 per million CUs) | 10,000 CU/s |
| Enterprise | Custom | Custom |

Throughput is enforced via a token-bucket algorithm over a 10-second rolling window — a free-tier key can burst up to 5,000 CUs within any 10-second window.

Per-request CU cost varies dramatically by endpoint. Use this table when sizing pipeline frequency and parallelism:

| Table | CU per request |
|-------|---------------:|
| `tokens_by_wallet` | 360 |
| `token_balances_by_wallet` | 200 |
| `nft_collections_by_wallet` | 600 |
| `wallet_transactions` | 1,000 |
| `token_prices_by_symbol` | 40 |
| `token_prices_by_address` | 40 |
| `token_prices_historical` | 40 |
| `nfts_by_wallet` | 480 |
| `nft_metadata` | 80 |
| `nft_contract_metadata` | 160 |
| `nft_floor_prices` | 80 |

Practical implications:
- **Prices endpoints are cheap.** A daily historical price load across many tokens is essentially free on the 30M-CU tier.
- **Portfolio / NFT-by-wallet endpoints are expensive.** A single high-NFT-count wallet may require many `nfts_by_wallet` pages at 480 CU each. Throttle pipeline frequency accordingly.
- **`wallet_transactions` is the most expensive endpoint.** 1,000 CU per page. Use the `limit` (max 50) and `max_records_per_batch` options to bound per-run cost.
- **`nft_floor_prices` is cached for ~5 minutes** server-side — polling more frequently wastes CUs without returning fresher data.

The connector retries 429 / 500 / 503 responses with exponential backoff (initial 1s, doubled per retry, up to 5 attempts) and honours `Retry-After` when present. 400 / 401 / 403 / 404 are non-retriable and propagate as errors.

## Troubleshooting

### `401` or `403` from any endpoint

**Causes:**
- The `api_key` is wrong, revoked, or the App was deleted in the Dashboard.
- The key's IP allowlist (configured in the Dashboard) doesn't include the source IP of the Databricks compute running the pipeline.

**Fix:** Verify the key works with a direct `curl` against `https://api.g.alchemy.com/prices/v1/{key}/tokens/by-symbol?symbols=ETH`. If that succeeds but the pipeline fails, check the IP allowlist on the App.

### `wallet_transactions` errors with "only supports ['base-mainnet', 'eth-mainnet']"

**Cause:** `wallet_transactions` is Beta and only supports ETH mainnet and Base mainnet. The connection-level `network` is anything else.

**Fix:** Set the connection's `network` parameter to `eth-mainnet` or `base-mainnet` — or create a dedicated UC connection for `wallet_transactions` on one of those chains.

### `nft_floor_prices` errors with "only supports network 'eth-mainnet'"

**Cause:** Alchemy only returns floor prices for Ethereum mainnet (from OpenSea + LooksRare). Polygon, Base, Arbitrum, etc. are not supported by the upstream API.

**Fix:** Either omit `network` (defaults to `eth-mainnet`) or set it explicitly to `eth-mainnet`.

### `429 Too Many Requests` warnings in logs

**Cause:** You're exceeding your plan's CU/s throughput. The connector retries with exponential backoff but repeated 429s slow the pipeline.

**Fix:** Reduce pipeline frequency, reduce the number of wallets per run, or upgrade to a paid plan for higher throughput.

### `token_prices_historical` returns no rows

**Causes:**
- The requested time range is in the future relative to the connector's init timestamp (the connector caps at "now" to make streaming converge).
- Neither `symbol` nor (`network` + `contract_address`) was provided.
- The token is not tracked by Alchemy's price feed.

**Fix:** Confirm the time range is in the past, ensure exactly one selector is set, and try a known-tracked token (e.g. `ETH`, `BTC`) to isolate.

### Balances look wrong (off by 10^N)

**Cause:** `tokenBalance` is the **raw on-chain integer**, not a human-readable amount. ETH balances will be in wei, USDC balances in 6-decimal units, etc.

**Fix:** Use `tokenMetadata.decimals` to scale downstream: `Decimal(tokenBalance) / Decimal(10 ** decimals)`.

### NFT `tokenId` mixes decimal and hex forms across rows

**Cause:** Different NFT contracts return `tokenId` in different forms (some decimal, some `0x...` hex). Alchemy passes them through.

**Fix:** Normalise downstream — convert `0x`-prefixed values with `int(token_id, 16)` and cast everything to a uniform type.

## Limitations

- **`wallet_transactions` is Beta** and **ETH mainnet + Base mainnet only**. Alchemy has indicated this endpoint may eventually be superseded by `alchemy_getAssetTransfers`. Treat as unstable; gate downstream consumers behind a feature flag.
- **`nft_floor_prices` is Ethereum mainnet only.** OpenSea and LooksRare are the only supported marketplaces; no floor data for Polygon / Base / Arbitrum NFTs.
- **`openseaMetadata` is ETH + Polygon only.** On other networks expect `null` for the entire struct on `nft_collections_by_wallet`, `nfts_by_wallet`, `nft_metadata`, and `nft_contract_metadata`.
- **No true CDC.** None of Alchemy's data-plane endpoints expose a change feed. Snapshot tables fully re-fetch on every run; append tables (`wallet_transactions`, `token_prices_historical`) walk forward by cursor but cannot detect deletes / reorgs after the fact.
- **Portfolio API address limits.** `tokens_by_wallet` accepts max 2 wallet addresses per request; `token_balances_by_wallet` max 3; `nft_collections_by_wallet` max 2; `wallet_transactions` max 1. The connector issues one wallet per call — ingest multiple wallets by configuring separate `table` entries (one per wallet) in the pipeline spec.
- **Only `token_prices_historical` supports partitioned reads.** All other tables use the single-driver path. Wallet-scoped tables don't benefit from partitioning (no range filter), and `wallet_transactions` uses an opaque cursor that resists parallel reads.
- **API key is URL-path-bound.** No `Authorization` header support. Mitigate via secrets + Dashboard allowlists.
- **No write-back.** Connector is read-only.

## References

- [Alchemy Portfolio API reference](https://www.alchemy.com/docs/reference/portfolio-apis)
- [Alchemy Prices API reference](https://www.alchemy.com/docs/data/prices-api/prices-api-endpoints/prices-api-endpoints/get-token-prices-by-symbol)
- [Alchemy NFT API v3 reference](https://www.alchemy.com/docs/reference/nft-api-quickstart)
- [Alchemy Compute Unit costs](https://www.alchemy.com/docs/reference/compute-unit-costs)
- [Alchemy throughput and rate limits](https://www.alchemy.com/docs/reference/throughput)
- [Alchemy Dashboard](https://dashboard.alchemy.com)
- [Lakeflow Community Connectors Documentation](https://docs.databricks.com/en/lakehouse-connect/)

## Connector Information

- **Source**: Alchemy Portfolio, Prices, and NFT v3 APIs
- **Hosts**: `api.g.alchemy.com` (Portfolio + Prices), `{network}.g.alchemy.com` (NFT v3)
- **Supported Objects**: 11 tables — 4 Portfolio, 3 Prices, 4 NFT v3
- **Authentication**: API key embedded in URL path
- **Supported Ingestion Types**: snapshot, append (no native CDC)
- **Partitioned reads**: `token_prices_historical` only
