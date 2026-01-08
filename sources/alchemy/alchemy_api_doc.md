# Alchemy REST API Documentation

This document describes the Alchemy REST API endpoints implemented in the Lakeflow connector.

## Overview

Alchemy provides REST APIs for accessing blockchain data including NFTs, token prices, wallet balances, and transaction history across multiple networks.

## Authentication

All requests require an API key obtained from the Alchemy Dashboard. The API key is passed in the URL path for most endpoints.

## Supported Tables

### NFT Tables

#### nfts_by_owner
Returns all NFTs owned by a wallet address.

**Required Parameters:**
- `owner_address`: Wallet address (supports ENS on Ethereum)

**Optional Parameters:**
- `contract_addresses`: Filter by specific contract addresses (comma-separated)
- `with_metadata`: Include NFT metadata (default: true)
- `page_size`: Results per page (max 100, default: 100)
- `network`: Blockchain network (default: eth-mainnet)

**API Endpoint:** `GET /{network}.g.alchemy.com/nft/v3/{apiKey}/getNFTsForOwner`

#### nfts_for_contract
Returns all NFTs for a specific contract/collection.

**Required Parameters:**
- `contract_address`: NFT contract address

**Optional Parameters:**
- `with_metadata`: Include metadata (default: true)
- `start_token`: Start from token ID
- `limit`: Max results (default: 100)
- `network`: Blockchain network (default: eth-mainnet)

**API Endpoint:** `GET /{network}.g.alchemy.com/nft/v3/{apiKey}/getNFTsForContract`

#### nft_metadata
Returns metadata for a specific NFT.

**Required Parameters:**
- `contract_address`: NFT contract address
- `token_id`: Token ID

**Optional Parameters:**
- `token_type`: ERC721 or ERC1155
- `refresh_cache`: Force metadata refresh
- `network`: Blockchain network (default: eth-mainnet)

**API Endpoint:** `GET /{network}.g.alchemy.com/nft/v3/{apiKey}/getNFTMetadata`

#### contract_metadata
Returns metadata for an NFT contract.

**Required Parameters:**
- `contract_address`: NFT contract address

**Optional Parameters:**
- `network`: Blockchain network (default: eth-mainnet)

**API Endpoint:** `GET /{network}.g.alchemy.com/nft/v3/{apiKey}/getContractMetadata`

#### nft_metadata_batch
Returns metadata for multiple NFTs (up to 100).

**Required Parameters:**
- `tokens`: Array of contract address and token ID pairs (format: "contract:tokenId,contract:tokenId")

**Optional Parameters:**
- `token_uri_timeout_in_ms`: Timeout for metadata fetch (ms)
- `refresh_cache`: Force cache refresh

**API Endpoint:** `POST /{network}.g.alchemy.com/nft/v3/{apiKey}/getNFTMetadataBatch`

#### contract_metadata_batch
Returns metadata for multiple NFT contracts.

**Required Parameters:**
- `contract_addresses`: Comma-separated list of contract addresses

**API Endpoint:** `POST /{network}.g.alchemy.com/nft/v3/{apiKey}/getContractMetadataBatch`

#### nft_sales
Returns NFT sales history.

**Optional Parameters:**
- `contract_address`: Filter by contract
- `token_id`: Filter by token ID
- `from_block`: Start block number
- `to_block`: End block number
- `order`: 'asc' or 'desc'
- `page_key`: Pagination cursor
- `network`: Blockchain network (default: eth-mainnet)

**API Endpoint:** `GET /{network}.g.alchemy.com/nft/v3/{apiKey}/getNFTSales`

#### floor_prices
Returns floor price by marketplace.

**Required Parameters:**
- `contract_address`: NFT contract address

**Optional Parameters:**
- `network`: Blockchain network (default: eth-mainnet)

**API Endpoint:** `GET /{network}.g.alchemy.com/nft/v3/{apiKey}/getFloorPrice`

### Token/Price Tables

#### token_prices
Returns current token prices by symbol.

**Required Parameters:**
- `symbols`: Token symbols (comma-separated, max 25)

**API Endpoint:** `GET /api.g.alchemy.com/prices/v1/{apiKey}/tokens/by-symbol`

#### token_prices_by_address
Returns current token prices by contract address.

**Required Parameters:**
- `addresses`: Network and address pairs (format: "network:address,network:address")

**API Endpoint:** `POST /api.g.alchemy.com/prices/v1/{apiKey}/tokens/by-address`

#### token_prices_historical
Returns historical price data for tokens.

**Required Parameters (choose one method):**
- Method 1: `symbol` + `network` + `address`
- Method 2: `symbol` only (for major tokens)

**Required Parameters:**
- `start_time`: ISO timestamp start
- `end_time`: ISO timestamp end

**Optional Parameters:**
- `interval`: Data interval

**API Endpoint:** `GET /api.g.alchemy.com/prices/v1/{apiKey}/tokens/historical`

### Portfolio Tables

#### tokens_by_wallet
Returns tokens with balances, prices, and metadata.

**Required Parameters:**
- `addresses`: Wallet addresses with networks (max 2 addresses, 5 networks each)

**Optional Parameters:**
- `with_metadata`: Include token metadata (default: true)
- `with_prices`: Include token prices (default: true)
- `include_native_tokens`: Include native tokens like ETH (default: true)

**API Endpoint:** `GET /api.g.alchemy.com/data/v1/{apiKey}/getTokensByWallet`

#### token_balances_by_wallet
Returns token balances only.

**Required Parameters:**
- `addresses`: Wallet addresses with networks (max 3 addresses, 20 networks each)

**Optional Parameters:**
- `include_native_tokens`: Include native tokens (default: true)
- `include_erc20_tokens`: Include ERC-20 tokens (default: true)

**API Endpoint:** `GET /api.g.alchemy.com/data/v1/{apiKey}/getTokenBalancesByWallet`

#### nfts_by_wallet
Returns NFTs owned by wallets across networks.

**Required Parameters:**
- `addresses`: Wallet addresses with networks (max 2 addresses, 5 networks each)

**Optional Parameters:**
- `page_size`: Results per page (max 100)

**API Endpoint:** `GET /api.g.alchemy.com/data/v1/{apiKey}/getNftsByWallet`

#### wallet_transactions
Returns transaction history for wallets.

**Required Parameters:**
- `addresses`: Wallet addresses with networks (max 2 addresses, 5 networks each)

**Optional Parameters:**
- `from_block`: Start block
- `to_block`: End block
- `from_timestamp`: Start timestamp
- `to_timestamp`: End timestamp
- `category`: Transaction categories to include
- `order`: 'asc' or 'desc'

**API Endpoint:** `POST /api.g.alchemy.com/data/v1/{apiKey}/getTransactionsByWallet`

#### nft_collections_by_wallet
Returns NFT collections owned by wallets across networks.

**Required Parameters:**
- `addresses`: Wallet addresses with networks (max 2 addresses, 5 networks each)

**Optional Parameters:**
- `with_metadata`: Include collection metadata (default: true)

**API Endpoint:** `POST /api.g.alchemy.com/data/v1/{apiKey}/getNftCollectionsByWallet`

### New POST Endpoints

#### nft_metadata_batch
Returns metadata for multiple NFTs (up to 100).

**Required Parameters:**
- `tokens`: Array of contract address and token ID pairs (format: "contract:tokenId,contract:tokenId")

**Optional Parameters:**
- `token_uri_timeout_in_ms`: Timeout for metadata fetch (ms)
- `refresh_cache`: Force cache refresh

**API Endpoint:** `POST /{network}.g.alchemy.com/nft/v3/{apiKey}/getNFTMetadataBatch`

#### contract_metadata_batch
Returns metadata for multiple NFT contracts.

**Required Parameters:**
- `contract_addresses`: Comma-separated list of contract addresses

**API Endpoint:** `POST /{network}.g.alchemy.com/nft/v3/{apiKey}/getContractMetadataBatch`

#### token_prices_by_address
Returns token prices by contract address.

**Required Parameters:**
- `addresses`: Network and address pairs (format: "network:address,network:address")

**API Endpoint:** `POST /api.g.alchemy.com/prices/v1/{apiKey}/tokens/by-address`

### Webhook Tables

#### webhooks
Returns all webhooks for the team.

**API Endpoint:** `GET /dashboard.alchemy.com/api/team-webhooks`

**Headers:**
- `X-Alchemy-Token`: Auth token from Webhooks dashboard

#### webhook_addresses
Returns addresses tracked by a webhook.

**Required Parameters:**
- `webhook_id`: Webhook ID

**Optional Parameters:**
- `limit`: Results per page
- `after`: Pagination cursor

**API Endpoint:** `GET /dashboard.alchemy.com/api/webhook-addresses`

## Supported Networks

- **Ethereum & L2s**: eth-mainnet, eth-sepolia, eth-holesky, arb-mainnet, opt-mainnet, base-mainnet, etc.
- **Polygon**: polygon-mainnet, polygon-amoy
- **Other EVM**: bnb-mainnet, avax-mainnet, linea-mainnet, etc.
- **Solana**: solana-mainnet, solana-devnet

## Rate Limits

Alchemy uses Compute Units (CU) for billing. Free tier provides 300M monthly CUs with rate limiting.