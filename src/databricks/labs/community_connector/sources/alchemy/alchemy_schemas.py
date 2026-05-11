"""Static schema definitions for the Alchemy connector.

Schemas are derived from the Alchemy Portfolio API, Prices API, and
NFT API v3.  See ``alchemy_api_doc.md`` for endpoint references and
per-table edge cases.

Design rules followed by every schema below:
- ``LongType`` everywhere an integer might exceed 32 bits (block
  numbers, balances expressed as strings, log indices).
- ``StructType`` for shape-stable nested objects (token metadata,
  contract metadata, image, etc.) rather than ``MapType``.
- ``raw_metadata`` on NFT records is the one genuinely open shape:
  it's modelled as ``MapType(StringType, StringType)`` because each
  contract's metadata layout differs and forcing a struct would drop
  fields silently.  Callers wanting structured access can re-parse
  the map values themselves.
- All numeric balance / price / value fields are kept as ``StringType``
  to preserve precision (Alchemy returns these as decimal strings).
- ``network`` and ``wallet`` columns are added so the same row can be
  identified end-to-end without round-tripping through table_options.
"""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)


# =============================================================================
# Reusable nested struct definitions
# =============================================================================

TOKEN_METADATA_STRUCT = StructType(
    [
        StructField("decimals", LongType(), True),
        StructField("logo", StringType(), True),
        StructField("name", StringType(), True),
        StructField("symbol", StringType(), True),
    ]
)

TOKEN_PRICE_ENTRY_STRUCT = StructType(
    [
        StructField("currency", StringType(), True),
        StructField("value", StringType(), True),
        StructField("lastUpdatedAt", StringType(), True),
    ]
)

OPENSEA_METADATA_STRUCT = StructType(
    [
        StructField("floorPrice", DoubleType(), True),
        StructField("collectionName", StringType(), True),
        StructField("safelistRequestStatus", StringType(), True),
        StructField("imageUrl", StringType(), True),
        StructField("description", StringType(), True),
        StructField("externalUrl", StringType(), True),
        StructField("twitterUsername", StringType(), True),
        StructField("discordUrl", StringType(), True),
        StructField("bannerImageUrl", StringType(), True),
        StructField("lastIngestedAt", StringType(), True),
    ]
)

# Used as embedded contract metadata on NFT collection / NFT records.
# Kept consistent across endpoints even though the live response field
# casing varies between ``openseaMetadata`` and ``openSeaMetadata`` —
# the connector normalises both to ``openseaMetadata`` before yielding.
CONTRACT_METADATA_STRUCT = StructType(
    [
        StructField("address", StringType(), True),
        StructField("name", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("totalSupply", StringType(), True),
        StructField("tokenType", StringType(), True),
        StructField("contractDeployer", StringType(), True),
        StructField("deployedBlockNumber", LongType(), True),
        StructField("openseaMetadata", OPENSEA_METADATA_STRUCT, True),
        StructField("isSpam", StringType(), True),
        StructField("spamClassifications", ArrayType(StringType()), True),
    ]
)

NFT_IMAGE_STRUCT = StructType(
    [
        StructField("cachedUrl", StringType(), True),
        StructField("thumbnailUrl", StringType(), True),
        StructField("pngUrl", StringType(), True),
        StructField("contentType", StringType(), True),
        StructField("size", LongType(), True),
        StructField("originalUrl", StringType(), True),
    ]
)

NFT_ANIMATION_STRUCT = StructType(
    [
        StructField("cachedUrl", StringType(), True),
        StructField("contentType", StringType(), True),
        StructField("size", LongType(), True),
        StructField("originalUrl", StringType(), True),
    ]
)

NFT_COLLECTION_STRUCT = StructType(
    [
        StructField("name", StringType(), True),
        StructField("slug", StringType(), True),
        StructField("externalUrl", StringType(), True),
        StructField("bannerImageUrl", StringType(), True),
    ]
)

NFT_RAW_STRUCT = StructType(
    [
        StructField("tokenUri", StringType(), True),
        # ``metadata`` shapes vary wildly per contract — keep as a flat
        # string->string map so heterogeneous keys are preserved.
        StructField("metadata", MapType(StringType(), StringType()), True),
        StructField("error", StringType(), True),
    ]
)

NFT_ACQUIRED_AT_STRUCT = StructType(
    [
        StructField("blockTimestamp", StringType(), True),
        StructField("blockNumber", LongType(), True),
    ]
)

NFT_MINT_STRUCT = StructType(
    [
        StructField("mintAddress", StringType(), True),
        StructField("blockNumber", LongType(), True),
        StructField("timestamp", StringType(), True),
        StructField("transactionHash", StringType(), True),
    ]
)

TX_LOG_STRUCT = StructType(
    [
        StructField("contractAddress", StringType(), True),
        StructField("logIndex", LongType(), True),
        StructField("data", StringType(), True),
        StructField("removed", BooleanType(), True),
        StructField("topics", ArrayType(StringType()), True),
    ]
)

TX_INTERNAL_STRUCT = StructType(
    [
        StructField("type", StringType(), True),
        StructField("from", StringType(), True),
        StructField("to", StringType(), True),
        StructField("value", StringType(), True),
        StructField("gas", StringType(), True),
        StructField("gasUsed", StringType(), True),
        StructField("input", StringType(), True),
        StructField("output", StringType(), True),
        StructField("error", StringType(), True),
    ]
)


# =============================================================================
# Table schemas
# =============================================================================

TABLE_SCHEMAS: dict[str, StructType] = {
    # ---- Portfolio API ------------------------------------------------ #
    "tokens_by_wallet": StructType(
        [
            StructField("address", StringType(), False),
            StructField("network", StringType(), False),
            # ``tokenAddress`` is null for native tokens; the connector
            # rewrites it to the literal ``"NATIVE"`` so the PK stays
            # non-null and joins behave.
            StructField("tokenAddress", StringType(), False),
            StructField("tokenBalance", StringType(), True),
            StructField("tokenMetadata", TOKEN_METADATA_STRUCT, True),
            StructField("tokenPrices", ArrayType(TOKEN_PRICE_ENTRY_STRUCT), True),
            StructField("error", StringType(), True),
        ]
    ),
    "token_balances_by_wallet": StructType(
        [
            StructField("address", StringType(), False),
            StructField("network", StringType(), False),
            StructField("tokenAddress", StringType(), False),
            StructField("tokenBalance", StringType(), True),
        ]
    ),
    "nft_collections_by_wallet": StructType(
        [
            StructField("address", StringType(), False),
            StructField("network", StringType(), False),
            StructField("contract", CONTRACT_METADATA_STRUCT, False),
        ]
    ),
    # ---- Portfolio API (beta) ----------------------------------------- #
    "wallet_transactions": StructType(
        [
            StructField("address", StringType(), False),
            StructField("network", StringType(), False),
            StructField("hash", StringType(), False),
            StructField("timeStamp", StringType(), True),
            StructField("blockNumber", LongType(), True),
            StructField("blockHash", StringType(), True),
            StructField("nonce", LongType(), True),
            StructField("transactionIndex", LongType(), True),
            StructField("fromAddress", StringType(), True),
            StructField("toAddress", StringType(), True),
            StructField("contractAddress", StringType(), True),
            StructField("value", StringType(), True),
            StructField("cumulativeGasUsed", StringType(), True),
            StructField("effectiveGasPrice", StringType(), True),
            StructField("gasUsed", StringType(), True),
            StructField("logs", ArrayType(TX_LOG_STRUCT), True),
            StructField("internalTxns", ArrayType(TX_INTERNAL_STRUCT), True),
        ]
    ),
    # ---- Prices API --------------------------------------------------- #
    "token_prices_by_symbol": StructType(
        [
            StructField("symbol", StringType(), False),
            StructField("prices", ArrayType(TOKEN_PRICE_ENTRY_STRUCT), True),
            StructField("error", StringType(), True),
        ]
    ),
    "token_prices_by_address": StructType(
        [
            StructField("network", StringType(), False),
            StructField("address", StringType(), False),
            StructField("prices", ArrayType(TOKEN_PRICE_ENTRY_STRUCT), True),
            StructField("error", StringType(), True),
        ]
    ),
    "token_prices_historical": StructType(
        [
            # ``symbol`` and (``network``, ``address``) are mutually
            # exclusive at request time, but we materialise both so the
            # schema is stable regardless of which selector the user
            # provides in table_options.  Only the relevant ones are
            # populated per row.
            StructField("symbol", StringType(), True),
            StructField("network", StringType(), True),
            StructField("address", StringType(), True),
            StructField("currency", StringType(), True),
            StructField("value", StringType(), True),
            StructField("timestamp", StringType(), False),
            StructField("marketCap", StringType(), True),
            StructField("totalVolume", StringType(), True),
        ]
    ),
    # ---- NFT API v3 --------------------------------------------------- #
    "nfts_by_wallet": StructType(
        [
            StructField("owner", StringType(), False),
            StructField("network", StringType(), False),
            StructField("contract", CONTRACT_METADATA_STRUCT, False),
            StructField("tokenId", StringType(), False),
            StructField("tokenType", StringType(), True),
            StructField("name", StringType(), True),
            StructField("description", StringType(), True),
            StructField("image", NFT_IMAGE_STRUCT, True),
            StructField("raw", NFT_RAW_STRUCT, True),
            StructField("collection", NFT_COLLECTION_STRUCT, True),
            StructField("tokenUri", StringType(), True),
            StructField("timeLastUpdated", StringType(), True),
            StructField("acquiredAt", NFT_ACQUIRED_AT_STRUCT, True),
            StructField("animation", NFT_ANIMATION_STRUCT, True),
            StructField("mint", NFT_MINT_STRUCT, True),
            StructField("owners", ArrayType(StringType()), True),
            StructField("balance", StringType(), True),
        ]
    ),
    "nft_metadata": StructType(
        [
            StructField("network", StringType(), False),
            StructField("contract", CONTRACT_METADATA_STRUCT, False),
            StructField("tokenId", StringType(), False),
            StructField("tokenType", StringType(), True),
            StructField("name", StringType(), True),
            StructField("description", StringType(), True),
            StructField("image", NFT_IMAGE_STRUCT, True),
            StructField("raw", NFT_RAW_STRUCT, True),
            StructField("collection", NFT_COLLECTION_STRUCT, True),
            StructField("tokenUri", StringType(), True),
            StructField("timeLastUpdated", StringType(), True),
            StructField("animation", NFT_ANIMATION_STRUCT, True),
            StructField("mint", NFT_MINT_STRUCT, True),
            StructField("owners", ArrayType(StringType()), True),
        ]
    ),
    "nft_contract_metadata": StructType(
        [
            StructField("network", StringType(), False),
            StructField("address", StringType(), False),
            StructField("name", StringType(), True),
            StructField("symbol", StringType(), True),
            StructField("totalSupply", StringType(), True),
            StructField("tokenType", StringType(), True),
            StructField("contractDeployer", StringType(), True),
            StructField("deployedBlockNumber", LongType(), True),
            StructField("openseaMetadata", OPENSEA_METADATA_STRUCT, True),
        ]
    ),
    "nft_floor_prices": StructType(
        [
            StructField("network", StringType(), False),
            StructField("contractAddress", StringType(), False),
            StructField("marketplace", StringType(), False),
            StructField("floorPrice", DoubleType(), True),
            StructField("priceCurrency", StringType(), True),
            StructField("collectionUrl", StringType(), True),
            StructField("retrievedAt", StringType(), False),
            StructField("error", StringType(), True),
        ]
    ),
}


# =============================================================================
# Table metadata
# =============================================================================
#
# Primary-key conventions follow ``alchemy_api_doc.md`` (one row per
# unique tuple per network / contract / token / wallet).  ``cursor_field``
# is only set for append tables — the rest are snapshot.

TABLE_METADATA: dict[str, dict] = {
    "tokens_by_wallet": {
        "primary_keys": ["address", "network", "tokenAddress"],
        "ingestion_type": "snapshot",
    },
    "token_balances_by_wallet": {
        "primary_keys": ["address", "network", "tokenAddress"],
        "ingestion_type": "snapshot",
    },
    "nft_collections_by_wallet": {
        # ``contract.address`` is a nested field; the engine accepts
        # dotted paths as PK references.
        "primary_keys": ["address", "network", "contract.address"],
        "ingestion_type": "snapshot",
    },
    "wallet_transactions": {
        "primary_keys": ["hash", "network"],
        "cursor_field": "timeStamp",
        "ingestion_type": "append",
    },
    "token_prices_by_symbol": {
        "primary_keys": ["symbol"],
        "ingestion_type": "snapshot",
    },
    "token_prices_by_address": {
        "primary_keys": ["network", "address"],
        "ingestion_type": "snapshot",
    },
    "token_prices_historical": {
        # Symbol XOR (network, address) — both columns are present in
        # the schema but only one path will have non-null values.
        "primary_keys": ["symbol", "network", "address", "timestamp"],
        "cursor_field": "timestamp",
        "ingestion_type": "append",
    },
    "nfts_by_wallet": {
        "primary_keys": ["owner", "network", "contract.address", "tokenId"],
        "ingestion_type": "snapshot",
    },
    "nft_metadata": {
        "primary_keys": ["network", "contract.address", "tokenId"],
        "ingestion_type": "snapshot",
    },
    "nft_contract_metadata": {
        "primary_keys": ["network", "address"],
        "ingestion_type": "snapshot",
    },
    "nft_floor_prices": {
        "primary_keys": ["network", "contractAddress", "marketplace"],
        "ingestion_type": "snapshot",
    },
}


SUPPORTED_TABLES: list[str] = list(TABLE_SCHEMAS.keys())


# =============================================================================
# Endpoint classification
# =============================================================================
#
# Internal — used by alchemy.py to route requests to the correct host
# and assemble per-endpoint payloads.

PORTFOLIO_TABLES = frozenset(
    {
        "tokens_by_wallet",
        "token_balances_by_wallet",
        "nft_collections_by_wallet",
        "wallet_transactions",
    }
)
PRICES_TABLES = frozenset(
    {
        "token_prices_by_symbol",
        "token_prices_by_address",
        "token_prices_historical",
    }
)
NFT_V3_TABLES = frozenset(
    {
        "nfts_by_wallet",
        "nft_metadata",
        "nft_contract_metadata",
        "nft_floor_prices",
    }
)

# Tables whose Alchemy endpoint accepts only Ethereum mainnet.
ETH_MAINNET_ONLY_TABLES = frozenset({"nft_floor_prices"})

# Tables that the wallet_transactions beta endpoint supports.  Used to
# validate user-supplied networks before issuing a doomed request.
WALLET_TX_ALLOWED_NETWORKS = frozenset({"eth-mainnet", "base-mainnet"})


# Per-endpoint URL fragments.  Kept here so alchemy.py stays declarative.
PORTFOLIO_PATHS: dict[str, str] = {
    "tokens_by_wallet": "/data/v1/{api_key}/assets/tokens/by-address",
    "token_balances_by_wallet": ("/data/v1/{api_key}/assets/tokens/balances/by-address"),
    "nft_collections_by_wallet": ("/data/v1/{api_key}/assets/nfts/contracts/by-address"),
    "wallet_transactions": ("/data/v1/{api_key}/transactions/history/by-address"),
}

PRICES_PATHS: dict[str, str] = {
    "token_prices_by_symbol": "/prices/v1/{api_key}/tokens/by-symbol",
    "token_prices_by_address": "/prices/v1/{api_key}/tokens/by-address",
    "token_prices_historical": "/prices/v1/{api_key}/tokens/historical",
}

NFT_V3_PATHS: dict[str, str] = {
    "nfts_by_wallet": "/nft/v3/{api_key}/getNFTsForOwner",
    "nft_metadata": "/nft/v3/{api_key}/getNFTMetadata",
    "nft_contract_metadata": "/nft/v3/{api_key}/getContractMetadata",
    "nft_floor_prices": "/nft/v3/{api_key}/getFloorPrice",
}


# =============================================================================
# Retry & request constants
# =============================================================================

RETRIABLE_STATUS_CODES = frozenset({429, 500, 503})
MAX_RETRIES = 5
INITIAL_BACKOFF = 1.0  # seconds; doubled after each retry
REQUEST_CONNECT_TIMEOUT = 5
REQUEST_READ_TIMEOUT = 30

DEFAULT_NETWORK = "eth-mainnet"
NATIVE_TOKEN_SENTINEL = "NATIVE"
