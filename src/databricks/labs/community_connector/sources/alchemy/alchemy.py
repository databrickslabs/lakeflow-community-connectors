"""Alchemy Lakeflow connector.

Wraps three Alchemy API families behind a single ``LakeflowConnect``
implementation: Portfolio (POST, body-network), Prices (mixed
GET/POST), and NFT API v3 (GET, subdomain-network).

See ``alchemy_api_doc.md`` for endpoint details, CU costs, and the
beta status of ``wallet_transactions``.

This connector implements ``SupportsPartitionedStream`` because one
table (``token_prices_historical``) supports range queries and
benefits from parallel reads over time windows.  All other tables
opt out of partitioned reads via ``is_partitioned`` and fall back to
the single-driver ``read_table`` path.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator

import requests
from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import (
    LakeflowConnect,
    SupportsPartitionedStream,
)
from databricks.labs.community_connector.sources.alchemy.alchemy_schemas import (
    DEFAULT_NETWORK,
    ETH_MAINNET_ONLY_TABLES,
    NFT_V3_PATHS,
    NFT_V3_TABLES,
    PORTFOLIO_PATHS,
    PORTFOLIO_TABLES,
    PRICES_PATHS,
    PRICES_TABLES,
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
    WALLET_TX_ALLOWED_NETWORKS,
)
from databricks.labs.community_connector.sources.alchemy.alchemy_utils import (
    api_call,
    normalise_contract,
    normalise_native_token_address,
    normalise_nft_record,
    parse_csv_option,
    parse_max_records,
    parse_networks,
    require_option,
)


_LOG = logging.getLogger(__name__)


PORTFOLIO_PRICES_HOST = "https://api.g.alchemy.com"


def _nft_v3_base_url(network: str) -> str:
    """Return the NFT v3 host for a given network (subdomain-routed)."""
    return f"https://{network}.g.alchemy.com"


# Default historical interval window per Alchemy's documented limits.
# See alchemy_api_doc.md: 5m → 7d, 1h → 30d, 1d → 365d.
_HISTORICAL_INTERVAL_DAYS = {"5m": 7, "1h": 30, "1d": 365}


class AlchemyLakeflowConnect(LakeflowConnect, SupportsPartitionedStream):
    """LakeflowConnect implementation for Alchemy.

    Connection options:
        api_key  (required, secret) — Alchemy API key.
        network  (optional, default "eth-mainnet") — default network
                 applied to tables that don't override it via
                 ``table_options``.

    Per-table options (read from ``table_options`` per call):
        wallet_address    — required for the four wallet-scoped tables.
        network           — per-table override; comma-separated for
                            tables that accept multiple networks.
        contract_address  — required for nft_metadata,
                            nft_contract_metadata, nft_floor_prices.
        token_id          — required for nft_metadata.
        symbols           — required for token_prices_by_symbol.
        addresses         — required for token_prices_by_address;
                            ``network:address`` pairs, comma-separated.
        symbol / network+address
                           — for token_prices_historical (mutually
                             exclusive selectors).
        start_time / end_time
                           — for token_prices_historical.
        interval          — for token_prices_historical (5m, 1h, 1d).
        with_market_data  — for token_prices_historical (true/false).
        max_records_per_batch
                           — admission control, honoured per microbatch.
    """

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)

        api_key = options.get("api_key")
        if not api_key:
            raise ValueError("Alchemy connector requires 'api_key'")
        self._api_key = api_key
        self._default_network = options.get("network") or DEFAULT_NETWORK

        self._session = requests.Session()
        self._session.headers.update(
            {
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )

        # Init-time cap.  Append tables clamp their returned cursor to
        # this value so Trigger.AvailableNow converges even when the
        # source has continuously-arriving new data (transactions /
        # historical prices).  A subsequent trigger creates a fresh
        # instance with a newer ``_init_ts`` and picks up the gap.
        self._init_ts = datetime.now(timezone.utc).isoformat()

    # ================================================================== #
    # LakeflowConnect interface
    # ================================================================== #

    def list_tables(self) -> list[str]:
        return list(SUPPORTED_TABLES)

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        if table_name not in TABLE_SCHEMAS:
            raise ValueError(f"Unsupported table: {table_name!r}")
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        if table_name not in TABLE_METADATA:
            raise ValueError(f"Unsupported table: {table_name!r}")
        return TABLE_METADATA[table_name]

    def read_table(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        if table_name not in SUPPORTED_TABLES:
            raise ValueError(f"Unsupported table: {table_name!r}")

        start_offset = start_offset or {}

        # ----- Portfolio (snapshot, paginated) ------------------------ #
        if table_name == "tokens_by_wallet":
            return self._read_tokens_by_wallet(table_options)
        if table_name == "token_balances_by_wallet":
            return self._read_token_balances_by_wallet(table_options)
        if table_name == "nft_collections_by_wallet":
            return self._read_nft_collections_by_wallet(table_options)

        # ----- Portfolio beta (append, dual cursor) ------------------- #
        if table_name == "wallet_transactions":
            return self._read_wallet_transactions(start_offset, table_options)

        # ----- Prices (snapshot) -------------------------------------- #
        if table_name == "token_prices_by_symbol":
            return self._read_token_prices_by_symbol(table_options)
        if table_name == "token_prices_by_address":
            return self._read_token_prices_by_address(table_options)

        # ----- Prices (append, time-range) ---------------------------- #
        if table_name == "token_prices_historical":
            return self._read_token_prices_historical(start_offset, table_options)

        # ----- NFT v3 (snapshot, GET) --------------------------------- #
        if table_name == "nfts_by_wallet":
            return self._read_nfts_by_wallet(table_options)
        if table_name == "nft_metadata":
            return self._read_nft_metadata(table_options)
        if table_name == "nft_contract_metadata":
            return self._read_nft_contract_metadata(table_options)
        if table_name == "nft_floor_prices":
            return self._read_nft_floor_prices(table_options)

        # Defensive; unreachable given the validation above.
        raise ValueError(f"Unhandled table: {table_name!r}")

    # ================================================================== #
    # SupportsPartitionedStream
    # ================================================================== #

    def is_partitioned(self, table_name: str) -> bool:
        """Only ``token_prices_historical`` benefits from partitioning.

        It accepts a range query (``startTime`` / ``endTime``) and a
        single token can produce a long time-series, so splitting the
        range across executors is worthwhile.  Everything else either
        has no range filter (snapshots) or a single-cursor pagination
        model that resists parallelisation (``wallet_transactions``).
        """
        return table_name == "token_prices_historical"

    def latest_offset(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict | None = None,
    ) -> dict:
        """Return the high-water-mark offset, capped at ``_init_ts``.

        Only invoked for partitioned-streaming tables — see
        ``is_partitioned``.
        """
        if table_name != "token_prices_historical":
            # Defensive; the framework should not call this for tables
            # whose ``is_partitioned`` returns False.
            return {"cursor": self._init_ts}

        end_time = self._resolve_end_time(table_options)
        # Cap at init time so streaming converges.
        capped = min(end_time, self._init_ts) if end_time else self._init_ts
        return {"cursor": capped}

    def get_partitions(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict | None = None,
        end_offset: dict | None = None,
    ) -> list[dict]:
        """Split the historical-prices range into per-window partitions."""
        if table_name != "token_prices_historical":
            return [{"all": True}]

        interval = table_options.get("interval", "1d")
        # Default partition size = the interval's maximum API window.
        # Users can override with ``partition_days`` if they want finer
        # granularity (e.g. higher parallelism at the cost of more
        # requests).
        max_days_per_window = _HISTORICAL_INTERVAL_DAYS.get(interval, 365)
        partition_days = int(table_options.get("partition_days", str(max_days_per_window)))
        partition_days = max(1, min(partition_days, max_days_per_window))

        if start_offset is None and end_offset is None:
            # Batch mode: cover the full user-supplied range.
            start = self._resolve_start_time(table_options)
            end = min(self._resolve_end_time(table_options), self._init_ts)
        else:
            start = (start_offset or {}).get("cursor")
            if not start:
                start = self._resolve_start_time(table_options)
            end = (end_offset or {}).get("cursor", self._init_ts)

        if not start or not end or start >= end:
            return []

        return _split_time_range_into_partitions(start, end, partition_days)

    def read_partition(
        self,
        table_name: str,
        partition: dict,
        table_options: dict[str, str],
    ) -> Iterator[dict]:
        """Read records for one historical-prices time window."""
        if table_name != "token_prices_historical":
            raise ValueError(f"Table '{table_name}' does not support partitioned reads")
        start_time = partition["start"]
        end_time = partition["end"]
        records = self._fetch_token_prices_historical(
            table_options, start_time=start_time, end_time=end_time
        )
        return iter(records)

    # ================================================================== #
    # Portfolio API readers
    # ================================================================== #

    def _portfolio_url(self, table_name: str) -> str:
        path = PORTFOLIO_PATHS[table_name].format(api_key=self._api_key)
        return f"{PORTFOLIO_PRICES_HOST}{path}"

    def _read_tokens_by_wallet(self, table_options: dict[str, str]) -> tuple[Iterator[dict], dict]:
        wallet = require_option(table_options, "wallet_address", "tokens_by_wallet")
        networks = parse_networks(table_options, self._default_network)
        body: dict[str, Any] = {
            "addresses": [{"address": wallet, "networks": networks}],
            "withMetadata": True,
            "withPrices": True,
        }
        url = self._portfolio_url("tokens_by_wallet")
        records: list[dict] = []
        for page in self._paginate_portfolio(url, body, "tokens_by_wallet"):
            for token in page.get("tokens", []):
                rec = dict(token)
                # Stamp the queried wallet on the row (Alchemy echoes it
                # but the connector is the source of truth for the PK).
                rec["address"] = rec.get("address") or wallet
                rec["tokenAddress"] = normalise_native_token_address(rec.get("tokenAddress"))
                records.append(rec)
        # Snapshot — no offset.
        return iter(records), {}

    def _read_token_balances_by_wallet(
        self, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        wallet = require_option(table_options, "wallet_address", "token_balances_by_wallet")
        networks = parse_networks(table_options, self._default_network)
        body: dict[str, Any] = {
            "addresses": [{"address": wallet, "networks": networks}],
        }
        url = self._portfolio_url("token_balances_by_wallet")
        records: list[dict] = []
        for page in self._paginate_portfolio(url, body, "token_balances_by_wallet"):
            for token in page.get("tokens", []):
                rec = dict(token)
                rec["address"] = rec.get("address") or wallet
                rec["tokenAddress"] = normalise_native_token_address(rec.get("tokenAddress"))
                records.append(rec)
        return iter(records), {}

    def _read_nft_collections_by_wallet(
        self, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        wallet = require_option(table_options, "wallet_address", "nft_collections_by_wallet")
        networks = parse_networks(table_options, self._default_network)
        body: dict[str, Any] = {
            "addresses": [{"address": wallet, "networks": networks}],
            "withMetadata": True,
        }
        url = self._portfolio_url("nft_collections_by_wallet")
        records: list[dict] = []
        for page in self._paginate_portfolio(url, body, "nft_collections_by_wallet"):
            for entry in page.get("contracts", []):
                rec = dict(entry)
                rec["address"] = rec.get("address") or wallet
                rec["contract"] = normalise_contract(rec.get("contract"))
                records.append(rec)
        return iter(records), {}

    def _read_wallet_transactions(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Append-only walk forward through transaction history.

        Uses Alchemy's ``after`` cursor.  The endpoint does not accept
        a ``fromBlock`` parameter, so the connector relies on the
        opaque cursor for resumption.  Once ``timeStamp`` advances past
        ``_init_ts`` the call stops emitting and returns a terminal
        offset anchored at ``_init_ts`` so AvailableNow converges.
        """
        wallet = require_option(table_options, "wallet_address", "wallet_transactions")
        networks = parse_networks(table_options, self._default_network)
        unsupported = [n for n in networks if n not in WALLET_TX_ALLOWED_NETWORKS]
        if unsupported:
            raise ValueError(
                "wallet_transactions only supports "
                f"{sorted(WALLET_TX_ALLOWED_NETWORKS)}; got {unsupported}"
            )

        # Already drained up to init time — terminate immediately.
        existing_cursor = start_offset.get("cursor")
        if existing_cursor and existing_cursor >= self._init_ts:
            return iter([]), start_offset

        max_records = parse_max_records(table_options)
        after_token = start_offset.get("after")

        url = self._portfolio_url("wallet_transactions")
        records: list[dict] = []
        max_ts_seen: str | None = existing_cursor
        cap_hit = False
        next_after: str | None = after_token

        while True:
            body: dict[str, Any] = {
                "addresses": [{"address": wallet, "networks": networks}],
                "limit": int(table_options.get("limit", "25")),
            }
            if next_after:
                body["after"] = next_after

            page = api_call(
                self._session,
                "POST",
                url,
                "wallet_transactions",
                json_body=body,
            )
            batch = page.get("transactions", []) or []
            if not batch:
                break

            for tx in batch:
                rec = dict(tx)
                rec["address"] = wallet
                ts = rec.get("timeStamp")
                # Defensive guard against late-arriving rows past init.
                if ts and ts > self._init_ts:
                    continue
                records.append(rec)
                if ts and (max_ts_seen is None or ts > max_ts_seen):
                    max_ts_seen = ts
                if max_records is not None and len(records) >= max_records:
                    cap_hit = True
                    break

            if cap_hit:
                break

            next_after = page.get("after")
            if not next_after:
                break

            # If the freshest record in this batch is already past the
            # cap, the next page would only return more post-cap data —
            # bail out and let the next trigger handle it.
            tail_ts = batch[-1].get("timeStamp") if batch else None
            if tail_ts and tail_ts >= self._init_ts:
                break

        if not records and not cap_hit:
            # No new data — anchor the cursor at init time so the next
            # call sees ``cursor >= _init_ts`` and short-circuits.
            terminal = (
                {"cursor": self._init_ts, "after": next_after}
                if next_after
                else {"cursor": self._init_ts}
            )
            if start_offset and start_offset == terminal:
                return iter([]), start_offset
            return iter([]), terminal

        if cap_hit:
            # Mid-walk admission cap — resume from the last cursor.
            return iter(records), {
                "cursor": max_ts_seen or self._init_ts,
                "after": next_after,
            }

        # Walk fully drained — anchor at init_ts to terminate.
        end_cursor = max_ts_seen if max_ts_seen and max_ts_seen >= self._init_ts else self._init_ts
        end_offset: dict[str, Any] = {"cursor": end_cursor}
        # ``after`` cursor isn't needed once drained, but echo it so
        # equality with start_offset is stable when both are None.
        if start_offset and start_offset == end_offset:
            return iter([]), start_offset
        return iter(records), end_offset

    # ================================================================== #
    # Prices API readers
    # ================================================================== #

    def _prices_url(self, table_name: str) -> str:
        path = PRICES_PATHS[table_name].format(api_key=self._api_key)
        return f"{PORTFOLIO_PRICES_HOST}{path}"

    def _read_token_prices_by_symbol(
        self, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        symbols = parse_csv_option(table_options, "symbols")
        if not symbols:
            raise ValueError(
                "token_prices_by_symbol requires table_options['symbols'] (comma-separated)"
            )
        # Alchemy supports both ``symbols=ETH&symbols=BTC`` (repeated)
        # and ``symbols=[ETH,BTC]`` forms.  ``requests`` encodes a list
        # value as repeated params, which is the canonical form.
        url = self._prices_url("token_prices_by_symbol")
        body = api_call(
            self._session,
            "GET",
            url,
            "token_prices_by_symbol",
            params={"symbols": symbols},
        )
        records = [dict(entry) for entry in body.get("data", [])]
        return iter(records), {}

    def _read_token_prices_by_address(
        self, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        # ``addresses`` option is ``network:address`` pairs, comma-sep.
        # e.g. ``eth-mainnet:0xA0b...,polygon-mainnet:0xC02...``
        raw_pairs = parse_csv_option(table_options, "addresses")
        if not raw_pairs:
            raise ValueError(
                "token_prices_by_address requires "
                "table_options['addresses'] (network:address pairs, "
                "comma-separated)"
            )
        addr_objects: list[dict[str, str]] = []
        for pair in raw_pairs:
            if ":" not in pair:
                raise ValueError(f"Invalid address pair {pair!r}; expected 'network:address'")
            network, address = pair.split(":", 1)
            addr_objects.append({"network": network.strip(), "address": address.strip()})
        url = self._prices_url("token_prices_by_address")
        body = api_call(
            self._session,
            "POST",
            url,
            "token_prices_by_address",
            json_body={"addresses": addr_objects},
        )
        records = [dict(entry) for entry in body.get("data", [])]
        return iter(records), {}

    def _read_token_prices_historical(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Append-only historical-prices read (non-partitioned path).

        Used as the ``read_table`` fallback when the framework opts
        out of the partitioned path.  Walks the time range in chunks
        equal to the maximum window for the interval.
        """
        existing_cursor = start_offset.get("cursor")
        if existing_cursor and existing_cursor >= self._init_ts:
            return iter([]), start_offset

        start_time = existing_cursor or self._resolve_start_time(table_options)
        end_time = min(self._resolve_end_time(table_options), self._init_ts)
        if start_time >= end_time:
            terminal = {"cursor": self._init_ts}
            if start_offset and start_offset == terminal:
                return iter([]), start_offset
            return iter([]), terminal

        max_records = parse_max_records(table_options)
        interval = table_options.get("interval", "1d")
        partition_days = _HISTORICAL_INTERVAL_DAYS.get(interval, 365)

        records: list[dict] = []
        windows = _split_time_range_into_partitions(start_time, end_time, partition_days)
        last_window_end = start_time
        cap_hit = False
        for win in windows:
            page = self._fetch_token_prices_historical(
                table_options, start_time=win["start"], end_time=win["end"]
            )
            records.extend(page)
            last_window_end = win["end"]
            if max_records is not None and len(records) >= max_records:
                cap_hit = True
                break

        if not records and not cap_hit:
            terminal = {"cursor": self._init_ts}
            if start_offset and start_offset == terminal:
                return iter([]), start_offset
            return iter([]), terminal

        # Walked through every window in this microbatch — anchor to
        # the last window end (clamped at init_ts) so a follow-up call
        # short-circuits.
        end_cursor = last_window_end if last_window_end >= self._init_ts else self._init_ts
        end_offset: dict[str, Any] = {"cursor": end_cursor}
        if start_offset and start_offset == end_offset:
            return iter([]), start_offset
        return iter(records), end_offset

    def _fetch_token_prices_historical(
        self,
        table_options: dict[str, str],
        *,
        start_time: str,
        end_time: str,
    ) -> list[dict]:
        """Issue a single ``tokens/historical`` request and normalise rows."""
        body: dict[str, Any] = {
            "startTime": start_time,
            "endTime": end_time,
            "interval": table_options.get("interval", "1d"),
            "withMarketData": (table_options.get("with_market_data", "false").lower() == "true"),
        }
        symbol = table_options.get("symbol")
        network = table_options.get("network")
        address = table_options.get("contract_address") or table_options.get("address")
        if symbol:
            body["symbol"] = symbol
        elif network and address:
            body["network"] = network
            body["address"] = address
        else:
            raise ValueError(
                "token_prices_historical requires either 'symbol' or "
                "('network' + 'contract_address') in table_options"
            )

        url = self._prices_url("token_prices_historical")
        page = api_call(
            self._session,
            "POST",
            url,
            "token_prices_historical",
            json_body=body,
        )
        currency = page.get("currency")
        out: list[dict] = []
        for point in page.get("data", []) or []:
            rec = {
                "symbol": page.get("symbol") or symbol,
                "network": page.get("network") or network,
                "address": page.get("address") or address,
                "currency": currency,
                "value": point.get("value"),
                "timestamp": point.get("timestamp"),
                "marketCap": point.get("marketCap"),
                "totalVolume": point.get("totalVolume"),
            }
            # Filter out post-cap rows defensively.
            ts = rec.get("timestamp")
            if ts and ts > self._init_ts:
                continue
            out.append(rec)
        return out

    # ================================================================== #
    # NFT v3 readers
    # ================================================================== #

    def _nft_v3_url(self, table_name: str, network: str) -> str:
        path = NFT_V3_PATHS[table_name].format(api_key=self._api_key)
        return f"{_nft_v3_base_url(network)}{path}"

    def _resolve_nft_network(self, table_options: dict[str, str], table_name: str) -> str:
        if table_name in ETH_MAINNET_ONLY_TABLES:
            # Floor prices only exist on ETH mainnet; reject overrides
            # rather than silently 404 against another network.
            requested = table_options.get("network", "eth-mainnet")
            if requested != "eth-mainnet":
                raise ValueError(
                    f"{table_name} only supports network 'eth-mainnet'; got {requested!r}"
                )
            return "eth-mainnet"
        return table_options.get("network") or self._default_network

    def _read_nfts_by_wallet(self, table_options: dict[str, str]) -> tuple[Iterator[dict], dict]:
        wallet = require_option(table_options, "wallet_address", "nfts_by_wallet")
        network = self._resolve_nft_network(table_options, "nfts_by_wallet")
        url = self._nft_v3_url("nfts_by_wallet", network)

        records: list[dict] = []
        page_key: str | None = None
        while True:
            params: dict[str, Any] = {
                "owner": wallet,
                "withMetadata": "true",
                "pageSize": int(table_options.get("page_size", "100")),
            }
            if page_key:
                params["pageKey"] = page_key

            body = api_call(self._session, "GET", url, "nfts_by_wallet", params=params)
            for nft in body.get("ownedNfts", []) or []:
                rec = normalise_nft_record(dict(nft))
                rec["owner"] = wallet
                rec["network"] = network
                records.append(rec)

            page_key = body.get("pageKey")
            if not page_key:
                break

        return iter(records), {}

    def _read_nft_metadata(self, table_options: dict[str, str]) -> tuple[Iterator[dict], dict]:
        contract = require_option(table_options, "contract_address", "nft_metadata")
        token_id = require_option(table_options, "token_id", "nft_metadata")
        network = self._resolve_nft_network(table_options, "nft_metadata")
        url = self._nft_v3_url("nft_metadata", network)

        params: dict[str, Any] = {
            "contractAddress": contract,
            "tokenId": token_id,
        }
        token_type = table_options.get("token_type")
        if token_type:
            params["tokenType"] = token_type

        body = api_call(self._session, "GET", url, "nft_metadata", params=params)
        rec = normalise_nft_record(dict(body))
        rec["network"] = network
        # Ensure ``contract`` shape is consistent (sometimes empty
        # contracts come back as just an address string).
        if rec.get("contract") is None and contract:
            rec["contract"] = {"address": contract}
        return iter([rec]), {}

    def _read_nft_contract_metadata(
        self, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        contract = require_option(table_options, "contract_address", "nft_contract_metadata")
        network = self._resolve_nft_network(table_options, "nft_contract_metadata")
        url = self._nft_v3_url("nft_contract_metadata", network)

        body = api_call(
            self._session,
            "GET",
            url,
            "nft_contract_metadata",
            params={"contractAddress": contract},
        )
        normalised = normalise_contract(dict(body)) or {}
        rec = {
            "network": network,
            "address": normalised.get("address") or contract,
            "name": normalised.get("name"),
            "symbol": normalised.get("symbol"),
            "totalSupply": normalised.get("totalSupply"),
            "tokenType": normalised.get("tokenType"),
            "contractDeployer": normalised.get("contractDeployer"),
            "deployedBlockNumber": normalised.get("deployedBlockNumber"),
            "openseaMetadata": normalised.get("openseaMetadata"),
        }
        return iter([rec]), {}

    def _read_nft_floor_prices(self, table_options: dict[str, str]) -> tuple[Iterator[dict], dict]:
        contract = require_option(table_options, "contract_address", "nft_floor_prices")
        network = self._resolve_nft_network(table_options, "nft_floor_prices")
        url = self._nft_v3_url("nft_floor_prices", network)

        params: dict[str, Any] = {"contractAddress": contract}
        slug = table_options.get("collection_slug")
        if slug:
            params["collectionSlug"] = slug

        body = api_call(self._session, "GET", url, "nft_floor_prices", params=params)
        # Response is a dynamic object keyed by marketplace name.
        records: list[dict] = []
        for marketplace, payload in (body or {}).items():
            if not isinstance(payload, dict):
                continue
            records.append(
                {
                    "network": network,
                    "contractAddress": contract,
                    "marketplace": marketplace,
                    "floorPrice": payload.get("floorPrice"),
                    "priceCurrency": payload.get("priceCurrency"),
                    "collectionUrl": payload.get("collectionUrl"),
                    "retrievedAt": payload.get("retrievedAt") or self._init_ts,
                    "error": payload.get("error"),
                }
            )
        return iter(records), {}

    # ================================================================== #
    # Helpers
    # ================================================================== #

    def _paginate_portfolio(
        self, url: str, body: dict[str, Any], label: str
    ) -> Iterator[dict[str, Any]]:
        """Yield the ``data`` block of each Portfolio API response page.

        Portfolio endpoints use a ``pageKey`` cursor returned inside
        the ``data`` block.  Pass it as ``pageKey`` in the next request
        body; stop when it comes back null.
        """
        next_key: str | None = None
        while True:
            request_body = dict(body)
            if next_key:
                request_body["pageKey"] = next_key
            page = api_call(
                self._session,
                "POST",
                url,
                label,
                json_body=request_body,
            )
            data = page.get("data") or {}
            yield data
            next_key = data.get("pageKey")
            if not next_key:
                break

    def _resolve_start_time(self, table_options: dict[str, str]) -> str:
        """Return ISO 8601 start time for the historical-prices range.

        Defaults to ``end_time - max_window`` if ``start_time`` is not
        explicitly supplied, so the connector always has a bounded
        request window.
        """
        explicit = table_options.get("start_time")
        if explicit:
            return explicit
        interval = table_options.get("interval", "1d")
        days = _HISTORICAL_INTERVAL_DAYS.get(interval, 365)
        end = self._resolve_end_time(table_options)
        end_dt = _parse_iso(end) or datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(days=days)
        return start_dt.isoformat()

    def _resolve_end_time(self, table_options: dict[str, str]) -> str:
        explicit = table_options.get("end_time")
        if explicit:
            return explicit
        return self._init_ts


# --------------------------------------------------------------------------- #
# Free helpers
# --------------------------------------------------------------------------- #


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    # Tolerate the trailing-Z form Alchemy uses ("...Z") which Python
    # 3.10's fromisoformat rejects pre-3.11.
    normalised = value.replace("Z", "+00:00") if value.endswith("Z") else value
    try:
        return datetime.fromisoformat(normalised)
    except ValueError:
        return None


def _split_time_range_into_partitions(start: str, end: str, days_per_window: int) -> list[dict]:
    """Split ``[start, end]`` into fixed-size time windows.

    Each window has ``start`` and ``end`` ISO 8601 strings.  Returns
    an empty list if the inputs cannot be parsed (the caller will fall
    back to a single-partition read at the framework level).
    """
    start_dt = _parse_iso(start)
    end_dt = _parse_iso(end)
    if not start_dt or not end_dt or start_dt >= end_dt:
        return []

    partitions: list[dict] = []
    cursor = start_dt
    step = timedelta(days=days_per_window)
    while cursor < end_dt:
        window_end = min(cursor + step, end_dt)
        partitions.append({"start": cursor.isoformat(), "end": window_end.isoformat()})
        cursor = window_end
    return partitions
