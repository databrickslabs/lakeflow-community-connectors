"""Reader for the market_load table (Terna load API)."""

import logging
from datetime import datetime, timedelta, timezone
from typing import Iterator

from databricks.labs.community_connector.sources.terna.terna_schemas import (
    MARKET_LOAD_METADATA,
    MARKET_LOAD_SCHEMA,
)
from databricks.labs.community_connector.sources.terna.utils.terna_api_client import (
    TernaApiClient,
)

logger = logging.getLogger(__name__)

# Terna API allows at most this many days per request; longer ranges are chunked
TERNA_MAX_DAYS_PER_REQUEST = 60
# Terna API allows history only within the last N solar years
TERNA_MAX_HISTORY_SOLAR_YEARS = 5

MARKET_LOAD_PATH = "/load/v2.0/market-load"
ARRAY_KEY = "market_load"


class MarketLoadReader:
    """Reads market_load data from the Terna Public API in date-range chunks."""

    MARKET_LOAD_SCHEMA = MARKET_LOAD_SCHEMA
    MARKET_LOAD_METADATA = MARKET_LOAD_METADATA

    # Bidding zones supported by the Terna API
    MARKET_LOAD_BIDDING_ZONES = [
        "North",
        "Centre-North",
        "South",
        "Centre-South",
        "Sardinia",
        "Sicily",
        "Calabria",
        "Italy",
    ]

    def __init__(self, client: TernaApiClient) -> None:
        self._client = client

    def read(
        self,
        start_offset: dict | None,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Read market_load records. Optional table_options: biddingZone (comma-separated or repeated)."""
        logger.info("Table options: %s", table_options)

        extra: dict[str, str | list[str]] = {}
        raw_bidding_zones = (
            table_options.get("biddingZones")
            or table_options.get("bidding_zones")
            or table_options.get("biddingzones")
        )
        if raw_bidding_zones is not None:
            zones = (
                [z.strip() for z in raw_bidding_zones.split(",")]
                if isinstance(raw_bidding_zones, str)
                else list(raw_bidding_zones)
            )
            for bidding_zone in zones:
                if bidding_zone not in self.MARKET_LOAD_BIDDING_ZONES:
                    raise ValueError(
                        f"Terna connector: Invalid biddingZone value {bidding_zone}. "
                        f"Must be one of {', '.join(self.MARKET_LOAD_BIDDING_ZONES)}"
                    )
            extra["biddingZone"] = zones

        date_from_str = (
            table_options.get("date_from")
            or table_options.get("dateFrom")
            or table_options.get("datefrom")
        )
        date_to_str = (
            table_options.get("date_to")
            or table_options.get("dateTo")
            or table_options.get("dateto")
        )

        if date_from_str is None:
            raise ValueError(
                "Terna connector, API market_load requires 'date_from'"
            )

        date_from = self._client.string_to_datetime(date_from_str)
        now = datetime.now(timezone.utc)
        min_allowed = datetime(
            now.year - TERNA_MAX_HISTORY_SOLAR_YEARS, 1, 1, tzinfo=timezone.utc
        )
        if date_from < min_allowed:
            raise ValueError(
                f"Terna connector: 'date_from' must be within the last "
                f"{TERNA_MAX_HISTORY_SOLAR_YEARS} solar years, not sooner than "
                f"01/01/{now.year - TERNA_MAX_HISTORY_SOLAR_YEARS}"
            )

        if date_to_str is not None:
            date_to = self._client.string_to_datetime(date_to_str)
        else:
            date_to = datetime.now(timezone.utc)

        if self._client.format_cursor(date_from) == self._client.format_cursor(date_to):
            return iter([]), {"cursor": self._client.format_cursor(date_to)}

        if start_offset and start_offset.get("cursor"):
            date_from = self._client.string_to_datetime(start_offset["cursor"])

        chunks: list[tuple[datetime, datetime]] = []
        current_start = date_from
        while current_start <= date_to:
            current_end = min(
                current_start + timedelta(days=TERNA_MAX_DAYS_PER_REQUEST - 1),
                date_to,
            )
            chunks.append((current_start, current_end))
            current_start = current_end + timedelta(days=1)

        if len(chunks) > 1:
            logger.info(
                "Requested more than 60 days, will be split in %s API calls.",
                len(chunks),
            )

        records: list[dict] = []
        for chunk_from, chunk_to in chunks:
            records.extend(
                self._client.read_table_chunk(
                    "market_load",
                    MARKET_LOAD_PATH,
                    chunk_from,
                    chunk_to,
                    table_options,
                    ARRAY_KEY,
                    extra_params=extra if extra else None,
                )
            )

        return iter(records), {
            "cursor": self._client.format_cursor(chunks[-1][1])
        }
