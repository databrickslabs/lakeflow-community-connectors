"""Reader for the daily_prices table (Terna fees API)."""

import logging
from datetime import datetime, timedelta, timezone
from typing import Iterator

from databricks.labs.community_connector.sources.terna.utils.terna_api_client import (
    TernaApiClient,
)

logger = logging.getLogger(__name__)

# Terna API allows at most this many days per request; longer ranges are chunked
MAX_DAYS_PER_REQUEST = 62
# Terna API allows history only within the last N solar years
MAX_HISTORY_SOLAR_YEARS = 5

DAILY_PRICES_PATH = "/fees/v1.0/daily-prices"
DAILY_PRICES_KEY = "daily_prices"


class DailyPricesReader:
    """Reads daily_prices data from the Terna Public API in date-range chunks."""

    # Bidding zones supported by the Terna API
    DAILY_PRICES_DATA_TYPES = [
        "Orario",
        "Quarto Orario"
    ]

    def __init__(self, client: TernaApiClient) -> None:
        self._client = client

    def read(
        self,
        start_offset: dict | None,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Read daily_prices records. Optional table_options: dataTypes (Orario, Quarto Orario)."""
        logger.info("Table options: %s", table_options)

        extra: dict[str, str | list[str]] = {}
        
        raw_data_types = table_options.get("data_types")
        
        if raw_data_types is not None:
            data_types = self._client.validate_extra_params(raw_data_types)

            for data_type in data_types:
                if data_type not in self.DAILY_PRICES_DATA_TYPES:
                    raise ValueError(
                        f"Terna connector: Invalid dataType value {data_type}. Must be one of {', '.join(self.DAILY_PRICES_DATA_TYPES)}"
                    )
            extra["dataType"] = data_types

        date_from_str = table_options.get("date_from")
        date_to_str = table_options.get("date_to")

        if date_from_str is None:
            raise ValueError(
                "Terna connector, API daily_prices requires 'date_from'"
            )

        date_from = self._client.string_to_datetime(date_from_str)
        now = datetime.now(timezone.utc)
        min_allowed = datetime(
            now.year - MAX_HISTORY_SOLAR_YEARS, 1, 1, tzinfo=timezone.utc
        )
        if date_from < min_allowed:
            raise ValueError(
                f"Terna connector: 'date_from' must be within the last "
                f"{MAX_HISTORY_SOLAR_YEARS} solar years, not sooner than "
                f"01/01/{now.year - MAX_HISTORY_SOLAR_YEARS}"
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
                current_start + timedelta(days=MAX_DAYS_PER_REQUEST - 1),
                date_to,
            )
            chunks.append((current_start, current_end))
            current_start = current_end + timedelta(days=1)

        if len(chunks) > 1:
            logger.info(
                f"Requested more than {MAX_DAYS_PER_REQUEST} days, will be split in %s API calls.",
                len(chunks),
            )

        records: list[dict] = []
        for chunk_from, chunk_to in chunks:
            records.extend(
                self._client.read_table_chunk(
                    DAILY_PRICES_KEY,
                    DAILY_PRICES_PATH,
                    chunk_from,
                    chunk_to,
                    table_options,
                    DAILY_PRICES_KEY,
                    extra_params=extra if extra else None,
                )
            )

        return iter(records), {
            "cursor": self._client.format_cursor(chunks[-1][1])
        }
