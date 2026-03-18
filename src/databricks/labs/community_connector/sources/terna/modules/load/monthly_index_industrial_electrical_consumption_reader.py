"""Reader for the total_load table (Terna load API)."""

import logging
from datetime import datetime, timedelta, timezone
from typing import Iterator

from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

from databricks.labs.community_connector.sources.terna.utils.terna_api_client import (
    TernaApiClient,
)

logger = logging.getLogger(__name__)

# Terna API allows at most this many days per request; longer ranges are chunked
TERNA_MAX_DAYS_PER_REQUEST = 60
# Terna API allows history only within the last N solar years
TERNA_MAX_HISTORY_SOLAR_YEARS = 5

MONTHLY_INDEX_INDUSTRIAL_ELECTRICAL_CONSUMPTION_PATH = "/load/v2.0/monthly-index-industrial-electrical-consumption"

class MonthlyIndexIndustrialElectricalConsumptionReader:
    """Reads monthly_index_industrial_electrical_consumption data from the Terna Public API in date-range chunks."""

    MONTHLY_INDEX_INDUSTRIAL_ELECTRICAL_CONSUMPTION_SECTORS = [
        "ALIMENTARE",
        "ALTRI",
        "CARTARIA",
        "CEMENTO CALCE E GESSO",
        "CERAMICHE E VETRARIE",
        "CHIMICA",
        "MECCANICA",
        "METALLI NON FERROSI",
        "MEZZI DI TRASPORTO",
        "SIDERURGIA"
    ]

    MONTHLY_INDEX_INDUSTRIAL_ELECTRICAL_CONSUMPTION_TENSION_TYPES = [
        "AT",
        "MT",
    ]

    MONTHLY_INDEX_INDUSTRIAL_ELECTRICAL_CONSUMPTION_KEY = "monthly_index_industrial_electrical_consumption"

    MONTHLY_INDEX_INDUSTRIAL_ELECTRICAL_CONSUMPTION_SCHEMA = StructType(
        [
            StructField("date", StringType(), True),
            StructField("date_offset", StringType(), True),
            StructField("region", StringType(), True),
            StructField("sector", StringType(), True),
            StructField("tension_type", StringType(), True),
            StructField("monthly_imcei", StringType(), True),
            StructField("consumption_Gwh", StringType(), True),
        ]
    )

    MONTHLY_INDEX_INDUSTRIAL_ELECTRICAL_CONSUMPTION_METADATA = {
        "primary_keys": ["date", "region"],
        "cursor_field": "date",
        "ingestion_type": "append",
    }

    def __init__(self, client: TernaApiClient) -> None:
        self._client = client

    def read(
        self,
        start_offset: dict | None,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Read total_load records. Optional table_options: biddingZone (comma-separated or repeated)."""
        logger.info("Table options: %s", table_options)

        extra: dict[str, str | list[str]] = {}

        raw_sectors = table_options.get("sectors")
        
        if raw_sectors is not None:
            sectors = self._client.validate_extra_params(raw_sectors)

            for sector in sectors:
                if sector not in self.MONTHLY_INDEX_INDUSTRIAL_ELECTRICAL_CONSUMPTION_SECTORS:
                    raise ValueError(
                        f"Terna connector: Invalid sector value {sector}. Must be one of {', '.join(self.MONTHLY_INDEX_INDUSTRIAL_ELECTRICAL_CONSUMPTION_SECTORS)}"
                    )
            extra["sector"] = sectors

        raw_tension_types = table_options.get("tension_types")

        if raw_tension_types is not None:
            tension_types = self._client.validate_extra_params(raw_tension_types)

            for tension_type in tension_types:
                if tension_type not in self.MONTHLY_INDEX_INDUSTRIAL_ELECTRICAL_CONSUMPTION_TENSION_TYPES:
                    raise ValueError(
                        f"Terna connector: Invalid tension_type value {tension_type}. Must be one of {', '.join(self.MONTHLY_INDEX_INDUSTRIAL_ELECTRICAL_CONSUMPTION_TENSION_TYPES)}"
                    )
            extra["tension_type"] = tension_types

        date_from_str = table_options.get("date_from")
        date_to_str = table_options.get("date_to")

        if date_from_str is None:
            raise ValueError(
                "Terna connector, API total_load requires 'date_from'"
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
                    self.MONTHLY_INDEX_INDUSTRIAL_ELECTRICAL_CONSUMPTION_KEY,
                    MONTHLY_INDEX_INDUSTRIAL_ELECTRICAL_CONSUMPTION_PATH,
                    chunk_from,
                    chunk_to,
                    table_options,
                    self.MONTHLY_INDEX_INDUSTRIAL_ELECTRICAL_CONSUMPTION_KEY,
                    extra_params=extra if extra else None,
                )
            )

        return iter(records), {
            "cursor": self._client.format_cursor(chunks[-1][1])
        }
