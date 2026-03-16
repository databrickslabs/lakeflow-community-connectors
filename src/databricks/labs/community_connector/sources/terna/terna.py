"""Terna Lakeflow community connector.

Ingests Italian electricity system data (load, generation, transmission)
from the Terna Public API. Uses OAuth 2.0 Client Credentials; optional
x_api_key for the Physical Foreign Flow (transmission) endpoint.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Iterator

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.terna.terna_schemas import (
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
    TOTAL_LOAD_BIDDING_ZONES,
)
from databricks.labs.community_connector.sources.terna.utils import TernaApiClient

logger = logging.getLogger(__name__)

# Terna API allows at most this many days per request; longer ranges are chunked
TERNA_MAX_DAYS_PER_REQUEST = 60
# Terna API allows history only within the last N solar years (date_from not sooner than 01/01/(year-N))
TERNA_MAX_HISTORY_SOLAR_YEARS = 5

class TernaLakeflowConnect(LakeflowConnect):
    """LakeflowConnect implementation for the Terna Public API."""

    def __init__(self, options: dict[str, str]) -> None:
        """
        Initialize the Terna connector.

        Expected options:
            - client_id: OAuth 2.0 Client Credentials application key (required).
            - client_secret: OAuth 2.0 Client Credentials secret (required).
            - base_url: Base URL for the API (default https://api.terna.it).
            - x_api_key: Optional API key for Physical Foreign Flow when that
              endpoint uses x-api-key instead of Bearer token.
        """
        super().__init__(options)
        self._client = TernaApiClient(options)


    def list_tables(self) -> list[str]:
        """List names of all tables supported by this connector."""
        return SUPPORTED_TABLES

    def get_table_schema(
        self, table_name: str, table_options: dict[str, str]
    ) -> StructType:
        """Return the Spark schema for the given table."""
        self._client.validate_table(table_name)
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(
        self, table_name: str, table_options: dict[str, str]
    ) -> dict:
        """Return metadata (primary_keys, cursor_field, ingestion_type) for the table."""
        self._client.validate_table(table_name)
        return dict(TABLE_METADATA[table_name])

    def read_table(
        self,
        table_name: str,
        start_offset: dict | None,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Read records by date-range chunks; cursor is the last 'date' value (yyyy-mm-dd hh:mm:ss)."""
        self._client.validate_table(table_name)
        reader = {
            "total_load": self._read_total_load,
            #"actual_generation": self._read_actual_generation,
            #"renewable_generation": self._read_renewable_generation,
            #"physical_foreign_flow": self._read_physical_foreign_flow,
        }[table_name]
        return reader(start_offset, table_options)

    def _read_total_load(
        self,
        start_offset: dict | None,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Read total_load in one date chunk. Optional table_options: biddingZone (comma or repeated)."""

        logger.info(f"Table options: {table_options}")

        extra = {}
        bidding_zones = (
            table_options.get("biddingZones")
            or table_options.get("bidding_zones")
            or table_options.get("biddingzones")
        )

        if bidding_zones is not None:
            # API accepts multiple biddingZone params
            for bidding_zone in bidding_zones:
                if bidding_zone not in TOTAL_LOAD_BIDDING_ZONES:
                    raise ValueError(
                        f"Terna connector: Invalid biddingZone value {bidding_zone}. Must be one of {', '.join(TOTAL_LOAD_BIDDING_ZONES)}"
                    )
            extra["biddingZone"] = bidding_zones
        
        date_from = (
            table_options.get("date_from")
            or table_options.get("dateFrom")
            or table_options.get("datefrom")
        )

        date_to = (
            table_options.get("date_to")
            or table_options.get("dateTo")
            or table_options.get("dateto")
        )

        if date_from is None:
            raise ValueError(
                "Terna connector, API total_load requires 'date_from'"
            )

        date_from = self._client.string_to_datetime(date_from)
        now = datetime.now(timezone.utc)
        min_allowed = datetime(
            now.year - TERNA_MAX_HISTORY_SOLAR_YEARS, 1, 1, tzinfo=timezone.utc
        )
        if date_from < min_allowed:
            raise ValueError(
                f"Terna connector: 'date_from' must be within the last {TERNA_MAX_HISTORY_SOLAR_YEARS} solar years, not sooner than 01/01/{now.year - TERNA_MAX_HISTORY_SOLAR_YEARS}"
            )

        if date_to is not None:
            date_to = self._client.string_to_datetime(date_to)
        else:
            date_to = datetime.now(timezone.utc)

        if self._client.format_cursor(date_from) == self._client.format_cursor(date_to):
            return iter([]), {"cursor": self._client.format_cursor(date_to)}

        if start_offset is None or start_offset.get("cursor") is None:
            # We are in a full refresh. Normally I want all data from date_from to date_to unless date_to is None
            pass
        else:
            # We are in CDC. date_from is the cursor from the previous run. Normally I want all data from date_from to date_to unless date_to is None
            date_from = self._client.string_to_datetime(start_offset.get("cursor"))

            if date_to is None:
                # We are in the setup where I want all data from date_from to current time
                pass
        
        chunks = []
        current_start = date_from
        while current_start <= date_to:
            current_end = min(current_start + timedelta(days=TERNA_MAX_DAYS_PER_REQUEST - 1), date_to)
            chunks.append((current_start, current_end))
            current_start = current_end + timedelta(days=1)

        if len(chunks) > 1:
            logger.info(f"Requested more than 60 days, will be split in {len(chunks)} API calls.")
        #else:
        #    chunks.append((date_from, date_to))

        records = []
        for chunk in chunks:
            chunk_from, chunk_to = chunk
            records.extend(
                self._client.read_table_chunk(
                    "total_load",
                    "/load/v2.0/total-load",
                    chunk_from,
                    chunk_to,
                    table_options,
                    "total_load",
                    extra_params=extra if extra else None,
                )
            )

        return iter(records), {"cursor": self._client.format_cursor(chunks[-1][1])}
'''
    def _read_actual_generation(
        self,
        start_offset: dict | None,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Read actual_generation in one date chunk. Optional table_options: type (primary source)."""
        chunk_result, range_end_opt = self._next_chunk(start_offset, table_options)
        if chunk_result is None:
            return iter([]), start_offset or {}

        from_date, to_date = chunk_result
        extra = {}
        type_opt = table_options.get("type")
        if type_opt:
            extra["type"] = type_opt.strip()

        records = self._read_table_chunk(
            "actual_generation",
            "/generation/v2.0/actual-generation",
            from_date,
            to_date,
            table_options,
            "actual_generation",
            extra_params=extra if extra else None,
        )
        if not records:
            end_offset = {"cursor": self._format_cursor(to_date)}
        else:
            max_date = self._max_date_in_records(records)
            end_offset = {"cursor": max_date} if max_date else dict(start_offset or {})
        if range_end_opt is not None:
            end_offset["range_end"] = range_end_opt
        elif start_offset and "range_end" in start_offset:
            end_offset["range_end"] = start_offset["range_end"]
        return iter(records), end_offset

    def _read_renewable_generation(
        self,
        start_offset: dict | None,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Read renewable_generation in one date chunk. Optional table_options: type."""
        chunk_result, range_end_opt = self._next_chunk(start_offset, table_options)
        if chunk_result is None:
            return iter([]), start_offset or {}

        from_date, to_date = chunk_result
        extra = {}
        type_opt = table_options.get("type")
        if type_opt:
            extra["type"] = type_opt.strip()

        records = self._read_table_chunk(
            "renewable_generation",
            "/generation/v2.0/renewable-generation",
            from_date,
            to_date,
            table_options,
            "renewable_generation",
            extra_params=extra if extra else None,
        )
        if not records:
            end_offset = {"cursor": self._format_cursor(to_date)}
        else:
            max_date = self._max_date_in_records(records)
            end_offset = {"cursor": max_date} if max_date else dict(start_offset or {})
        if range_end_opt is not None:
            end_offset["range_end"] = range_end_opt
        elif start_offset and "range_end" in start_offset:
            end_offset["range_end"] = start_offset["range_end"]
        return iter(records), end_offset

    def _read_physical_foreign_flow(
        self,
        start_offset: dict | None,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Read physical_foreign_flow in one date chunk."""
        chunk_result, range_end_opt = self._next_chunk(start_offset, table_options)
        if chunk_result is None:
            return iter([]), start_offset or {}

        from_date, to_date = chunk_result
        records = self._read_table_chunk(
            "physical_foreign_flow",
            "/transmission/v2.0/physical-foreign-flow",
            from_date,
            to_date,
            table_options,
            "physical_foreign_flow"
        )
        if not records:
            end_offset = {"cursor": self._format_cursor(to_date)}
        else:
            max_date = self._max_date_in_records(records)
            end_offset = {"cursor": max_date} if max_date else dict(start_offset or {})
        if range_end_opt is not None:
            end_offset["range_end"] = range_end_opt
        elif start_offset and "range_end" in start_offset:
            end_offset["range_end"] = start_offset["range_end"]
        return iter(records), end_offset
'''