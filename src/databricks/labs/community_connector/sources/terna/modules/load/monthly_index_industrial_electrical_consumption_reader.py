"""Reader for the monthly_index_industrial_electrical_consumption table (Terna load API).

This API uses year/month query params (not dateFrom/dateTo like other load APIs).
"""

import calendar
import logging
from datetime import datetime, timezone
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

TERNA_MAX_HISTORY_SOLAR_YEARS = 5

MONTHLY_INDEX_INDUSTRIAL_ELECTRICAL_CONSUMPTION_PATH = (
    "/load/v2.0/monthly-index-industrial-electrical-consumption"
)


class MonthlyIndexIndustrialElectricalConsumptionReader:
    """Reads monthly_index_industrial_electrical_consumption data from the Terna Public API, one month at a time."""

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
        "SIDERURGIA",
    ]

    MONTHLY_INDEX_INDUSTRIAL_ELECTRICAL_CONSUMPTION_TENSION_TYPES = [
        "AT",
        "MT",
    ]

    MONTHLY_INDEX_INDUSTRIAL_ELECTRICAL_CONSUMPTION_KEY = (
        "monthly_index_industrial_electrical_consumption"
    )

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

    @staticmethod
    def _month_range(
        year_from: int, month_from: int, year_to: int, month_to: int
    ) -> list[tuple[int, int]]:
        """Generate a list of (year, month) tuples from start to end (inclusive)."""
        months: list[tuple[int, int]] = []
        y, m = year_from, month_from
        while (y, m) <= (year_to, month_to):
            months.append((y, m))
            if m == 12:
                y += 1
                m = 1
            else:
                m += 1
        return months

    def read(
        self,
        start_offset: dict | None,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Read monthly_index_industrial_electrical_consumption records month by month."""
        logger.info("Table options: %s", table_options)

        extra: dict[str, str | list[str]] = {}

        raw_sectors = table_options.get("sectors")
        if raw_sectors is not None:
            sectors = self._client.validate_extra_params(raw_sectors)
            for sector in sectors:
                if sector not in self.MONTHLY_INDEX_INDUSTRIAL_ELECTRICAL_CONSUMPTION_SECTORS:
                    raise ValueError(
                        f"Terna connector: Invalid sector value {sector}. "
                        f"Must be one of {', '.join(self.MONTHLY_INDEX_INDUSTRIAL_ELECTRICAL_CONSUMPTION_SECTORS)}"
                    )
            extra["sector"] = sectors

        raw_tension_types = table_options.get("tension_types")
        if raw_tension_types is not None:
            tension_types = self._client.validate_extra_params(raw_tension_types)
            for tension_type in tension_types:
                if tension_type not in self.MONTHLY_INDEX_INDUSTRIAL_ELECTRICAL_CONSUMPTION_TENSION_TYPES:
                    raise ValueError(
                        f"Terna connector: Invalid tension_type value {tension_type}. "
                        f"Must be one of {', '.join(self.MONTHLY_INDEX_INDUSTRIAL_ELECTRICAL_CONSUMPTION_TENSION_TYPES)}"
                    )
            extra["tensionType"] = tension_types

        date_from_str = table_options.get("date_from")
        date_to_str = table_options.get("date_to")

        if date_from_str is None:
            raise ValueError(
                "monthly_index_industrial_electrical_consumption requires 'date_from'"
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
            if date_from >= date_to:
                return iter([]), {"cursor": self._client.format_cursor(date_to)}

        months = self._month_range(
            date_from.year, date_from.month, date_to.year, date_to.month
        )

        if len(months) > 1:
            logger.info(
                "Requested %s months, will issue %s API calls.", len(months), len(months)
            )

        records: list[dict] = []
        for year, month in months:
            params: dict[str, str | list[str]] = {
                "year": str(year),
                "month": str(month),
            }
            if extra:
                params.update(extra)

            logger.debug("Querying IMCEI for year=%s month=%s extra=%s", year, month, extra)
            resp = self._client.request(
                "GET",
                MONTHLY_INDEX_INDUSTRIAL_ELECTRICAL_CONSUMPTION_PATH,
                params=params,
            )
            if resp.status_code != 200:
                raise RuntimeError(
                    f"Terna API error for monthly_index_industrial_electrical_consumption: "
                    f"{resp.status_code} {resp.text}"
                )
            body = resp.json()
            data = body.get(self.MONTHLY_INDEX_INDUSTRIAL_ELECTRICAL_CONSUMPTION_KEY)
            if isinstance(data, list):
                records.extend(data)

        last_year, last_month = months[-1]
        last_day = calendar.monthrange(last_year, last_month)[1]
        cursor_dt = datetime(last_year, last_month, last_day, tzinfo=timezone.utc)

        return iter(records), {"cursor": self._client.format_cursor(cursor_dt)}
