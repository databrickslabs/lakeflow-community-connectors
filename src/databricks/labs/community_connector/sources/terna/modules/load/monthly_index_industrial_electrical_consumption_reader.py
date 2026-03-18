"""Reader for the monthly_index_industrial_electrical_consumption table (Terna load API).

This API uses year/month query params (not dateFrom/dateTo like other load APIs).
Both year and month are mandatory.
"""

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
    """Reads monthly_index_industrial_electrical_consumption data from the Terna Public API for a single year/month."""

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

    def read(
        self,
        start_offset: dict | None,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Read monthly_index_industrial_electrical_consumption records for a single year/month."""
        logger.info("Table options: %s", table_options)

        year_str = table_options.get("year")
        month_str = table_options.get("month")

        if year_str is None:
            raise ValueError(
                "monthly_index_industrial_electrical_consumption requires 'year'"
            )
        if month_str is None:
            raise ValueError(
                "monthly_index_industrial_electrical_consumption requires 'month'"
            )

        try:
            year = int(year_str)
        except ValueError:
            raise ValueError(
                f"monthly_index_industrial_electrical_consumption: 'year' must be an integer, got '{year_str}'"
            )

        try:
            month = int(month_str)
        except ValueError:
            raise ValueError(
                f"monthly_index_industrial_electrical_consumption: 'month' must be an integer, got '{month_str}'"
            )

        if month < 1 or month > 12:
            raise ValueError(
                f"monthly_index_industrial_electrical_consumption: 'month' must be between 1 and 12, got {month}"
            )

        now = datetime.now(timezone.utc)
        min_year = now.year - TERNA_MAX_HISTORY_SOLAR_YEARS
        if year < min_year:
            raise ValueError(
                f"Terna connector: 'year' must be within the last "
                f"{TERNA_MAX_HISTORY_SOLAR_YEARS} solar years, not earlier than {min_year}"
            )

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

        records: list[dict] = data if isinstance(data, list) else []

        cursor = f"{year}/{month:02d}"
        return iter(records), {"cursor": cursor}
