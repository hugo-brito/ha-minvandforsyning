"""DataUpdateCoordinator for MinVandforsyning."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .api_client import MinvandforsyningClient
from .const import (
    ACUTE_NIGHT_TABLE_INDEX,
    COL_CONSUMPTION,
    COL_HIGH_ALERT_COUNT,
    COL_INFO_CODE_ACTIVE,
    COL_INFO_CODE_VALUE,
    COL_LATEST_ZERO,
    COL_MIN_HOURLY,
    COL_NIGHTS_CONTINUOUS,
    COL_READING,
    COL_READING_DATE,
    COL_REAL_READINGS_COUNT,
    COL_TOTAL_NIGHT,
    COL_ZERO_COUNT,
    DEFAULT_SCAN_INTERVAL,
    DOMAIN,
    FULL_DAY_TABLE_INDEX,
    HISTORICAL_NIGHT_TABLE_INDEX,
    INFO_CODE_TABLE_INDEX,
    QUERY_LOOKBACK_HOURS,
    READINGS_TABLE_INDEX,
)
from .protobuf_parser import parse_dataset

_LOGGER = logging.getLogger(__name__)


class MeterReading:
    """A single hourly meter reading."""

    __slots__ = ("date", "reading", "consumption")

    def __init__(self, date: datetime, reading: Decimal, consumption: Decimal) -> None:
        self.date = date
        self.reading = reading
        self.consumption = consumption


class MinvandforsyningData:
    """Processed data from the API."""

    def __init__(
        self,
        readings: list[MeterReading],
        min_hourly_consumption: Decimal | None = None,
        latest_zero_consumption: datetime | None = None,
        zero_consumption_hours: int | None = None,
        high_consumption_hours: int | None = None,
        real_readings_count: int | None = None,
        night_consumption_total: Decimal | None = None,
        nights_with_continuous_flow: int | None = None,
        info_code_active: bool | None = None,
        info_code_value: int | None = None,
    ) -> None:
        self.readings = readings
        self.min_hourly_consumption = min_hourly_consumption
        self.latest_zero_consumption = latest_zero_consumption
        self.zero_consumption_hours = zero_consumption_hours
        self.high_consumption_hours = high_consumption_hours
        self.real_readings_count = real_readings_count
        self.night_consumption_total = night_consumption_total
        self.nights_with_continuous_flow = nights_with_continuous_flow
        self.info_code_active = info_code_active
        self.info_code_value = info_code_value

    @property
    def latest_reading(self) -> MeterReading | None:
        return self.readings[-1] if self.readings else None

    @property
    def total_m3(self) -> Decimal | None:
        latest = self.latest_reading
        return latest.reading if latest else None

    @property
    def last_hour_liters(self) -> Decimal | None:
        latest = self.latest_reading
        return latest.consumption if latest else None

    def daily_liters(self, date: datetime | None = None) -> Decimal:
        """Sum consumption for a given date (default: today)."""
        if date is None:
            date = datetime.now()
        target_date = date.date()
        return sum(
            (r.consumption for r in self.readings if r.date.date() == target_date),
            Decimal(0),
        )


class MinvandforsyningCoordinator(DataUpdateCoordinator[MinvandforsyningData]):
    """Coordinator that fetches meter data from the Rambøll API."""

    def __init__(
        self,
        hass: HomeAssistant,
        client: MinvandforsyningClient,
        meter_number: str,
        supplier_id: int,
    ) -> None:
        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            update_interval=timedelta(seconds=DEFAULT_SCAN_INTERVAL),
        )
        self._client = client
        self._meter_number = meter_number
        self._supplier_id = supplier_id

    async def _async_update_data(self) -> MinvandforsyningData:
        """Fetch and parse meter data."""
        now = datetime.now(timezone.utc)
        date_from = now - timedelta(hours=QUERY_LOOKBACK_HOURS)
        date_to = now

        try:
            raw = await self._client.async_get_meter_data(
                self._meter_number, self._supplier_id, date_from, date_to,
            )
        except Exception as err:
            raise UpdateFailed(f"Error fetching meter data: {err}") from err

        try:
            tables = parse_dataset(raw)
        except Exception as err:
            raise UpdateFailed(f"Error parsing meter data: {err}") from err

        if len(tables) <= READINGS_TABLE_INDEX:
            raise UpdateFailed(
                f"Response has {len(tables)} tables, expected at least {READINGS_TABLE_INDEX + 1}"
            )

        readings_table = tables[READINGS_TABLE_INDEX]
        readings: list[MeterReading] = []
        for row in readings_table.rows:
            reading_date = row.get(COL_READING_DATE)
            reading = row.get(COL_READING)
            consumption = row.get(COL_CONSUMPTION)
            if reading_date is not None and reading is not None and consumption is not None:
                readings.append(MeterReading(reading_date, reading, consumption))

        _LOGGER.debug(
            "Fetched %d readings for meter %s (latest: %s)",
            len(readings),
            self._meter_number,
            readings[-1].date if readings else "none",
        )

        # Extract analysis data from Tables 3-6 (optional, don't fail if missing)
        analysis_kwargs: dict[str, Any] = {}

        # Table 3: AcuteNightConsumption (index 2)
        # Contains: zero consumption count, high alert count, real readings count
        if len(tables) > ACUTE_NIGHT_TABLE_INDEX:
            acute_night = tables[ACUTE_NIGHT_TABLE_INDEX]
            if acute_night.rows:
                row = acute_night.rows[0]
                analysis_kwargs["zero_consumption_hours"] = row.get(COL_ZERO_COUNT)
                analysis_kwargs["high_consumption_hours"] = row.get(COL_HIGH_ALERT_COUNT)
                analysis_kwargs["real_readings_count"] = row.get(COL_REAL_READINGS_COUNT)

        # Table 4: FullDayConsumption (index 3)
        # Contains: minimum hourly consumption, latest zero consumption timestamp
        if len(tables) > FULL_DAY_TABLE_INDEX:
            full_day = tables[FULL_DAY_TABLE_INDEX]
            if full_day.rows:
                row = full_day.rows[0]
                analysis_kwargs["min_hourly_consumption"] = row.get(COL_MIN_HOURLY)
                # LatestZeroConsumption is a String, not DateTime - store as-is
                analysis_kwargs["latest_zero_consumption"] = row.get(COL_LATEST_ZERO)

        # Table 5: HistoricalNightConsumption (index 4)
        if len(tables) > HISTORICAL_NIGHT_TABLE_INDEX:
            hist_night = tables[HISTORICAL_NIGHT_TABLE_INDEX]
            if hist_night.rows:
                row = hist_night.rows[0]
                analysis_kwargs["night_consumption_total"] = row.get(COL_TOTAL_NIGHT)
                analysis_kwargs["nights_with_continuous_flow"] = row.get(COL_NIGHTS_CONTINUOUS)

        # Table 6: InfoCode (index 5)
        if len(tables) > INFO_CODE_TABLE_INDEX:
            info_code_table = tables[INFO_CODE_TABLE_INDEX]
            if info_code_table.rows:
                row = info_code_table.rows[0]
                analysis_kwargs["info_code_active"] = row.get(COL_INFO_CODE_ACTIVE)
                analysis_kwargs["info_code_value"] = row.get(COL_INFO_CODE_VALUE)

        return MinvandforsyningData(readings, **analysis_kwargs)
